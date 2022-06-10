/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.checkpoint;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.checkpoint.CheckpointManager.User;
import es.bsc.compss.checkpoint.types.CheckpointData;
import es.bsc.compss.checkpoint.types.CheckpointDataVersion;
import es.bsc.compss.checkpoint.types.CheckpointTask;
import es.bsc.compss.checkpoint.types.request.ap.CheckpointerDataCopyEndedRequest;
import es.bsc.compss.checkpoint.types.request.ap.CheckpointerDataCopyFailedRequest;
import es.bsc.compss.checkpoint.types.request.ap.CheckpointerSaveLastDataVersionsRequest;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.Direction;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.DictCollectionParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.TreeMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CheckpointRecord {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.CP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String SEPARATOR = "#";
    private static final String DEFAULT_CHECKPOINT_FILE_NAME = "checkpoint.cp";

    private static final String CHECKPOINT_FOLDER;

    static {
        String folder = System.getProperty(COMPSsConstants.CHECKPOINT_FOLDER_PATH);
        if (!folder.endsWith(File.separator)) {
            folder = folder + File.separator;
        }
        CHECKPOINT_FOLDER = folder;
    }

    private final String checkpointFile;
    private final PrintWriter checkpointAppender;

    private final User cpUser;

    private int numCopiesRequested;
    private final CheckpointCopiesHandler cpHandler;
    private boolean isNotifyNoCopies;

    // Tasks executed and its state.
    private final Map<Integer, TaskState> checkpointedTasks;

    // Checkpointing information related to each data
    private final Map<Integer, CheckpointData> dataInfo;

    // Chekpointing information related to each data version
    private final Map<String, CheckpointDataVersion> dataVersion;


    /**
     * Constructs a new checkpoint record.
     * 
     * @param user Element using the checkpointing
     */
    public CheckpointRecord(User user) {
        this.cpUser = user;

        this.isNotifyNoCopies = false;
        numCopiesRequested = 0;

        this.checkpointedTasks = new TreeMap<>();
        this.dataInfo = new HashMap<>();
        this.dataVersion = new HashMap<>();

        this.checkpointFile = CHECKPOINT_FOLDER + DEFAULT_CHECKPOINT_FILE_NAME;
        File cpFolder = new File(CHECKPOINT_FOLDER);
        if (cpFolder.exists()) {
            try {
                loadCheckpointFile(checkpointFile);
            } catch (FileNotFoundException e) {
                LOGGER.warn("Checkpoint file not found");
            } catch (Exception e) {
                LOGGER.warn("Error reading checkpoint file.", e);
            }
        } else {
            if (cpFolder.mkdirs()) {
                LOGGER.debug("Created Checkpointing folder: " + CHECKPOINT_FOLDER);
            } else {
                ErrorManager.fatal("Could not create the directory");
            }
        }

        PrintWriter checkpointAppender;
        try {
            checkpointAppender = new PrintWriter(new FileWriter(checkpointFile, true));
        } catch (IOException ioe) {
            ErrorManager.fatal("Could not create checkpoint file", ioe);
            checkpointAppender = null;
        }
        this.checkpointAppender = checkpointAppender;
        this.cpHandler = new CheckpointCopiesHandler();

    }

    private void loadCheckpointFile(String cpFile) throws FileNotFoundException {
        File checkpoint = new File(cpFile);
        if (DEBUG) {
            LOGGER.debug("Loading checkpoint file " + cpFile);
        }
        Scanner reader = new Scanner(checkpoint);
        while (reader.hasNextLine()) {
            String data = reader.nextLine();
            String[] splited = data.split(SEPARATOR);
            switch (splited[0]) {
                case "D":
                    String dv = splited[1];
                    String location = splited[2];
                    DataType type = DataType.valueOf(splited[3]);
                    Boolean checkpointed = Boolean.valueOf(splited[4]);
                    CheckpointDataVersion cdvi;
                    cdvi = new CheckpointDataVersion(location, type, checkpointed);
                    this.dataVersion.put(dv, cdvi);
                    break;
                case "T":
                    int taskId = Integer.parseInt(splited[1]);
                    TaskState state = TaskState.valueOf(splited[2]);
                    this.checkpointedTasks.put(taskId, state);
                    break;
                default:
                    LOGGER.warn("Unknown checkpoint record type" + data);
            }
        }
        reader.close();
    }

    /**
     * Returns true if a task has been checkpointed.
     *
     * @param task Task to be evaluated.
     * @return {@literal true} if the task has been
     */
    public final boolean isTaskCheckpointed(Task task) {
        TaskState state = checkpointedTasks.get(task.getId());
        return state == TaskState.FINISHED || state == TaskState.RECOVERED;
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * ----------------------------------------------- Task Recovery -----------------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */

    protected final void recoverTask(Task t) {
        t.setStatus(TaskState.RECOVERED);
        for (Parameter param : t.getParameters()) {
            if (param.isPotentialDependency()) {
                recoverTaskParameter(param);
            }
        }
    }

    private void recoverTaskParameter(Parameter param) {
        DataType type = param.getType();

        if (type == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) param;
            for (Parameter sp : cp.getParameters()) {
                recoverTaskParameter(sp);
            }
        } else {
            if (type == DataType.DICT_COLLECTION_T) {
                DictCollectionParameter dcp = (DictCollectionParameter) param;
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    recoverTaskParameter(entry.getKey());
                    recoverTaskParameter(entry.getValue());
                }
            }
        }

        if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T) {
            DependencyParameter dp = ((DependencyParameter) param);
            recoverTaskSimpleTaskParameter(dp);
        }
    }

    private void recoverTaskSimpleTaskParameter(DependencyParameter dp) {
        DataAccessId paramId = dp.getDataAccessId();
        DataVersion outDV = null;
        if (paramId instanceof RWAccessId) {
            outDV = ((RWAccessId) paramId).getWrittenDataVersion();
        } else if (paramId instanceof WAccessId) {
            outDV = ((WAccessId) paramId).getWrittenDataVersion();
        }
        if (outDV != null) {
            DataInstanceId outDaId = outDV.getDataInstanceId();
            String outRename = outDaId.getRenaming();
            String shortRename = getShortName(outRename);

            CheckpointDataVersion cdvi = dataVersion.get(shortRename);
            if (cdvi != null) {
                String location = cdvi.getLocation();
                if (location != null) {
                    SimpleURI resultUri = new SimpleURI(location);

                    // Set type and data target as if it was the endTask function
                    dp.setType(cdvi.getType());
                    dp.setDataTarget(resultUri.toString());

                    // Increment the reader of the checkpointed task
                    outDV.willBeRead();

                    int outDataId = outDaId.getDataId();
                    CheckpointData cpi = dataInfo.get(outDataId);
                    if (cpi == null) {
                        cpi = new CheckpointData();
                        dataInfo.put(outDataId, cpi);
                    } else {
                        if (paramId.getDirection() == Direction.RW) {
                            // If we have a previous data from the INOUT decrease the readers
                            DataVersion prevDV = cpi.getLastCheckpointedVersion();
                            if (prevDV != null) {
                                prevDV.hasBeenRead();
                            }
                        }
                    }
                    // Update the last version of the data, for both INOUT and OUT
                    cpi.setLastCheckpointedVersion(outDV);

                    try {
                        // Set the location of the data, in the path location may not be the data (if it is an
                        // obsolete version)
                        DataLocation dataLocation = DataLocation.createLocation(Comm.getAppHost(), resultUri);
                        dataLocation.isCheckpointLocation(true);
                        LogicalData ld = Comm.getData(outRename);
                        ld.addLocation(dataLocation);
                    } catch (IOException e) {
                        e.printStackTrace();
                        ErrorManager.warn(DataLocation.ERROR_INVALID_LOCATION + " " + resultUri, e);
                    }
                }
            }
        }
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * --------------------------------------- Future values Registration ------------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */

    protected final void registerTask(Task t) {
        CheckpointTask ctl = new CheckpointTask(t, 0);
        for (Parameter param : t.getParameters()) {
            if (param instanceof DependencyParameter) {
                registerTaskParameter(param, ctl);
            }
        }
    }

    private void registerTaskParameter(Parameter param, CheckpointTask ctl) {
        DataType type = param.getType();

        if (type == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) param;
            for (Parameter sp : cp.getParameters()) {
                registerTaskParameter(sp, ctl);
            }
        } else if (type == DataType.DICT_COLLECTION_T) {
            DictCollectionParameter dcp = (DictCollectionParameter) param;
            for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                registerTaskParameter(entry.getKey(), ctl);
                registerTaskParameter(entry.getValue(), ctl);
            }
        }

        if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T) {
            DependencyParameter dp = (DependencyParameter) param;
            registerTaskSimpleParameter(dp, ctl);
        }
    }

    private void registerTaskSimpleParameter(DependencyParameter dp, CheckpointTask ctl) {

        DataAccessId paramId = dp.getDataAccessId();
        DataVersion outDV = null;
        if (paramId instanceof RWAccessId) {
            outDV = ((RWAccessId) paramId).getWrittenDataVersion();
        } else if (paramId instanceof WAccessId) {
            outDV = ((WAccessId) paramId).getWrittenDataVersion();
        }
        if (outDV != null) {
            ctl.addOutputToCheckpoint();
            CheckpointDataVersion cdvi = new CheckpointDataVersion(outDV, ctl);

            DataInstanceId outDaId = outDV.getDataInstanceId();
            String outRename = outDaId.getRenaming();
            String shortRename = getShortName(outRename);
            dataVersion.put(shortRename, cdvi);

            // Add two readers, one for the checkpointing and one for not deleting the data
            outDV.willBeRead();
            outDV.willBeRead();

            int outDataId = outDaId.getDataId();
            CheckpointData cpi = dataInfo.get(outDataId);
            if (cpi == null) {
                cpi = new CheckpointData();
                dataInfo.put(outDataId, cpi);
            }
            // Update last version of the data checkpointed
            cpi.setLastCheckpointedVersion(outDV);
        }

        DataInstanceId inDaId = null;
        if (paramId instanceof RWAccessId) {
            inDaId = ((RWAccessId) paramId).getReadDataInstance();
        } else if (paramId instanceof RAccessId) {
            inDaId = ((RAccessId) paramId).getReadDataInstance();
        }
        if (inDaId != null) {
            String inRename = inDaId.getRenaming();
            String shortRename = getShortName(inRename);
            CheckpointDataVersion cdvl = dataVersion.get(shortRename);
            if (cdvl != null) {
                ctl.addReadValue(cdvl);
                cdvl.addReader(ctl);
            }

        }
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * ------------------------------------------ Computed Values Update -------------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */

    /**
     * Registers the completion of a task and the existence of all its output values.
     *
     * @param t Completed task
     */
    protected final void completedTask(Task t) {
        for (Parameter param : t.getParameters()) {
            computedTaskParameter(param);
        }
        if (t.getStatus() == TaskState.RECOVERED) {
            checkpointedTasks.remove(t.getId());
        }
    }

    private void computedTaskParameter(Parameter param) {
        DataType type = param.getType();

        if (type == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) param;
            for (Parameter sp : cp.getParameters()) {
                computedTaskParameter(sp);
            }
        } else {
            if (type == DataType.DICT_COLLECTION_T) {
                DictCollectionParameter dcp = (DictCollectionParameter) param;
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    computedTaskParameter(entry.getKey());
                    computedTaskParameter(entry.getValue());
                }
            }
        }

        if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T) {
            DependencyParameter dp = (DependencyParameter) param;
            computedTaskSimpleParameter(dp);
        } else {
            LOGGER.warn("Checkpoint does nor support parameter type " + param.getType());
        }
    }

    private void computedTaskSimpleParameter(DependencyParameter dp) {
        DataAccessId paramId = dp.getDataAccessId();
        if (paramId instanceof RWAccessId || paramId instanceof WAccessId) {
            int dataId = paramId.getDataId();
            registerLastCompletedProducer(dataId, dp);
        }
    }

    private void registerLastCompletedProducer(Integer dataId, DependencyParameter dp) {
        CheckpointData cdi = dataInfo.get(dataId);
        if (cdi == null) {
            cdi = new CheckpointData();
            dataInfo.put(dataId, cdi);
        }
        cdi.setLastCompletedProducer(dp);
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * ---------------------------------------- Request Checkpointing Values ---------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */
    /**
     * Request the checkpoint a task saving all its output values that belong to a whitelist.
     * 
     * @param t task to checkpoint
     * @param allowed whitelist of values that can be checkpointed
     */
    protected final void requestTaskCheckpoint(Task t, List<DataVersion> allowed) {
        for (Parameter param : t.getParameters()) {
            requestTaskParameterCheckpoint(param, allowed);
        }
    }

    private void requestTaskParameterCheckpoint(Parameter param) {
        DataType type = param.getType();

        if (type == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) param;
            for (Parameter sp : cp.getParameters()) {
                requestTaskParameterCheckpoint(sp);
            }
        } else {
            if (type == DataType.DICT_COLLECTION_T) {
                DictCollectionParameter dcp = (DictCollectionParameter) param;
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    requestTaskParameterCheckpoint(entry.getKey());
                    requestTaskParameterCheckpoint(entry.getValue());
                }
            }
        }

        if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T) {
            DependencyParameter dp = (DependencyParameter) param;
            requestTaskSimpleParameterCheckpoint(dp);
        } else {
            LOGGER.warn("Checkpoint does nor support parameter type " + param.getType());
        }
    }

    private void requestTaskParameterCheckpoint(Parameter param, List<DataVersion> allowed) {
        DataType type = param.getType();

        if (type == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) param;
            for (Parameter sp : cp.getParameters()) {
                requestTaskParameterCheckpoint(sp, allowed);
            }
        } else {
            if (type == DataType.DICT_COLLECTION_T) {
                DictCollectionParameter dcp = (DictCollectionParameter) param;
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    requestTaskParameterCheckpoint(entry.getKey(), allowed);
                    requestTaskParameterCheckpoint(entry.getValue(), allowed);
                }
            }
        }

        if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T) {
            DependencyParameter dp = (DependencyParameter) param;
            requestTaskSimpleParameterCheckpoint(dp, allowed);
        } else {
            LOGGER.warn("Checkpoint does nor support parameter type " + param.getType());
        }
    }

    private void requestTaskSimpleParameterCheckpoint(DependencyParameter dp) {
        DataAccessId paramId = dp.getDataAccessId();
        DataVersion outDV = null;
        if (paramId instanceof RWAccessId) {
            outDV = ((RWAccessId) paramId).getWrittenDataVersion();
        } else {
            if (paramId instanceof WAccessId) {
                outDV = ((WAccessId) paramId).getWrittenDataVersion();
            }
        }
        if (outDV != null) {
            DataInstanceId outDaId = outDV.getDataInstanceId();
            String outRename = outDaId.getRenaming();
            String shortRename = getShortName(outRename);
            CheckpointDataVersion cdvi = dataVersion.get(shortRename);
            if (cdvi != null && !cdvi.getCheckpointRequested()) {
                DataType type = dp.getType();
                cdvi.generatedData(ProtocolType.FILE_URI.getSchema() + CHECKPOINT_FOLDER + outRename, type);
                saveData(outDaId, cdvi);
            }
        }
    }

    private void requestTaskSimpleParameterCheckpoint(DependencyParameter dp, List<DataVersion> allowed) {
        DataAccessId paramId = dp.getDataAccessId();
        DataVersion outDV = null;
        if (paramId instanceof RWAccessId) {
            outDV = ((RWAccessId) paramId).getWrittenDataVersion();
        } else {
            if (paramId instanceof WAccessId) {
                outDV = ((WAccessId) paramId).getWrittenDataVersion();
            }
        }
        if (outDV != null && allowed.contains(outDV)) {
            DataInstanceId outDaId = outDV.getDataInstanceId();
            String outRename = outDaId.getRenaming();
            String shortRename = getShortName(outRename);
            CheckpointDataVersion cdvi = dataVersion.get(shortRename);
            if (cdvi != null && !cdvi.getCheckpointRequested()) {
                DataType type = dp.getType();
                cdvi.generatedData(ProtocolType.FILE_URI.getSchema() + CHECKPOINT_FOLDER + outRename, type);
                saveData(outDaId, cdvi);
            }
        }
    }

    /**
     * Request for saving the data to checkpoint.
     *
     * @param cdvi CheckpointDataVersion belonging to the parameter.
     */
    private void saveData(DataInstanceId daId, CheckpointDataVersion cdvi) {
        DataLocation targetLocation = null;
        try {
            SimpleURI targetURI = new SimpleURI(cdvi.getLocation());
            targetLocation = DataLocation.createLocation(Comm.getAppHost(), targetURI);
            targetLocation.isCheckpointLocation(true);
        } catch (IOException ioe) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + CHECKPOINT_FOLDER, ioe);
        }

        cdvi.setCheckpointRequested();
        CheckpointCopyListener dataListener = new CheckpointCopyListener(cdvi);

        LogicalData srcData = daId.getData();

        this.numCopiesRequested++;
        this.cpHandler.requestCopy(srcData, targetLocation, dataListener);
    }


    private class CheckpointCopyListener extends EventListener {

        private final CheckpointDataVersion cdvi;


        public CheckpointCopyListener(CheckpointDataVersion cdvl) {
            this.cdvi = cdvl;
        }

        @Override
        public void notifyEnd(DataOperation fOp) {
            cpHandler.completedCopy();
            // Remove one reader from the data
            CheckpointerDataCopyEndedRequest request;
            request = new CheckpointerDataCopyEndedRequest(CheckpointRecord.this, cdvi);
            String dataRenaming = cdvi.getVersion().getDataInstanceId().getRenaming();
            cpUser.addCheckpointRequest(request, "checkpoint copy completion notification for data " + dataRenaming);
        }

        @Override
        public void notifyFailure(DataOperation fOp, Exception e) {
            cpHandler.completedCopy();
            CheckpointerDataCopyFailedRequest request;
            request = new CheckpointerDataCopyFailedRequest(CheckpointRecord.this, cdvi);
            String dataRenaming = cdvi.getVersion().getDataInstanceId().getRenaming();
            cpUser.addCheckpointRequest(request, "checkpoint copy failure notification for data " + dataRenaming);
        }
    }


    /**
     * Notifies that a requested copy to checkpoint a data has finished.
     *
     * @param cdvi Data Version tried to checkpoint
     */
    public final void dataCheckpointed(CheckpointDataVersion cdvi) {
        DataVersion dv = cdvi.getVersion();
        if (DEBUG) {
            LOGGER.warn("Checkpointing copy for  " + dv.getDataInstanceId().getRenaming() + " completed.");
        }
        commitDataVersionCheckpoint(cdvi);
        // Remove checkpoint copy additional reader
        dv.hasBeenRead();

        CheckpointData di = dataInfo.get(cdvi.getDataId());
        DataVersion prevCPVersion = di.getLastCheckpointedVersion();
        if (prevCPVersion != null) {
            DataInstanceId daId = dv.getDataInstanceId();
            DataInstanceId prevDaId = prevCPVersion.getDataInstanceId();
            if (prevDaId.getVersionId() < daId.getVersionId()) {

                // Remove last version checkpointed additional reader
                if (prevCPVersion.hasBeenRead()) {
                    String prevRenaming = prevDaId.getRenaming();
                    // If had a previous version remove data
                    if (DEBUG) {
                        LOGGER.debug("Removing previous checkpoint data" + prevRenaming);
                    }
                    Comm.removeData(prevRenaming, true);
                }
            }
        }
        di.addNotDeletedFinishedCopies();

        if (cdvi.valueCheckpointed()) {
            checkpointProducerTasks(cdvi);
        }

        this.numCopiesRequested--;
        if (this.isNotifyNoCopies && numCopiesRequested == 0) {
            cpUser.allAvailableDataCheckpointed();
            isNotifyNoCopies = false;
        }
    }

    /**
     * Notifies that a requested copy to checkpoint a data failed.
     *
     * @param cdvi Data Version tried to checkpoint
     */
    public final void dataCheckpointFailed(CheckpointDataVersion cdvi) {
        DataVersion dv = cdvi.getVersion();
        LOGGER.warn("Checkpointing copy for  " + dv.getDataInstanceId().getRenaming() + " failed.");

        this.numCopiesRequested--;
        if (this.isNotifyNoCopies && numCopiesRequested == 0) {
            cpUser.allAvailableDataCheckpointed();
            isNotifyNoCopies = false;
        }
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * ---------------------------------------- Checkpoint completed ----------------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */
    private void commitDataVersionCheckpoint(CheckpointDataVersion cdvi) {
        String shortName = getShortName(cdvi.getVersion().getDataInstanceId().getRenaming());
        String location = cdvi.getLocation();
        DataType type = cdvi.getType();
        boolean checkpointRequested = cdvi.getCheckpointRequested();
        // D#d1v2#location#type
        String dataRecord =
            "D" + SEPARATOR + shortName + SEPARATOR + location + SEPARATOR + type + SEPARATOR + checkpointRequested;
        appendRecord(dataRecord);
    }

    private void checkpointProducerTasks(CheckpointDataVersion cdvi) {
        CheckpointTask producer = cdvi.getProducer();
        if (producer != null) {
            DataVersion dv = cdvi.getVersion();
            if (producer.checkpointedOutput()) {
                Task task = producer.getTask();
                commitTaskCheckpoint(task.getId(), task.getStatus());
                for (CheckpointDataVersion readValue : producer.getReadValues()) {
                    if (readValue.readerCheckpointed(producer)) {
                        checkpointProducerTasks(readValue);
                    }
                }
            }

            deleteIntermidateVersionsRecursively(getShortName(dv.getDataInstanceId().getRenaming()), true);
        }
    }

    private void commitTaskCheckpoint(Integer taskId, TaskState state) {

        if (DEBUG) {
            LOGGER.debug("Task " + taskId + " has been checkpointed.");
        }

        if (state == TaskState.FINISHED || state == TaskState.RECOVERED) {
            // T#1#RECOVERED
            String taskRecord = "T" + SEPARATOR + taskId + SEPARATOR + state;
            appendRecord(taskRecord);
        }
    }

    private void appendRecord(String record) {
        if (checkpointAppender != null) {
            checkpointAppender.println(record);
            checkpointAppender.flush();
        }
    }

    /**
     * Deletes the previous obsolete version of a data recursively.
     *
     * @param shortName of the data to be eliminated
     * @param first Checks if it is the first deleted data
     */
    private void deleteIntermidateVersionsRecursively(String shortName, Boolean first) {
        CheckpointDataVersion cdvi = dataVersion.get(shortName);
        if (cdvi != null) {
            DataVersion version = cdvi.getVersion();
            if (version != null) {
                DataInstanceId daId = version.getDataInstanceId();
                int dataId = daId.getDataId();
                CheckpointData cdi = dataInfo.get(dataId);
                if (cdi.getNotDeletedFinishedCopies() > 1) {

                    if (cdvi.isCheckpointed() && cdvi.areReadersEmpty() && version.getNumberOfReaders() > 0
                        && daId.getVersionId() > 1) {
                        if (first) {
                            first = false;
                        } else {
                            LogicalData ld = daId.getData();
                            if (version.hasBeenRead() && !ld.isAccessedByMain()) {
                                String renaming = daId.getRenaming();
                                if (DEBUG) {
                                    LOGGER.debug("Removing data " + renaming + " because it became obsolete");
                                }
                                Comm.removeData(daId.getRenaming(), true);
                                cdi.removeNotDeletedFinishedCopies();
                            }
                        }
                    }
                    deleteIntermidateVersionsRecursively(getPreviousDv(shortName), first);
                }
            }
        }
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * --------------------------------------- Data Accesses from main ---------------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */
    /**
     * Marks data as accessed by the main, and therefore will not be deleted.
     *
     * @param di DataInstance to be marked.
     */
    public final void mainAccess(DataInstanceId di) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CHECKPOINT_MAIN_ACCESS);
        }
        LogicalData ld = di.getData();
        ld.setAccessedByMain(true);
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.CHECKPOINT_MAIN_ACCESS);
        }
    }

    /**
     * Notifies the Checkpoint Manager that a data has been deleted.
     *
     * @param data DataInfo to be deleted
     */
    public final void deletedData(DataInfo data) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CHECKPOINT_DELETE_DATA);
        }
        int dataId = data.getDataId();
        LOGGER.info("Deleting data " + dataId);
        CheckpointData cpi = dataInfo.get(dataId);
        if (cpi != null) {
            DataVersion dv = data.getCurrentDataVersion();
            if (dv != null && cpi.getLastCheckpointedVersion() != dv) {
                if (dv.hasBeenRead()) {
                    if (DEBUG) {
                        LOGGER.debug("Checkpointer deleting obsolete data " + dv.getDataInstanceId().getRenaming());
                    }
                    Comm.removeData(dv.getDataInstanceId().getRenaming(), true);
                }
            }

        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.CHECKPOINT_DELETE_DATA);
        }
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * --------------------------------------- Global state management ---------------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */
    /**
     * Save last data versions.
     */
    protected final void requestSaveLastDataVersions() {
        CheckpointerSaveLastDataVersionsRequest request = new CheckpointerSaveLastDataVersionsRequest(this);
        cpUser.addCheckpointRequest(request, "force checkpointing  all last data versions.");
    }

    /**
     * Requests the checkpointing of the last version of all the data produced by the execution.
     */
    public final void requestSaveAllLastDataVersion() {
        if (dataInfo.size() > 0) {
            for (CheckpointData cdi : dataInfo.values()) {
                Parameter param = cdi.getLastCompletedProducer();
                if (param != null) {
                    DependencyParameter dp = ((DependencyParameter) param);
                    this.requestTaskParameterCheckpoint(dp);
                }
            }
        }
    }

    /**
     * Requests the submission of all copies regardless the concurrency limit.
     */
    protected final void performAllCopies() {
        if (numCopiesRequested > 0) {
            this.cpHandler.ignoreConcurrencyLimit();
            this.isNotifyNoCopies = true;
        } else {
            cpUser.allAvailableDataCheckpointed();
        }
    }

    /*
     * -------------------------------------------------------------------------------------------------------------
     * ------------------------------------------- Auxiliar methods ------------------------------------------------
     * -------------------------------------------------------------------------------------------------------------
     */

    /**
     * Gives the previous short name of a data version.
     *
     * @param shortName of the data to get its previous version.
     * @return String containing the previous version
     */
    private String getPreviousDv(String shortName) {
        List<String> dataAndVersion = Arrays.asList(shortName.split("v"));
        return dataAndVersion.get(0) + "v" + (Integer.valueOf(dataAndVersion.get(1)) - 1);
    }

    /**
     * Returns the short name of a data (d1v2_12324.IT -> d1v2)
     *
     * @param rename Complete name of the data.
     */
    private static String getShortName(String rename) {
        return rename.substring(0, rename.indexOf("_"));
    }

}
