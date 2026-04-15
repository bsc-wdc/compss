/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.api.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.ParameterCollectionMonitor;
import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.access.BindingObjectMainAccess;
import es.bsc.compss.types.data.access.DirectoryMainAccess;
import es.bsc.compss.types.data.access.ExternalPSCObjectMainAccess;
import es.bsc.compss.types.data.access.FileMainAccess;
import es.bsc.compss.types.data.access.ObjectMainAccess;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.params.BindingObjectData;
import es.bsc.compss.types.data.params.CollectionData;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.data.params.FileData;
import es.bsc.compss.types.data.params.ObjectData;
import es.bsc.compss.types.parameter.impl.BasicTypeParameter;
import es.bsc.compss.types.parameter.impl.BindingObjectParameter;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DirectoryParameter;
import es.bsc.compss.types.parameter.impl.ExternalPSCOParameter;
import es.bsc.compss.types.parameter.impl.ExternalStreamParameter;
import es.bsc.compss.types.parameter.impl.FileParameter;
import es.bsc.compss.types.parameter.impl.ObjectParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.parameter.impl.StreamParameter;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourcesPool;
import es.bsc.compss.types.tracing.APIEvent;
import es.bsc.compss.types.tracing.APITracer;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.FileOpsManager;
import es.bsc.compss.worker.COMPSsException;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WorkflowImpl extends Application implements Workflow {

    // Error constants definition
    private static final String ERROR_BINDING_OBJECT_PARAMS =
        "ERROR: Incorrect number of parameters for external objects";
    private static final String ERROR_FILE_NAME = "ERROR: Cannot parse file name";
    private static final String ERROR_DIR_NAME = "ERROR: Not a valid directory";
    private static final String WARN_WRONG_DIRECTION = "WARNING: Invalid parameter direction: ";
    private static final String WARN_NULL_PARAM = "WARNING: Optional parameter: ";

    // Number of fields per parameter
    public static final int NUM_FIELDS_PER_PARAM = 9;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);
    // Data Provenance logger
    private static final Logger DP_LOGGER = LogManager.getLogger(Loggers.DATA_PROVENANCE);
    private static final boolean DP_ENABLED = Boolean.parseBoolean(System.getProperty(COMPSsConstants.DATA_PROVENANCE));

    private static AccessProcessor AP;
    // Language
    private static final COMPSsConstants.Lang DEFAULT_LANG;

    static {
        String defaultLang = System.getProperty(COMPSsConstants.LANG);
        COMPSsConstants.Lang lang;
        if (defaultLang == null) {
            lang = COMPSsConstants.Lang.JAVA;
        } else {
            lang = COMPSsConstants.Lang.valueOf(defaultLang.toUpperCase());
        }
        DEFAULT_LANG = lang;
    }


    public static void setAP(AccessProcessor ap) {
        WorkflowImpl.AP = ap;
    }

    public WorkflowImpl(String parallelismSource, ApplicationRunner runner) {
        super(parallelismSource, runner);
    }

    @Override
    public Long getId() {
        return super.getId();
    }

    @Override
    public void deregister(boolean deleteData) {
        APITracer.traced(APIEvent.DEREGISTER_APP, (Runnable) () -> {
            if (deleteData) {
                AP.deleteAllApplicationDataRequest(this);
            }
            super.deregister();
        });
    }

    /*
     * ************************************************************************************************************
     * **************************************** TASK MANAGEMENT ***************************************************
     * ************************************************************************************************************
     */

    @Override
    public void openTaskGroup(String groupName, boolean implicitBarrier) {
        APITracer.traced(APIEvent.OPEN_GROUP, (Runnable) () -> {
            AP.setCurrentTaskGroup(groupName, this);
        });
    }

    @Override
    public void closeTaskGroup(String groupName) {
        APITracer.traced(APIEvent.CLOSE_GROUP, (Runnable) () -> {
            AP.closeCurrentTaskGroup(this);
        });
    }

    @Override
    public int executeTask(String signature, OnFailure onFailure, int timeOut, boolean isPrioritary, int numNodes,
        boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget,
        Integer numReturns, int parameterCount, Object... parameters) {
        // Tracing flag for task creation
        return APITracer.traced(APIEvent.TASK, () -> {
            // Log the details
            LOGGER.info("Creating task from method " + signature + " for application " + this.getId());

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("There " + (parameterCount == 1 ? "is " : "are ") + parameterCount + " parameter"
                    + (parameterCount > 1 ? "s" : ""));
            }

            TaskMonitor monitor = this.getTaskMonitor();

            // Process the parameters
            List<Parameter> pars = processParameters(parameterCount, parameters, monitor);
            Integer nReturns = numReturns;
            if (nReturns == null) {
                nReturns = hasReturn(pars) ? 1 : 0;
            }

            int task = AP.newTask(this, monitor, signature, isPrioritary, numNodes, isReduce, reduceChunkSize,
                isReplicated, isDistributed, hasTarget, nReturns, pars, onFailure, timeOut);

            if (DP_ENABLED) {
                StringBuilder taskInfoBuilder = new StringBuilder("task " + task + " " + signature + " ");
                for (Parameter p : pars) {
                    taskInfoBuilder.append(p.getName()).append(".").append(p.getType().name()).append(".")
                        .append(p.getDirection().toString()).append("::");
                }
                String taskInfo = taskInfoBuilder.substring(0, taskInfoBuilder.length() - 2);
                DP_LOGGER.info(taskInfo);
            }

            for (Parameter p : pars) {
                if (p.getDirection().equals(Direction.IN_DELETE)) {
                    deleteParameter(p);
                }
            }
            // Return the taskId
            return task;
        });
    }

    private List<Parameter> processParameters(int parameterCount, Object[] parameters,
        ParameterCollectionMonitor monitors) {
        ArrayList<Parameter> pars = new ArrayList<>();
        // Parameter parsing needed, object is not serializable
        for (int paramIdx = 0; paramIdx < parameterCount; ++paramIdx) {
            int paramOffset = NUM_FIELDS_PER_PARAM * paramIdx;
            Object content = parameters[paramOffset];
            DataType type = (DataType) parameters[paramOffset + 1];
            if (type == null) {
                type = DataType.NULL_T;
            }
            Direction direction = (Direction) parameters[paramOffset + 2];
            StdIOStream stream = (StdIOStream) parameters[paramOffset + 3];
            String prefix = (String) parameters[paramOffset + 4];
            String name = (String) parameters[paramOffset + 5];
            String contentType = (String) parameters[paramOffset + 6];
            String wStr = EnvironmentLoader.loadFromEnvironment((String) parameters[paramOffset + 7]);
            double weight = Double.parseDouble(wStr);
            boolean keepRename = (Boolean) parameters[paramOffset + 8];
            // Add parameter to list
            // This function call is isolated for better readability and to easily
            // allow recursion in the case of collections
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(" Parameter " + paramIdx + " has type " + type.name());
            }
            ParameterMonitor monitor = monitors.getParameterMonitor(paramIdx);
            addParameter(monitor, content, type, direction, stream, prefix, name, contentType, weight, keepRename, pars,
                0, null);
        }

        // Return parameters
        return pars;
    }

    private int addParameter(ParameterMonitor monitor, Object content, DataType type, Direction direction,
        StdIOStream stream, String prefix, String name, String pyType, double weight, boolean keepRename,
        ArrayList<Parameter> pars, int offset, String[] vals) {
        String nameToPrint = name;
        if (name.contains(".")) {
            nameToPrint = name.substring(0, name.indexOf('.'));
        }
        switch (type) {
            case DIRECTORY_T:
                try {
                    String dirName = content.toString();
                    File dirFile = new File(dirName);
                    String originalName = dirFile.getName();
                    DataLocation location = createLocation(ProtocolType.DIR_URI, dirName);
                    pars.add(DirectoryParameter.newDP(this, direction, stream, prefix, name, pyType, weight, keepRename,
                        location, originalName, monitor));
                    if (DP_ENABLED) {
                        // Log access to directory in the dataprovenance.log
                        String finalPath = location.toString();
                        String pathToPrint = finalPath;
                        if (finalPath.startsWith("shared")) { // Need to fix URI from SharedDisks
                            Resource host = Comm.getAppHost();
                            String absolute = dirFile.getAbsolutePath();
                            String fixedFinalPath = "dir://" + host.getName() + absolute;
                            pathToPrint = fixedFinalPath;
                        }
                        DP_LOGGER
                            .info("file " + nameToPrint + " " + type + " " + pathToPrint + " " + direction.toString());
                    }
                } catch (Exception e) {
                    LOGGER.error(ERROR_DIR_NAME + " : " + e.getMessage());
                    ErrorManager.fatal(ERROR_DIR_NAME, e);
                }
                break;
            case FILE_T:
                try {
                    String fileName = content.toString();
                    File f = new File(fileName);
                    String originalName = f.getName();
                    DataLocation location = createLocation(ProtocolType.FILE_URI, content.toString());
                    pars.add(FileParameter.newFP(this, direction, stream, prefix, name, pyType, weight, keepRename,
                        location, originalName, monitor));
                    if (DP_ENABLED) {
                        // Log access to file in the dataprovenance.log.
                        // Corner case: PyCOMPSs objects are passed as files to the runtime
                        String finalPath = location.toString();
                        String pathToPrint = finalPath;
                        if (!finalPath.contains("tmpFiles/pycompss")) {
                            if (finalPath.startsWith("shared")) { // Need to fix URI from SharedDisks
                                Resource host = Comm.getAppHost();
                                String absolute = f.getAbsolutePath();
                                String fixedFinalPath = "file://" + host.getName() + absolute;
                                pathToPrint = fixedFinalPath;
                            }
                            DP_LOGGER.info(
                                "file " + nameToPrint + " " + type + " " + pathToPrint + " " + direction.toString());
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error(ERROR_FILE_NAME, e);
                    ErrorManager.fatal(ERROR_FILE_NAME, e);
                }
                break;
            case OBJECT_T:
            case PSCO_T:
                int code = System.identityHashCode(content);
                pars.add(ObjectParameter.newOP(this, direction, stream, prefix, name, pyType, weight, content, code,
                    monitor));
                break;
            case STREAM_T:
                int streamCode = System.identityHashCode(content);
                pars.add(StreamParameter.newSP(this, direction, stream, prefix, name, content, streamCode, monitor));
                break;
            case EXTERNAL_STREAM_T:
                try {
                    String fileName = content.toString();
                    DataLocation location = createLocation(ProtocolType.EXTERNAL_STREAM_URI, fileName);
                    String originalName = new File(fileName).getName();
                    pars.add(ExternalStreamParameter.newESP(this, direction, stream, prefix, name, location,
                        originalName, monitor));
                } catch (Exception e) {
                    LOGGER.error(ERROR_FILE_NAME, e);
                    ErrorManager.fatal(ERROR_FILE_NAME, e);
                }
                break;
            case EXTERNAL_PSCO_T:
                String id = content.toString();
                int pscoCode = externalObjectHashcode(id);
                pars.add(ExternalPSCOParameter.newEPOP(this, direction, stream, prefix, name, weight, id, pscoCode,
                    monitor));
                break;
            case BINDING_OBJECT_T:
                String value = content.toString();
                if (value.contains(":")) {
                    String[] fields = value.split(":");
                    if (fields.length == 3) {
                        String extObjectId = fields[0];
                        int extObjectType = Integer.parseInt(fields[1]);
                        int extObjectElements = Integer.parseInt(fields[2]);
                        int boCode = externalObjectHashcode(extObjectId);
                        pars.add(BindingObjectParameter.newBOP(this, direction, stream, prefix, name, pyType, weight,
                            new BindingObject(extObjectId, extObjectType, extObjectElements), boCode, monitor));
                    } else {
                        LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    }
                } else {
                    LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                }
                break;
            case COLLECTION_T:
                // A collection value contains the file of the collection object and the collection
                // elements, separated by spaces
                String[] values = vals == null ? ((String) content).split(" ") : vals;
                final String collectionId = values[offset];
                int numOfElements = Integer.parseInt(values[offset + 1]);
                String colPyType = values[offset + 2];
                // The elements of the collection are all the elements of the list except for the first one
                // Each element is defined by TYPE VALUE PYTHON_CONTENT_TYPE
                // Also note the +3 offset!
                List<DataType> contentTypes = new ArrayList<>();
                List<String> contentIds = new ArrayList<>();
                ArrayList<Parameter> collectionParameters = new ArrayList<>();
                // Ret = number of read elements by this recursive step (atm 3: id + numOfElements + pyContentType)
                int ret = 3;
                for (int j = 0; j < numOfElements; ++j) {
                    // First element is the type, translate it to the corresponding DataType field by direct indexing
                    int idx = Integer.parseInt(values[offset + ret]);
                    final DataType dataType = DataType.values()[idx];
                    contentTypes.add(dataType);
                    // Second element is the content
                    contentIds.add(values[offset + ret + 1]);
                    final DataType elemType = contentTypes.get(j);
                    final Direction elemDir = direction;
                    // Third element is the Python type of the object
                    final String elemPyType = values[offset + ret + 2];
                    // Prepare stuff for recursive call
                    final Object elemContent = elemType == DataType.COLLECTION_T ? values : contentIds.get(j);
                    // N/A to non-direct parameters
                    final StdIOStream elemStream = StdIOStream.UNSPECIFIED;
                    final String elemPrefix = Constants.PREFIX_EMPTY;
                    String elemName = name + "." + j;
                    // Add @ only for the first time
                    // This names elements as @collection.0, @collection.1, etc
                    // Easily extended in the case of nested collections
                    // @collection.1.0.1.2
                    // Means that this is the third element of the second element of the first element
                    // of the named collection "collection1"
                    ParameterMonitor submonitor = ((ParameterCollectionMonitor) monitor).getParameterMonitor(j);
                    ret += addParameter(submonitor, elemContent, elemType, elemDir, elemStream, elemPrefix, elemName,
                        elemPyType, weight, keepRename, collectionParameters, offset + ret + 1, values) + 2;
                }
                CollectiveParameter cp = CollectiveParameter.newCP(this, type, collectionId, direction, stream, prefix,
                    name, colPyType, weight, keepRename, monitor, collectionParameters);
                pars.add(cp);
                return ret;
            case DICT_COLLECTION_T:
                // TODO: Simplify this case.
                // A dictionary collection value contains the file of the dictionary collection object
                // and the dictionary collection elements, separated by spaces
                String[] values1 = vals == null ? (content.toString()).split(" ") : vals;
                String dictCollectionId = values1[offset];
                int numOfEntries = Integer.parseInt(values1[offset + 1]);
                String dictColPyType = values1[offset + 2];
                // Each element is defined by TYPE VALUE PYTHON_CONTENT_TYPE. Also note the +3 offset!
                ArrayList<Parameter> dictCollectionParams = new ArrayList<>();
                // dret = number of read elements by this recursive step (atm 3: id + numOfEntries + pyContentType)
                int pointer = 3;
                for (int j = 0; j < numOfEntries; ++j) {
                    // First element is the type, translate it to the corresponding DataType field by direct indexing
                    int idKey = Integer.parseInt(values1[offset + pointer]);
                    DataType dataTypeKey = DataType.values()[idKey];
                    // Second element is the content
                    String contentKey = values1[offset + pointer + 1];
                    // Third element is the Python type of the object
                    final String elemPyTypeKey = values1[offset + pointer + 2];

                    // N/A to non-direct parameters
                    final StdIOStream elemStreamKey = StdIOStream.UNSPECIFIED;
                    final String elemPrefixKey = Constants.PREFIX_EMPTY;

                    String elemNameKey = name + "." + j;
                    // Add @key only for the first time - as in collections
                    if (!elemNameKey.startsWith("@key")) {
                        elemNameKey = "@key" + elemNameKey;
                    }
                    Direction elemDirKey = direction;

                    // Key element recursive call
                    Object elemContentKey = contentKey;
                    int extraKey = 2;
                    if (dataTypeKey == DataType.DICT_COLLECTION_T || dataTypeKey == DataType.COLLECTION_T) {
                        elemContentKey = values1;
                        pointer += 1;
                        extraKey = 0;
                    }
                    ParameterMonitor submonitor = ((ParameterCollectionMonitor) monitor).getParameterMonitor(j * 2);
                    int kDret = addParameter(submonitor, elemContentKey, dataTypeKey, elemDirKey, elemStreamKey,
                        elemPrefixKey, elemNameKey, elemPyTypeKey, weight, keepRename, dictCollectionParams,
                        offset + pointer, values1) + extraKey;
                    pointer += kDret;

                    // Next three elements correspond to the VALUE
                    // First element is the type, translate it to the corresponding DataType field by direct indexing
                    int idValue = Integer.parseInt(values1[offset + pointer]);
                    DataType dataTypeValue = DataType.values()[idValue];
                    // Second element is the content
                    String contentValue = values1[offset + pointer + 1];
                    // Third element is the Python type of the object
                    final String elemPyTypeValue = values1[offset + pointer + 2];

                    // N/A to non-direct parameters
                    final StdIOStream elemStreamValue = StdIOStream.UNSPECIFIED;
                    final String elemPrefixValue = Constants.PREFIX_EMPTY;

                    String elemNameValue = name + "." + j;
                    // Add @key only for the first time - as in collections
                    if (!elemNameValue.startsWith("@value")) {
                        elemNameValue = "@value" + elemNameKey;
                    }
                    Direction elemDirValue = direction;

                    // Value element recursive call
                    Object elemContentValue = contentValue;
                    int extraValue = 2;
                    if (dataTypeValue == DataType.DICT_COLLECTION_T || dataTypeValue == DataType.COLLECTION_T) {
                        elemContentValue = values1;
                        pointer += 1;
                        extraValue = 0;
                    }
                    submonitor = ((ParameterCollectionMonitor) monitor).getParameterMonitor(j * 2 + 1);
                    int vDret = addParameter(submonitor, elemContentValue, dataTypeValue, elemDirValue, elemStreamValue,
                        elemPrefixValue, elemNameValue, elemPyTypeValue, weight, keepRename, dictCollectionParams,
                        offset + pointer, values1) + extraValue;
                    pointer += vDret;
                }
                CollectiveParameter dcp = CollectiveParameter.newCP(this, type, dictCollectionId, direction, stream,
                    prefix, name, dictColPyType, weight, keepRename, monitor, dictCollectionParams);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Add Dictionary Collection " + dcp.getName() + " with " + dcp.getElements().size() / 2
                        + " entries");
                    LOGGER.debug(dcp.toString());
                }
                pars.add(dcp);
                return pointer;
            case NULL_T:
                LOGGER.warn(WARN_NULL_PARAM + "Parameter " + name + " is defined as None or Null");
                pars.add(BasicTypeParameter.newBP(type, Direction.IN, stream, prefix, name, content, weight, "null",
                    monitor));
                break;
            default:
                // Basic types (including String)
                // The only possible direction is IN, warn otherwise
                if (direction != Direction.IN && direction != Direction.IN_DELETE) {
                    LOGGER.warn(WARN_WRONG_DIRECTION + "Parameter " + name
                        + " is a basic type, therefore it must have IN direction");
                }
                pars.add(BasicTypeParameter.newBP(type, Direction.IN, stream, prefix, name, content, weight, pyType,
                    monitor));
                break;
        }
        return 1;
    }

    private boolean hasReturn(List<Parameter> parameters) {
        boolean hasReturn = false;
        if (!parameters.isEmpty()) {
            Parameter lastParam = parameters.get(parameters.size() - 1);
            DataType type = lastParam.getType();
            hasReturn = (lastParam.getDirection() == Direction.OUT && (type == DataType.OBJECT_T
                || type == DataType.PSCO_T || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T));
        }

        return hasReturn;
    }

    private void deleteParameter(Parameter p) {
        if (p.isCollective()) {
            for (Parameter sp : ((CollectiveParameter) p).getElements()) {
                deleteParameter(sp);
            }
        } else {
            switch (p.getType()) {
                case DIRECTORY_T:
                case FILE_T:
                case BINDING_OBJECT_T:
                    AP.deleteData(this, p.getAccess().getData(), false, false);
                    break;
                default:
                    // Do nothing
            }
        }
    }

    @Override
    public void cancelTaskGroup(String groupName) throws COMPSsException {
        APITracer.traced(APIEvent.CANCEL_GROUP, (APITracer.ThrowingRunnable) () -> {
            AP.cancelTaskGroup(this, groupName);
            // This is required that changes in metadata have been applied before
            // generating new tasks
            AP.barrierGroup(this, groupName);
        });
    }

    @Override
    public void cancelApplicationTasks() {
        AP.cancelApplicationTasks(this);
    }

    @Override
    public void barrierGroup(String groupName) throws COMPSsException {
        APITracer.traced(APIEvent.WAIT_FOR_GROUP_TASKS, (APITracer.ThrowingRunnable) () -> {
            // Regular barrier
            AP.barrierGroup(this, groupName);
        });
    }

    @Override
    public void barrier() {
        barrier(false);
    }

    @Override
    public void barrier(boolean noMoreTasks) {
        APITracer.traced(APIEvent.WAIT_FOR_ALL_TASKS, (Runnable) () -> {
            // Wait until all tasks have finished
            LOGGER.info("Barrier for app " + this.getId() + " with noMoreTasks = " + noMoreTasks);
            if (noMoreTasks) {
                // No more tasks expected, we can unregister application
                handleNoMoreTasks();
            } else {
                // Regular barrier
                AP.barrier(this);
            }
        });
    }

    @Override
    public void noMoreTasks() {
        APITracer.traced(APIEvent.NO_MORE_TASKS, (Runnable) () -> {
            handleNoMoreTasks();
        });
    }

    /**
     * Notifies the runtime that an application will not produce more tasks.
     */
    public void handleNoMoreTasks() {
        LOGGER.info("No more tasks for app " + this.getId());
        // Wait until all tasks have finished
        AP.noMoreTasks(this);

        // Retrieve result files
        LOGGER.debug("Getting Result Files for app" + this.getId());
        AP.getResultFiles(this);

    }

    /*
     * ************************************************************************************************************
     * **************************************** DATA MANAGEMENT ***************************************************
     * ************************************************************************************************************
     */

    @Override
    public void registerData(DataType type, Object stub, String data) {
        APITracer.traced(APIEvent.REGISTER_DATA, (Runnable) () -> {
            DataParams dp = null;
            switch (type) {
                case DIRECTORY_T:
                case FILE_T:
                    try {
                        String fileName = (String) stub;
                        // Parse arguments to internal structures
                        DataLocation loc;
                        try {
                            loc = createLocation(ProtocolType.FILE_URI, fileName);
                        } catch (IOException ioe) {
                            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
                            return;
                        }
                        dp = new FileData(loc);
                    } catch (NullPointerException npe) {
                        LOGGER.error(ERROR_FILE_NAME, npe);
                        ErrorManager.fatal(ERROR_FILE_NAME, npe);
                    }
                    break;
                case OBJECT_T:
                case PSCO_T:
                    int hashcode = System.identityHashCode(stub);
                    dp = new ObjectData(hashcode);
                    break;
                case STREAM_T:
                    // int streamCode = System.identityHashCode(stub);
                    throw new UnsupportedOperationException("Not implemented yet.");
                case EXTERNAL_STREAM_T:
                    try {
                        String fileName = (String) stub;
                        new File(fileName).getName();
                    } catch (NullPointerException npe) {
                        LOGGER.error(ERROR_FILE_NAME, npe);
                        ErrorManager.fatal(ERROR_FILE_NAME, npe);
                    }
                    throw new UnsupportedOperationException("Not implemented yet.");
                case EXTERNAL_PSCO_T:
                    // String id = (String) stub;
                    throw new UnsupportedOperationException("Not implemented yet.");
                case BINDING_OBJECT_T:
                    String value = (String) stub;
                    if (value.contains(":")) {
                        String[] fields = value.split(":");
                        // if (fields.length == 3) {
                        // String extObjectId = fields[0];
                        // int extObjectType = Integer.parseInt(fields[1]);
                        // int extObjectElements = Integer.parseInt(fields[2]);
                        // BindingObject bo = new BindingObject(extObjectId, extObjectType, extObjectElements);
                        // new BindingObject(extObjectId, extObjectType, extObjectElements);
                        // int externalCode = externalObjectHashcode(extObjectId);
                        // externalObjectHashcode(extObjectId);
                        // }
                        if (fields.length != 3) {
                            LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                            ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        }
                    } else {
                        LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    }
                    throw new UnsupportedOperationException("Not implemented yet.");
                case COLLECTION_T:
                    dp = new CollectionData((String) stub);
                    break;
                case DICT_COLLECTION_T:
                    throw new UnsupportedOperationException("Not implemented yet.");
                default:
                    // Basic types (including String)
                    // Already passed in as a value
                    break;
            }
            if (dp != null) {
                AP.registerRemoteData(this, dp, data);
            }
        });
    }

    @Override
    public boolean bindExistingVersionToData(String fileName, String dataId) {
        return APITracer.traced(APIEvent.BIND_DATA_TO_VERSION, () -> {
            // Parse the file name
            DataLocation sourceLocation = null;
            try {
                sourceLocation = createLocation(ProtocolType.FILE_URI, fileName);
            } catch (IOException ioe) {
                ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            }
            if (sourceLocation == null) {
                ErrorManager.fatal(ERROR_FILE_NAME);
            }

            FileData fd = new FileData(sourceLocation);
            return bindExistingVersionToData(fd, dataId);
        });
    }

    @Override
    public boolean bindExistingVersionToData(Object o, String dataId) {
        return APITracer.traced(APIEvent.BIND_DATA_TO_VERSION, () -> {
            int hashCode = System.identityHashCode(o);
            ObjectData od = new ObjectData(hashCode);
            return bindExistingVersionToData(od, dataId);
        });
    }

    private boolean bindExistingVersionToData(DataParams data, String dataId) {
        LOGGER.debug("Binding " + data.getDescription() + "'s last version to data " + dataId);
        LogicalData lastVersion = AP.getDataLastVersion(this, data);
        if (lastVersion != null) {
            LogicalData src = Comm.getData(dataId);
            try {
                LOGGER.debug("Binding " + src.getKnownAlias() + " to data " + dataId);
                LogicalData.link(src, lastVersion);
                return true;
            } catch (CommException e) {
                LOGGER.warn("Could not link " + dataId + " and " + lastVersion.getName());
            }
        }
        return false;
    }

    @Override
    public boolean isFileAccessed(String fileName) {
        return APITracer.traced(APIEvent.CHECK_FILE, () -> {
            DataLocation loc;
            try {
                loc = createLocation(ProtocolType.FILE_URI, fileName);
            } catch (IOException ioe) {
                ErrorManager.fatal(ERROR_FILE_NAME, ioe);
                loc = null;
            }
            if (loc != null) {
                FileData fd = new FileData(loc);
                return AP.alreadyAccessed(this, fd);
            } else {
                return false;
            }
        });
    }

    @Override
    public String openFile(String fileName, Direction mode) {
        return APITracer.traced(APIEvent.OPEN_FILE, () -> {
            return openFileSystemData(fileName, mode, false);
        });
    }

    @Override
    public void getFile(String fileName) {
        APITracer.traced(APIEvent.GET_FILE, (Runnable) () -> {
            // Parse the file name
            DataLocation sourceLocation = null;
            try {
                sourceLocation = createLocation(ProtocolType.FILE_URI, fileName);
            } catch (IOException ioe) {
                ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            }
            if (sourceLocation == null) {
                ErrorManager.fatal(ERROR_FILE_NAME);
            }

            LOGGER.debug("Getting file " + fileName);
            String renamedPath = openFileSystemData(fileName, Direction.INOUT, false);
            // If renamePth is the same as original, file has not accessed. Nothing to do.
            if (!renamedPath.equals(sourceLocation.getPath())) {
                try {
                    String intermediateTmpPath = renamedPath + ".tmp";
                    FileOpsManager.moveSync(new File(renamedPath), new File(intermediateTmpPath));
                    closeFileData(fileName, Direction.INOUT);
                    AP.deleteData(this, new FileData(sourceLocation), true, false);
                    FileOpsManager.moveSync(new File(intermediateTmpPath), new File(fileName));
                } catch (IOException ioe) {
                    LOGGER.error("Move not possible ", ioe);
                }
            }
        });
    }

    @Override
    public void closeFile(String fileName, Direction mode) {
        APITracer.traced(APIEvent.CLOSE_FILE, (Runnable) () -> {
            closeFileData(fileName, mode);
        });
    }

    @Override
    public boolean deleteFile(String fileName, boolean waitForData, boolean applicationDelete) {
        return APITracer.traced(APIEvent.DELETE_FILE, () -> {
            // Check parameters
            if (fileName == null || fileName.isEmpty()) {
                return false;
            }

            LOGGER.info("Deleting File " + fileName + " with wait for data " + waitForData);

            // Parse the file name and translate the access mode
            try {
                DataLocation loc = createLocation(ProtocolType.FILE_URI, fileName);
                AP.deleteData(this, new FileData(loc), waitForData, applicationDelete);
            } catch (IOException ioe) {
                ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            }
            LOGGER.info("File " + fileName + " Deleted.");
            // Return deletion was successful
            return true;
        });
    }

    @Override
    public void getDirectory(String dirName) {
        APITracer.traced(APIEvent.GET_DIRECTORY, (Runnable) () -> {
            // Parse the dir name
            DataLocation sourceLocation = null;
            try {
                sourceLocation = createLocation(ProtocolType.DIR_URI, dirName);
            } catch (IOException ioe) {
                ErrorManager.fatal(ERROR_DIR_NAME, ioe);
            }
            if (sourceLocation == null) {
                ErrorManager.fatal(ERROR_DIR_NAME);
            }

            LOGGER.debug("Getting directory " + dirName);
            String renamedPath = openFileSystemData(dirName, Direction.IN, true);
            try {
                LOGGER.debug("Getting directory renamed path: " + renamedPath);
                String intermediateTmpPath = renamedPath + ".tmp";
                FileOpsManager.moveDirSync(new File(renamedPath), new File(intermediateTmpPath));
                closeFileData(dirName, Direction.IN);

                AP.deleteData(this, new FileData(sourceLocation), true, false);
                FileOpsManager.moveDirSync(new File(intermediateTmpPath), new File(dirName));
            } catch (IOException ioe) {
                LOGGER.error("Move not possible ", ioe);
            }
        });
    }

    @Override
    public <T> T getObject(T obj) {
        /*
         * We know that the object has been accessed before by a task, otherwise the ObjectRegistry would have discarded
         * it and this method would not have been called.
         */
        return APITracer.traced(APIEvent.GET_OBJECT, () -> {
            int hashCode = System.identityHashCode(obj);
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Getting object with hash code " + hashCode);
            }

            ObjectMainAccess<T, ?, ?> oap = ObjectMainAccess.constructOMA(this, Direction.INOUT, obj, hashCode);
            T oUpdated;
            try {
                oUpdated = AP.mainAccess(oap);
            } catch (ValueUnawareRuntimeException e) {
                oUpdated = null;
            }
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Object obtained " + ((oUpdated == null) ? oUpdated : oUpdated.hashCode()));
            }

            return oUpdated;
        });
    }

    @Override
    public String getBindingObject(String fileName) {
        return APITracer.traced(APIEvent.GET_BINDING_OBJECT, () -> {
            // Parse the file name
            LOGGER.debug(" Calling get binding object : " + fileName);
            BindingObject bo = BindingObject.generate(fileName);
            BindingObjectLocation boLoc = new BindingObjectLocation(Comm.getAppHost(), bo);
            String boId = boLoc.getId();
            int hashCode = externalObjectHashcode(boId);
            BindingObjectMainAccess boap = BindingObjectMainAccess.constructBOMA(this, Direction.INOUT, bo, hashCode);

            // Otherwise we request it from a task
            String finalPath;
            try {
                BindingObject newBO = AP.mainAccess(boap);
                String bindingObjectID = newBO.getName();
                finalPath = bindingObjectID;
            } catch (ValueUnawareRuntimeException e) {
                finalPath = bo.toString();
            }
            LOGGER.debug("Returning binding object as id: " + finalPath);
            return finalPath;
        });
    }

    @Override
    public boolean removeObject(Object o) {
        APITracer.traced(APIEvent.DELETE_OBJECT, (Runnable) () -> {
            int hashcode = System.identityHashCode(o);
            // This will remove the object from the Object Registry and the Data Info Provider
            // eventually allowing the garbage collector to free it (better use of memory)
            AP.deleteData(this, new ObjectData(hashcode), false, false);
        });
        return true;
    }

    @Override
    public boolean deleteBindingObject(String fileName) {
        // Emit event
        return APITracer.traced(APIEvent.DELETE_BIND_OBJECT, () -> {
            if (fileName == null || fileName.isEmpty()) {
                return false;
            }

            LOGGER.info("Deleting BindingObject " + fileName);

            // Parse the binding object name and translate the access mode
            BindingObject bo = BindingObject.generate(fileName);
            int boCode = externalObjectHashcode(bo.getId());
            AP.deleteData(this, new BindingObjectData(boCode), false, false);

            // Return deletion was successful
            return true;
        });
    }

    @Override
    public void snapshot() {
        APITracer.traced(APIEvent.SNAPSHOT_API, (Runnable) () -> {
            // Wait until all tasks have finished
            LOGGER.info("Requesting snapshot for application " + this.getId());
            AP.snapshot(this);
        });
    }

    /*
     * ************************************************************************************************************
     * ***************************************** HELPER METHODS ***************************************************
     * ************************************************************************************************************
     */

    private String openFileSystemData(String fileName, Direction direction, boolean isDir) {
        LOGGER.info("Opening " + fileName + " in direction " + direction);
        // Parse arguments to internal structures
        DataLocation loc;
        try {
            loc = createLocation(isDir ? ProtocolType.DIR_URI : ProtocolType.FILE_URI, fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            return null;
        }

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        String finalPath;
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                FileMainAccess<?, ?> access;
                if (isDir) {
                    access = DirectoryMainAccess.constructDMA(this, direction, loc);
                } else {
                    access = FileMainAccess.constructFMA(this, direction, loc);
                }
                finalPath = mainAccessToFile(access, fileName);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("File " + (isDir ? "(dir) " : "") + "target Location: " + finalPath);
                }
                break;
            case PERSISTENT:
                String id = ((PersistentLocation) loc).getId();
                int ePscoHashcode = externalObjectHashcode(id);
                ExternalPSCObjectMainAccess eoap;
                eoap = ExternalPSCObjectMainAccess.constructEPOMA(this, Direction.INOUT, id, ePscoHashcode);

                // Otherwise we request it from a task
                try {
                    String newPscoId = AP.mainAccess(eoap);
                    finalPath = ProtocolType.PERSISTENT_URI.getSchema() + newPscoId;
                } catch (ValueUnawareRuntimeException e) {
                    finalPath = id;
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("External PSCO target Location: " + finalPath);
                }
                break;

            default:
                finalPath = null;
                ErrorManager.error(
                    "ERROR: Unrecognised protocol requesting " + (isDir ? "openDirectory " : "openFile ") + fileName);
        }
        return finalPath;
    }

    private void closeFileData(String fileName, Direction direction) {
        LOGGER.info("Closing " + fileName + " in direction " + direction);

        // Parse arguments to internal structures
        DataLocation loc;
        try {
            loc = createLocation(ProtocolType.FILE_URI, fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
            return;
        }

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                FileMainAccess fma = FileMainAccess.constructFMA(this, direction, loc);
                AP.finishDataAccess(fma, null);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Closing file " + loc.getPath());
                }
                break;
            case PERSISTENT:
                // Nothing to do
                ErrorManager.warn("WARN: Cannot close file " + fileName + " with PSCO protocol");
                break;
            case BINDING:
                // Nothing to do
                ErrorManager.warn("WARN: Cannot close binding object " + fileName + " with PSCO protocol");
                break;
            default:
                ErrorManager.error("ERROR: Unrecognised protocol requesting closeFile " + fileName);
        }
    }

    /**
     * Computes the HashCode identifier for an external Binding object.
     *
     * @param id identifier for an external BO
     * @return hashcode of the identifier
     */
    private static int externalObjectHashcode(String id) {
        int hashCode = 7;
        for (int i = 0; i < id.length(); ++i) {
            hashCode = hashCode * 31 + id.charAt(i);
        }

        return hashCode;
    }

    private static String mainAccessToFile(FileMainAccess<?, ?> access, String fileName) {
        // Tell the AP that the application wants to access a file.
        DataLocation targetLocation;
        try {
            targetLocation = AP.mainAccess(access);
        } catch (ValueUnawareRuntimeException ex) {
            targetLocation = access.getParameters().getLocation();
        }

        // Checks on target
        String path = (targetLocation == null) ? fileName : targetLocation.getPath();
        DataLocation finalLocation = (targetLocation == null) ? access.getParameters().getLocation() : targetLocation;
        if (finalLocation == null) {
            ErrorManager.fatal(ERROR_FILE_NAME);
            return null;
        }

        // Return the final target path
        String finalPath;
        MultiURI u = finalLocation.getURIInHost(Comm.getAppHost());
        if (u != null) {
            finalPath = u.getPath();
        } else {
            finalPath = path;
        }

        return finalPath;
    }

    /**
     * Creates a location from an URI as a string.
     *
     * @param defaultSchema schema, if not indicated in the URI
     * @param uri uri to create a Data Location
     * @return DataLocation equivalent to the URI
     * @throws IOException cannot convert to SimpleURI
     */
    private static DataLocation createLocation(ProtocolType defaultSchema, String uri) throws IOException {
        // Check if fileName contains schema
        SimpleURI sURI = new SimpleURI(uri);

        // Check host
        Resource host;
        String hostName = sURI.getHost();
        host = Comm.getAppHost();
        if (hostName != null && !hostName.isEmpty()) {
            Resource uriHost = ResourcesPool.getResource(hostName);
            if (uriHost == null) {
                ErrorManager.error("Host " + hostName + " not found when creating data location.");
            } else {
                host = uriHost;
                uri = sURI.getPath();
            }
        }

        if (sURI.getSchema().isEmpty()) {
            if (uri.startsWith("/")) {
                // todo: make pretty and sure it works
                sURI = new SimpleURI(defaultSchema.getSchema() + uri);
            } else {
                // Add default File scheme and wrap local paths
                String canonicalPath = new File(uri).getCanonicalPath();
                sURI = new SimpleURI(defaultSchema.getSchema() + canonicalPath);
            }
        }

        // Create location
        return DataLocation.createLocation(host, sURI);
    }
}
