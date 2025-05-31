/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.nio.worker;

import es.bsc.comm.Connection;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsConstants.TaskExecution;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.data.DataManager;
import es.bsc.compss.data.DataProvider;
import es.bsc.compss.data.FetchDataListener;
import es.bsc.compss.data.MultiOperationFetchListener;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.invokers.types.JavaParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.invokers.types.RParams;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOData;
import es.bsc.compss.nio.NIOMessageHandler;
import es.bsc.compss.nio.NIOParam;
import es.bsc.compss.nio.NIOParamCollection;
import es.bsc.compss.nio.NIOResult;
import es.bsc.compss.nio.NIOResultCollection;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.NIOTaskProfile;
import es.bsc.compss.nio.NIOTaskResult;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.nio.commands.CommandCancelTask;
import es.bsc.compss.nio.commands.CommandDataReceived;
import es.bsc.compss.nio.commands.CommandExecutorShutdown;
import es.bsc.compss.nio.commands.CommandExecutorShutdownACK;
import es.bsc.compss.nio.commands.CommandNIOTaskDone;
import es.bsc.compss.nio.commands.CommandNewTask;
import es.bsc.compss.nio.commands.CommandRemoveObsoletes;
import es.bsc.compss.nio.commands.CommandShutdown;
import es.bsc.compss.nio.commands.CommandShutdownACK;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFiles;
import es.bsc.compss.nio.commands.tracing.CommandGenerateAnalysisFilesDone;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFiles;
import es.bsc.compss.nio.commands.workerfiles.CommandGenerateDebugFilesDone;
import es.bsc.compss.nio.datarequest.WorkerDataRequest;
import es.bsc.compss.nio.exceptions.DataNotAvailableException;
import es.bsc.compss.nio.listeners.FetchDataOperationListener;
import es.bsc.compss.nio.listeners.TaskExecutionListener;
import es.bsc.compss.nio.listeners.TaskFetchOperationsListener;
import es.bsc.compss.nio.requests.DataRequest;
import es.bsc.compss.nio.worker.components.DataManagerImpl;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationExecutionRequest;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.LanguageParams;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.NonExistentDataException;
import es.bsc.compss.types.execution.exceptions.UnloadableValueException;
import es.bsc.compss.types.execution.exceptions.UnwritableValueException;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.utils.execution.ExecutionManager;
import es.bsc.compss.utils.execution.ThreadedPrintStream;
import es.bsc.compss.worker.COMPSsException;
import es.bsc.distrostreamlib.server.types.StreamBackend;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StubItf;


public class NIOWorker extends NIOAgent implements InvocationContext, DataProvider {

    // Logger
    private static final Logger WORKER_LOGGER = LogManager.getLogger(Loggers.WORKER);
    private static final boolean WORKER_LOGGER_DEBUG = WORKER_LOGGER.isDebugEnabled();

    private static final Logger TIMER_LOGGER = LogManager.getLogger(Loggers.TIMER);

    // Error messages
    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";
    private static final String DATA_MANAGER_ERROR = "Error starting DataManager";
    private static final String ERROR_INCORRECT_NUM_PARAMS = "Error: Incorrect number of parameters";

    // JVM Flag for WorkingDir removal
    private static final boolean REMOVE_WD;

    // JVM Flag for timers
    public static final boolean IS_TIMER_COMPSS_ENABLED;

    // Processes to capture out/err of each job
    private static final ThreadedPrintStream OUT;
    private static final ThreadedPrintStream ERR;
    public static final String SUFFIX_OUT = ".out";
    public static final String SUFFIX_ERR = ".err";

    // Application dependent attributes
    private final String deploymentId;
    private final boolean transferLogs;

    private final String hostName;
    private final String workingDir;
    private final String installDir;
    private final String appDir;

    private final TaskExecution executionType;
    private final boolean persistentC;

    private final LanguageParams[] langParams;

    private final boolean ear;
    private final boolean dataProvenance;

    // Transfer times
    private final Map<Integer, Long> transferStartTimes;

    // Internal components
    private final ExecutionManager executionManager;
    private final DataManager dataManager;

    static {
        // Set REMOVE_WD flag
        String removeWDFlag = System.getProperty(COMPSsConstants.WORKER_REMOVE_WD);
        boolean removeWDFlagDefined = removeWDFlag != null && !removeWDFlag.isEmpty();
        REMOVE_WD = removeWDFlagDefined ? Boolean.valueOf(removeWDFlag) : true;

        // Load timer property
        String isTimerCOMPSsEnabledProperty = System.getProperty(COMPSsConstants.TIMER_COMPSS_NAME);
        IS_TIMER_COMPSS_ENABLED = (isTimerCOMPSsEnabledProperty == null || isTimerCOMPSsEnabledProperty.isEmpty()
            || isTimerCOMPSsEnabledProperty.equals("null")) ? false : Boolean.valueOf(isTimerCOMPSsEnabledProperty);

        // Set processes to capturer out/error
        OUT = new ThreadedPrintStream(SUFFIX_OUT, System.out);
        ERR = new ThreadedPrintStream(SUFFIX_ERR, System.err);
        System.setErr(ERR);
        System.setOut(OUT);
    }


    /**
     * Creates a new NIOWorker instance.
     *
     * @param transferLogs Whether to transfer the output/error files or not.
     * @param snd Number of senders.
     * @param rcv Number of receivers.
     * @param hostName Worker hostname.
     * @param masterName Master hostname.
     * @param masterPort Master port.
     * @param streamingPort Streaming port.
     * @param computingUnitsCPU Worker CPU computing units.
     * @param computingUnitsGPU Worker GPU computing units.
     * @param computingUnitsFPGA Worker FPGA computing units
     * @param cpuMap String describing the thread-cpu mapping.
     * @param gpuMap String describing the thread-gpu mapping.
     * @param fpgaMap String describing the thread-fpga mapping.
     * @param limitOfTasks Limit of simultaneous tasks.
     * @param ioExecNum Number of IO Executors.
     * @param appUuid Application UUID.
     * @param traceFlag Tracing flag.
     * @param traceHost Tracing host name.
     * @param storageConf Storage configuration file path.
     * @param executionType Task execution type.
     * @param persistentC Whether to spawn persistent C workers or not.
     * @param workingDir Worker working directory.
     * @param installDir Installation directory.
     * @param appDir Application directory.
     * @param javaParams Java specific parameters.
     * @param pyParams Python specific parameters.
     * @param cParams C specific parameters.
     * @param lang Language.
     * @param ear Ear energy and power measurement.
     * @param dataProvenance Check if provenance is enabled.
     */
    public NIOWorker(boolean transferLogs, int snd, int rcv, String hostName, String masterName, int masterPort,
        int streamingPort, int computingUnitsCPU, int computingUnitsGPU, int computingUnitsFPGA, String cpuMap,
        String gpuMap, String fpgaMap, int limitOfTasks, int ioExecNum, String appUuid, String traceFlag,
        String traceHost, String tracingTaskDependencies, String storageConf, TaskExecution executionType,
        boolean persistentC, String workingDir, String installDir, String appDir, JavaParams javaParams,
        PythonParams pyParams, CParams cParams, RParams rParams, String lang, boolean ear, boolean dataProvenance) {

        super(snd, rcv, masterPort);

        this.transferLogs = transferLogs;
        // Log worker creation
        WORKER_LOGGER.info("NIO Worker init");

        // Set tracing attributes and initialize module if needed
        this.tracing = Boolean.parseBoolean(traceFlag);
        try {
            this.tracingId = Integer.parseInt(traceHost);
        } catch (Exception e) {
            WORKER_LOGGER.error("No valid hostID provided to the tracing system. Provided ID: " + hostName);
        }
        this.tracingTaskDependencies = Boolean.parseBoolean(tracingTaskDependencies);
        NIOTracer.init(this.tracing, this.tracingId, hostName, installDir, this.tracingTaskDependencies);
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(TraceEvent.START);
        }

        // Set attributes
        this.deploymentId = appUuid;
        this.hostName = hostName;
        this.workingDir = (workingDir.endsWith(File.separator) ? workingDir : workingDir + File.separator);
        this.installDir = (installDir.endsWith(File.separator) ? installDir : installDir + File.separator);
        this.appDir = appDir.equals("null") ? "" : appDir;

        this.executionType = executionType;
        System.setProperty(COMPSsConstants.STORAGE_CONF, storageConf);
        this.persistentC = persistentC;

        this.langParams = new LanguageParams[Lang.values().length];
        this.langParams[Lang.JAVA.ordinal()] = javaParams;
        this.langParams[Lang.PYTHON.ordinal()] = pyParams;
        this.langParams[Lang.C.ordinal()] = cParams;
        this.langParams[Lang.R.ordinal()] = rParams;

        this.ear = ear;
        this.dataProvenance = dataProvenance;

        this.transferStartTimes = new HashMap<>();

        // Set master node to null (will be set afterwards to the right value)
        this.masterNode = null;
        // If master name is defined set masterNode with the defined value
        if (masterName != null && !masterName.isEmpty() && !masterName.equals("null")) {
            this.masterNode = new NIONode(masterName, masterPort);
        }

        // Start DataManagerImpl
        this.dataManager = new DataManagerImpl(this.hostName, masterName, streamingPort, workingDir, this);

        try {
            this.dataManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(DATA_MANAGER_ERROR, ie);
        }

        this.executionManager = new ExecutionManager(this, computingUnitsCPU, cpuMap, false, computingUnitsGPU, gpuMap,
            computingUnitsFPGA, fpgaMap, ioExecNum, limitOfTasks);

        try {
            this.executionManager.init();
            // Starting the Python mirror here is avoids the overhead of mirror creation
            // right before the task execution
            if (lang.toUpperCase().equals(String.valueOf(COMPSsConstants.Lang.PYTHON))) {
                this.executionManager.startMirror();
            }
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }

    }

    private void removeWorkingDir(String workingDir) throws IOException {
        File dir = new File(workingDir);
        remove(dir);
        // Check if parent WD is empty
        Path parent = dir.getParentFile().toPath();
        if (isDirectoryEmpty(parent)) {
            Files.delete(parent);
        }

    }

    private boolean isDirectoryEmpty(Path path) throws IOException {
        if (Files.isDirectory(path)) {
            try (Stream<Path> entries = Files.list(path)) {
                return !entries.findFirst().isPresent();
            }
        }
        return false;
    }

    @Override
    public void setWorkerIsReady(String nodeName) {
        // Implemented on NIOAdaptor to notify that the worker is up and ready
    }

    @Override
    public void setMaster(NIONode master) {
        if (this.masterNode == null) {
            this.masterNode = new NIONode(master.getIp(), this.masterPort);
        }
    }

    @Override
    public boolean isMyUuid(String uuid, String nodeName) {
        return uuid.equals(this.deploymentId) && nodeName.equals(this.hostName);
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask task, List<String> obsoleteFiles) {
        Thread.currentThread().setPriority(Thread.MAX_PRIORITY);
        task.profileArrival();
        WORKER_LOGGER.info("Received Job " + task);
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("ARGUMENTS:");
            for (InvocationParam param : task.getParams()) {
                WORKER_LOGGER.info("    -" + param.getPrefix() + " " + param.getType() + ":" + param.getValue());
            }
            WORKER_LOGGER.debug("TARGET:");
            if (task.getTarget() != null) {
                WORKER_LOGGER.info("    -" + task.getTarget().getPrefix() + " " + task.getTarget().getType() + ":"
                    + task.getTarget().getValue());
            }
            WORKER_LOGGER.debug("RESULTS:");
            for (InvocationParam param : task.getResults()) {
                WORKER_LOGGER.info("    -" + param.getPrefix() + " " + param.getType() + ":" + param.getValue());
            }
        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(TraceEvent.WORKER_RECEIVED_NEW_TASK);
        }

        // Remove obsoletes
        long obsoletesTimeStart = 0L;
        long obsoletesTimeEnd = 0L;
        if (IS_TIMER_COMPSS_ENABLED) {
            obsoletesTimeStart = System.nanoTime();
        }
        if (obsoleteFiles != null && !obsoleteFiles.isEmpty()) {
            removeObsolete(obsoleteFiles);
        }
        if (IS_TIMER_COMPSS_ENABLED) {
            obsoletesTimeEnd = System.nanoTime();
            final float obsoletesTimeElapsed = (obsoletesTimeEnd - obsoletesTimeStart) / (float) 1_000_000;
            TIMER_LOGGER
                .info("[TIMER] Erasing obsoletes for task " + task.getJobId() + ": " + obsoletesTimeElapsed + " ms");
        }

        // Demand files
        WORKER_LOGGER.info("Checking parameters");
        TaskFetchOperationsListener listener = new TaskFetchOperationsListener(task, this);
        int paramIdx = 0;
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(TraceEvent.FETCH_PARAM);
        }
        for (NIOParam param : task.getParams()) {
            WORKER_LOGGER.info("Checking parameter " + param);
            paramIdx++;
            if (param.getData() != null) {
                // Parameter has associated data
                if (WORKER_LOGGER_DEBUG) {
                    WORKER_LOGGER
                        .debug("- Checking transfers for data " + param.getDataMgmtId() + " for parameter " + paramIdx);
                }
                listener.addOperation();
                dataManager.fetchParam(param, paramIdx, listener);
            }
        }
        WORKER_LOGGER.info("Checking target");
        NIOParam targetParam = task.getTarget();
        if (targetParam != null) {
            // Parameter has associated data
            WORKER_LOGGER
                .debug("- Checking transfers for data " + targetParam.getDataMgmtId() + " for target parameter");
            listener.addOperation();
            dataManager.fetchParam(targetParam, -1, listener);

        }
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEventEnd(TraceEvent.FETCH_PARAM);
            // Request the transfers
            NIOTracer.emitEvent(TraceEventType.TASK_TRANSFERS, listener.getTask().getTaskId());
        }
        requestTransfers();
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEventEnd(TraceEventType.TASK_TRANSFERS);
        }

        if (IS_TIMER_COMPSS_ENABLED) {
            final long paramsTimeEnd = System.nanoTime();
            final float paramsTimeElapsed = (paramsTimeEnd - obsoletesTimeEnd) / (float) 1_000_000;
            TIMER_LOGGER
                .info("[TIMER] Process parameters for task " + task.getJobId() + ": " + paramsTimeElapsed + " ms");

            // Add start transfer time
            this.transferStartTimes.put(task.getJobId(), paramsTimeEnd);
        }
        WORKER_LOGGER.debug("Enabling listener for task fetching");
        listener.enable();

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEventEnd(TraceEvent.WORKER_RECEIVED_NEW_TASK);
        }
    }

    @Override
    public void receivedNewDataFetchOrder(NIOParam data, int transferId) {
        FetchDataOperationListener listener = new FetchDataOperationListener(transferId, this);

        if (data != null) {
            // Parameter has associated data
            WORKER_LOGGER.debug("- Checking transfers for data " + data.getDataMgmtId());
            listener.addOperation();
            this.dataManager.fetchParam(data, -1, listener);
        }

        // Request the transfers
        /*
         * if (NIOTracer.extraeEnabled()) { NIOTracer.emitEvent(listener.getTask().getTaskId(),
         * NIOTracer.getTaskTransfersType()); }
         */
        requestTransfers();
        /*
         * if (NIOTracer.extraeEnabled()) { NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.getTaskTransfersType()); }
         */
    }

    @Override
    public void askForTransfer(InvocationParam param, int index, FetchDataListener listener) {
        NIOData data = ((NIOParam) param).getData();
        String target = (String) param.getValue();
        DataRequest dr = new WorkerDataRequest(listener, param.getType(), data, target);
        addTransferRequest(dr);
    }

    @Override
    public boolean isTransferingData(InvocationParam param) {
        NIOParam nioParam = ((NIOParam) param);
        List<DataRequest> requests = getDataRequests(nioParam.getData().getDataMgmtId());
        return requests != null && !requests.isEmpty();
    }

    @Override
    protected void handleDataToSendNotAvailable(Connection c, NIOData d) {
        // Now only manage at C (python could do the same when cache available)
        WORKER_LOGGER.debug("Handling data not available");
        /*
         * if (Lang.valueOf(lang.toUpperCase()) == Lang.C) { String path = d.getFirstURI().getPath();
         * WORKER_LOGGER.debug("about to serialize"); if (executionManager.serializeExternalData(d.getDataMgmtId(),
         * path)) { c.sendDataFile(path); return; } }
         */
        // If error or not external
        ErrorManager.warn(
            "Data " + d.getDataMgmtId() + "in this worker " + this.getHostName() + " could not be sent to master.");
        c.finishConnection();
    }

    // This is called when the master couldn't send a data to the worker.
    // The master abruptly finishes the connection. The NIOMessageHandler
    // handles this as an error, which treats with its function handleError,
    // and notifies the worker in this case.
    @Override
    public void handleRequestedDataNotAvailableError(List<DataRequest> failedRequests, String dataId) {
        for (DataRequest dr : failedRequests) {
            // For every task pending on this request, flag it as an error
            WorkerDataRequest wdr = (WorkerDataRequest) dr;
            // Mark as an error task. When all the params've been consumed, sendTaskDone unsuccessful
            wdr.getListener().errorFetchingValue(dataId, new DataNotAvailableException(dataId));

        }
    }

    @Override
    public void receivedValue(Destination type, String dataId, Object object, List<DataRequest> achievedRequests) {
        if (type == Transfer.Destination.OBJECT) {
            WORKER_LOGGER.info("Received data " + dataId + " with associated object " + object);
            this.dataManager.storeValue(dataId, object);
        } else {
            String nameId = (new File(dataId)).getName();
            WORKER_LOGGER.info("Received data " + nameId + " with path " + dataId);
            this.dataManager.storeFile(nameId, dataId);
        }
        for (DataRequest dr : achievedRequests) {
            WorkerDataRequest wdr = (WorkerDataRequest) dr;
            wdr.getListener().fetchedValue(dataId);
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug(
                    "Pending parameters: " + ((MultiOperationFetchListener) wdr.getListener()).getMissingOperations());
            }
        }
        if (NIOTracer.isActivated()) {
            String nameId = (new File(dataId)).getName();
            NIOTracer.emitDataTransferEvent(nameId, true);
        }
    }

    /**
     * Starts a new connection.
     *
     * @return The new connection.
     */
    public Connection startConnection() {
        return TM.startConnection(this.masterNode);
    }

    /**
     * Sends a task done notification for the given invocation and with the given result.
     *
     * @param invocation Associated Invocation.
     * @param successful Whether the task has ended successfully or not.
     */
    public void sendTaskDone(Invocation invocation, boolean successful, Exception e) {
        NIOTask nt = (NIOTask) invocation;
        int jobId = nt.getJobId();
        int taskId = nt.getTaskId();
        nt.profileEndNotification();
        NIOTaskResult tr = new NIOTaskResult(jobId);

        for (NIOParam param : nt.getParams()) {
            tr.addParamResult(getParameterResult(param));
        }

        NIOParam targetParam = nt.getTarget();
        if (targetParam != null) {
            tr.addParamResult(getParameterResult(targetParam));
        }

        for (NIOParam param : nt.getResults()) {
            tr.addParamResult(getParameterResult(param));
        }

        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("RESULT FOR JOB " + jobId + " (TASK ID: " + taskId + ")");
            WORKER_LOGGER.debug(tr);
        }

        CommandNIOTaskDone cmd = null;
        if (e instanceof COMPSsException) {
            cmd = new CommandNIOTaskDone(tr, successful, nt.getProfile(), invocation.getHistory().toString(),
                (COMPSsException) e);
        } else {
            cmd = new CommandNIOTaskDone(tr, successful, nt.getProfile(), invocation.getHistory().toString(), null);
        }

        // Notify task done
        sendNIOTaskDoneCommandSequence(cmd);

        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("Job " + jobId + "(Task " + taskId + ") send job done");
        }

    }

    private static NIOResult getParameterResult(NIOParam param) {
        DataType type = param.getType();
        String path;
        switch (type) {
            case DIRECTORY_T:
                path = ProtocolType.DIR_URI.getSchema() + param.getValue();
                break;
            case FILE_T:
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                path = ProtocolType.FILE_URI.getSchema() + param.getValue();
                break;
            case OBJECT_T:
                path = ProtocolType.OBJECT_URI.getSchema() + param.getDataMgmtId();
                break;
            case STREAM_T:
                path = ProtocolType.STREAM_URI.getSchema() + param.getDataMgmtId();
                break;
            case EXTERNAL_STREAM_T:
                path = ProtocolType.EXTERNAL_STREAM_URI.getSchema() + param.getValue();
                break;
            case PSCO_T:
                // Search for the PSCO id
                String id = ((StubItf) param.getValue()).getID();
                path = ProtocolType.PERSISTENT_URI.getSchema() + id;
                break;
            case EXTERNAL_PSCO_T:
                // The value of the registered object in the runtime is the PSCO Id
                path = ProtocolType.PERSISTENT_URI.getSchema() + param.getValue();
                break;
            case BINDING_OBJECT_T:
                path = ProtocolType.BINDING_URI.getSchema() + param.getValue();
                break;
            default:
                path = null;
        }

        if (param.isCollective()) {
            List<NIOResult> elements = new LinkedList<>();
            for (NIOParam p : ((NIOParamCollection) param).getCollectionParameters()) {
                elements.add(getParameterResult(p));
            }
            return new NIOResultCollection(path, elements);
        } else {
            return new NIOResult(path);
        }

    }

    private void sendNIOTaskDoneCommandSequence(CommandNIOTaskDone cmd) {
        Connection c = TM.startConnection(this.masterNode);
        registerOngoingCommand(c, cmd);
        c.sendCommand(cmd);

        if (this.transferLogs || !cmd.isSuccessful()) {
            String jobStdsFileName = this.getStandardStreamsPath(cmd.getTaskResult().getJobId(), cmd.getJobHistory());
            // Check that output files already exists. If not exists generate an empty one.
            String taskFileOutName = jobStdsFileName + ".out";
            checkStreamFileExistence(taskFileOutName, "out",
                "Autogenerated Empty file. An error was produced before generating any log in the stdout", null);
            String taskFileErrName = jobStdsFileName + ".err";
            checkStreamFileExistence(taskFileErrName, "err",
                "Autogenerated Empty file. An error was produced before generating any log in the stderr", null);
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("Sending file " + taskFileOutName + ", for connection: " + c.hashCode());
            }
            c.sendDataFile(taskFileOutName);
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("Sending file " + taskFileErrName + ", for connection: " + c.hashCode());
            }
            c.sendDataFile(taskFileErrName);
        }

        c.finishConnection();

    }

    /**
     * Checks whether a STD IO stream file (OUT / ERR) exists or not.
     *
     * @param taskFileName Task file name.
     * @param streamName Stream name.
     * @param errorMessage Error message.
     */
    public void checkStreamFileExistence(String taskFileName, String streamName, String errorMessage, Exception e) {
        File taskFile = new File(taskFileName);
        if (!taskFile.exists()) {
            try (FileOutputStream stream = new FileOutputStream(taskFile)) {
                stream.write(errorMessage.getBytes());
                if (e != null) {
                    try (PrintStream s = new PrintStream(stream)) {
                        e.printStackTrace(s);
                    }
                }
                stream.close();
            } catch (IOException ioe) {
                WORKER_LOGGER.error("IOException writing worker " + streamName + " file: " + taskFile, ioe);
            }
        }
    }

    /**
     * Executes the given task.
     *
     * @param task Task to execute.
     */
    public void executeTask(NIOTask task) {
        task.profileFetchedData();
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("Enqueueing job " + task.getJobId() + " for execution.");
        }

        // Execute the job
        TaskExecutionListener tel = new TaskExecutionListener(this);
        InvocationExecutionRequest e = new InvocationExecutionRequest(task, tel);
        this.executionManager.enqueue(e);

        // Notify the master that the data has been transfered
        // The message is sent after the task enqueue because the connection can
        // have N pending task transfer and will wait until they
        // are finished to send all the answers (blocking the task execution)
        if (WORKER_LOGGER_DEBUG) {
            WORKER_LOGGER.debug("Notifying presence of all data for job " + task.getJobId() + ".");
        }

        CommandDataReceived cdr = new CommandDataReceived(task.getTransferGroupId());
        Connection c = TM.startConnection(this.masterNode);
        registerOngoingCommand(c, cdr);
        c.sendCommand(cdr);
        c.finishConnection();
    }

    @Override
    public void cancelRunningTask(NIONode node, int jobId) {
        this.executionManager.cancelJob(jobId);
    }

    /**
     * Remove obsolete objects.
     *
     * @param obsolete List of obsolete objects.
     */
    public void removeObsolete(List<String> obsolete) {
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(TraceEvent.REMOVE_OBSOLETES);
        }
        this.dataManager.removeObsoletes(obsolete);
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEventEnd(TraceEvent.REMOVE_OBSOLETES);
        }
    }

    @Override
    public void shutdownExecutionManager(Connection closingConnection) {
        // Stop the ExecutorRequest Manager
        WORKER_LOGGER.debug("Stopping Execution Manager...");
        this.executionManager.stop();

        if (closingConnection != null) {
            closingConnection.sendCommand(new CommandExecutorShutdownACK());
            closingConnection.finishConnection();
        }
    }

    @Override
    public void shutdownExecutionManagerNotification(Connection c) {
        ErrorManager.warn("Shutdown execution ACK notification should never be received by a worker");
    }

    // Shutdown the worker, at this point there are no active transfers
    @Override
    public void shutdown(Connection closingConnection) {
        WORKER_LOGGER.debug("Entering shutdown method on worker");

        // Stop the NIOData Manager
        this.dataManager.stop();

        // Finish the main thread
        if (closingConnection != null) {
            closingConnection.sendCommand(new CommandShutdownACK());
            closingConnection.finishConnection();
        }

        TM.shutdown(true, closingConnection);
        if (REMOVE_WD) {
            try {
                removeWorkingDir(workingDir);
                WORKER_LOGGER.debug(" Working Dir removed:" + workingDir);
            } catch (IOException e) {
                WORKER_LOGGER.error("Removing Worker Dir failed:" + workingDir);
            }
        }
        WORKER_LOGGER.debug("Finish shutdown method on worker");
    }

    private void removeFolder(String sandBox) throws IOException {
        File wdirFile = new File(sandBox);
        remove(wdirFile);
    }

    private void remove(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                FileUtils.deleteDirectory(f);
            } else {
                Files.delete(f.toPath());
            }
        }
    }

    @Override
    public String getObjectAsFile(String s) {
        // This method should never be called in the worker side
        WORKER_LOGGER.warn("getObjectAsFile has been called in the worker side!");

        return null;
    }

    @Override
    public void receivedNIOTaskDone(Connection c, NIOTaskResult tr, NIOTaskProfile profile, boolean successful,
        Exception e) {
        // Should not receive this call
    }

    @Override
    public void copiedData(int transfergroupID) {
        // Should not receive this call
    }

    @Override
    public void shutdownNotification(Connection c) {
        // Never orders the shutdown of a worker peer
    }

    @Override
    public void notifyAnalysisFilesDone(Set<String> tracingFilesPaths) {
        // Nothing to do
    }

    @Override
    public void notifyDebugFilesDone(Set<String> logPath) {
        // Nothing to do
    }

    /**
     * Freezes the files on the folder and returns the paths to those frozen files.
     *
     * @param foldername folder path to the files
     * @return Set the paths of the files in that folder
     */
    private Set<String> getFilesPathFromFolder(String folderPath) {

        Set<String> pathSet = Stream.of(new File(folderPath).listFiles()).filter(file -> !file.isDirectory())
            .map(File::getName).collect(Collectors.toSet());

        Set<String> frozenPaths = new HashSet<String>();

        for (String path : pathSet) {
            String completePath = folderPath + File.separator + path;
            String frozenPath = folderPath + File.separator + "static_" + path;
            freezeFile(completePath, frozenPath);
            frozenPaths.add(frozenPath);
        }
        return frozenPaths;
    }

    private void freezeFolderFiles(String folderPath) throws Exception {
        // cd to the folder and cp every file onto static_${file} and removes the original file
        String cmd = "cd " + folderPath + " && for file in *; do cp ${file} static_${file} && rm -rf ${file}; done";
        WORKER_LOGGER.debug("Executing: " + cmd.toString());
        int exitCode = new ProcessBuilder("/bin/bash", "-c", cmd.toString()).inheritIO().start().waitFor();
        if (exitCode != 0) {
            throw new Exception("freezeFolderFailed");
        }
    }

    private void freezeFile(String sourcePath, String targetPath) {
        File source = new File(sourcePath);
        if (source.exists() && source.length() > 0) {
            try {
                Files.copy(source.toPath(), new File(targetPath).toPath());
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception copying source to target file", e);
            }
        } else {
            // TODO: Sending a file with "Empty file" is a patch because the comm library fails when transferring an
            // empty file
            try (FileOutputStream fos = new FileOutputStream(targetPath)) {
                fos.write("Empty file".getBytes());
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception writing empty file", e);
            }
        }
    }

    private Set<String> generatePackageFromFolder(String sourceFolder, String tarTargetPath) throws Exception {
        // we create the tar.gz in the workingDir because otherwise it creates an empty file with the name of the tar
        // inside the tar
        freezeFolderFiles(sourceFolder);

        StringBuilder sb = new StringBuilder();
        // need to cd to the analysis director in order for the tar to contain relative paths
        sb.append("cd " + sourceFolder + " && ");
        // we only want to make the tar if we can get into the apropiate folder
        sb.append("tar -czf " + tarTargetPath + " " + ".");

        WORKER_LOGGER.debug("Executing: " + sb.toString());
        int exitCode = new ProcessBuilder("/bin/bash", "-c", sb.toString()).inheritIO().start().waitFor();
        if (exitCode != 0) {
            throw new Exception("package from folder creation returned not 0 exit code");
        }

        Set<String> res = new HashSet<String>();
        res.add(tarTargetPath);
        return res;
    }

    @Override
    public void generateDebugFiles(Connection c) {

        Set<String> logFilesPaths;
        // Ideally the transfer should bea single compressed file
        // following comented code was a try at that but was not working properly

        // try {
        // // Tries to generate a tar.gz file with the contents of the analysis folder.
        // final String tarTargetPath = this.getWorkingDir() + File.separator + "debug.tar.gz";
        // logFilesPaths = generatePackageFromFolder(this.getLogDir(), tarTargetPath);
        // } catch (Exception e) {
        // // If it runs out of space to do the tar.gz package sends the paths to the files
        // WORKER_LOGGER.warn("Something failed while generatin tar.gz package with the contents of the debug folder.",
        // e);
        // logFilesPaths = getFilesPathFromFolder(this.getLogDir());
        // }

        logFilesPaths = getFilesPathFromFolder(this.getLogDir());

        c.sendCommand(new CommandGenerateDebugFilesDone(logFilesPaths));
        c.finishConnection();
    }

    @Override
    public void generateAnalysisFiles(Connection c) {

        generateTracingFiles();
        Set<String> analysisFilesPaths;

        // Ideally the transfer should be a single compressed file
        // following commented code was a try at that but was not working properly

        // try {
        // // Tries to generate a tar.gz file with the contents of the analysis folder.
        // final String tarTargetPath = this.getWorkingDir() + File.separator + "analysis.tar.gz";
        // analysisFilesPaths = generatePackageFromFolder(this.getAnalysisDir(), tarTargetPath);
        // } catch (Exception e) {
        // // If it runs out of space to do the tar.gz package sends the paths to the files
        // WORKER_LOGGER
        // .warn("Something failed while generating tar.gz package with the contents of the analysis folder.", e);
        // analysisFilesPaths = getFilesPathFromFolder(this.getAnalysisDir());
        // }

        analysisFilesPaths = getFilesPathFromFolder(this.getAnalysisDir());

        c.sendCommand(new CommandGenerateAnalysisFilesDone(analysisFilesPaths));
        c.finishConnection();
    }

    @Override
    public void receivedBindingObjectAsFile(String filename, String target) {
        // Nothing to do at worker
    }

    @Override
    protected String getPossiblyRenamedFileName(File originalFile, NIOData d) {
        return originalFile.getParentFile().getAbsolutePath() + File.separator + d.getDataMgmtId();
    }

    // ********************************************************************
    // *************** INVOCATION CONTEXT IMPLEMENTATIONS *****************
    // ********************************************************************
    // WORKER CONFIGURATION
    @Override
    public String getHostName() {
        return this.hostName;
    }

    @Override
    public long getTracingHostID() {
        return Long.parseLong(NIOTracer.getHostID());
    }

    @Override
    public String getAppDir() {
        return this.appDir;
    }

    @Override
    public String getInstallDir() {
        return this.installDir;
    }

    @Override
    public String getWorkingDir() {
        return this.workingDir;
    }

    @Override
    public String getLogDir() {
        // This decides where the binding files are stored
        // Setup.sh logDir decides where the worker.out files are stored
        // TODO: unify this with setup.sh file logDir variable to have it defined only in one place
        return this.workingDir + "log/";
    }

    @Override
    public String getAnalysisDir() {
        // This decides where the files generated by the worker that are not log are stored
        return this.workingDir + "analysis/";
    }

    @Override
    public boolean isPersistentCEnabled() {
        return this.persistentC;
    }

    public long getTransferStartTime(Integer jobId) {
        return this.transferStartTimes.get(jobId);
    }

    // EXECUTION CONFIGURATION
    @Override
    public COMPSsConstants.TaskExecution getExecutionType() {
        return this.executionType;
    }

    @Override
    public LanguageParams getLanguageParams(Lang lang) {
        WORKER_LOGGER
            .info("GETTING LANGUAGE PARAMS :" + Lang.PYTHON.ordinal() + " -> " + this.langParams[lang.ordinal()]);
        return this.langParams[lang.ordinal()];
    }

    // EXECUTION MANAGEMENT
    @Override
    public void registerOutputs(String path) {
        ERR.registerThread(path);
        OUT.registerThread(path);
    }

    @Override
    public void unregisterOutputs() {
        ERR.unregisterThread();
        OUT.unregisterThread();
    }

    @Override
    public String getStandardStreamsPath(Invocation invocation) {
        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        return getStandardStreamsPath(invocation.getJobId(), invocation.getHistory().toString());
    }

    private String getStandardStreamsPath(int jobId, String jobHistory) {
        // Set outputs paths (Java will register them, ExternalExec will redirect processes outputs)
        return this.getWorkingDir() + "jobs" + File.separator + "job" + jobId + "_" + jobHistory;
    }

    /**
     * Get the Std Err stream assigned to this computing thread.
     *
     * @return The Std Err stream assigned to this computing thread.
     */
    @Override
    public PrintStream getThreadErrStream() {
        return ERR.getStream();
    }

    /**
     * Get the Std Out stream assigned to this computing thread.
     *
     * @return The Std Out stream assigned to this computing thread.
     */
    @Override
    public PrintStream getThreadOutStream() {
        return OUT.getStream();
    }

    // DATA MANAGEMENT
    @Override
    public String getStorageConf() {
        return this.dataManager.getStorageConf();
    }

    @Override
    public StreamBackend getStreamingBackend() {
        return this.dataManager.getStreamingBackend();
    }

    @Override
    public String getStreamingMasterName() {
        return this.dataManager.getStreamingMasterName();
    }

    @Override
    public int getStreamingMasterPort() {
        return this.dataManager.getStreamingMasterPort();
    }

    @Override
    public void loadParam(InvocationParam param) throws UnloadableValueException {
        this.dataManager.loadParam(param);
    }

    @Override
    public void storeParam(InvocationParam param, boolean createifNonExistent)
        throws UnwritableValueException, NonExistentDataException {
        this.dataManager.storeParam(param);
        if (param.getType() == DataType.FILE_T) {
            String filepath = (String) param.getValue();
            // if (Tracer.isActivated()) {
            // Tracer.emitEvent(TraceEvent.CHECK_OUT_PARAM);
            // }
            File f = new File(filepath);
            boolean fExists = f.exists();
            // if (Tracer.isActivated()) {
            // Tracer.emitEventEnd(TraceEvent.CHECK_OUT_PARAM);
            // }
            if (!fExists) {
                if (createifNonExistent) {
                    System.out.println("Creating new blank file at " + filepath);
                    try {
                        f.createNewFile(); // NOSONAR ignoring result. It couldn't exists.
                    } catch (IOException e) {
                        WORKER_LOGGER.warn("ERROR creating new blank file at " + filepath);
                        throw new UnwritableValueException(e);
                    }
                }
                throw new NonExistentDataException(filepath);
            }
        }
    }

    @Override
    public Object getObject(String name) {
        String realName = name.substring(name.lastIndexOf('/') + 1);
        return this.dataManager.getObject(realName);
    }

    @Override
    public void increaseResources(MethodResourceDescription description) {
        int cpuCount = description.getTotalCPUComputingUnits();
        int gpuCount = description.getTotalGPUComputingUnits();
        int fpgaCount = description.getTotalFPGAComputingUnits();
        int otherCount = description.getTotalOTHERComputingUnits();
        this.executionManager.increaseCapabilities(cpuCount, gpuCount, fpgaCount, otherCount);
    }

    @Override
    public void reduceResources(MethodResourceDescription description) {
        int cpuCount = description.getTotalCPUComputingUnits();
        int gpuCount = description.getTotalGPUComputingUnits();
        int fpgaCount = description.getTotalFPGAComputingUnits();
        int otherCount = description.getTotalOTHERComputingUnits();
        this.executionManager.reduceCapabilities(cpuCount, gpuCount, fpgaCount, otherCount);
    }

    @Override
    public void performedResourceUpdate(Connection c) {
        // Should never request a resourceModification
    }

    // **************************************
    // *************** MAIN *****************
    // **************************************
    /**
     * Entry Point.
     *
     * @param args Command line arguments.
     */
    public static void main(String[] args) {
        // Check arguments length
        if (args.length != (NUM_PARAMS_NIO_WORKER)) {
            WORKER_LOGGER.debug("Received parameters: ");
            for (int i = 0; i < args.length; ++i) {
                WORKER_LOGGER.debug("Param " + i + ":  " + args[i]);
            }
            ErrorManager.fatal(ERROR_INCORRECT_NUM_PARAMS);
        }

        // Parse arguments
        boolean debug = Boolean.valueOf(args[0]);

        int maxSnd = Integer.parseInt(args[1]);
        int maxRcv = Integer.parseInt(args[2]);
        String workerIP = args[3];
        int wPort = Integer.parseInt(args[4]);
        String mName = args[5];
        int mPort = Integer.parseInt(args[6]);
        int streamingPort = Integer.parseInt(args[7]);

        int computingUnitsCPU = Integer.parseInt(args[8]);
        int computingUnitsGPU = Integer.parseInt(args[9]);
        int computingUnitsFPGA = Integer.parseInt(args[10]);
        String cpuMap = args[11];
        String gpuMap = args[12];
        String fpgaMap = args[13];
        int ioExecNum = Integer.parseInt(args[14]);
        int limitOfTasks = Integer.parseInt(args[15]);

        String appUuid = args[16];
        String lang = args[17];
        String workingDir = args[18];
        String installDir = args[19];
        final String appDir = args[20];
        String libPath = args[21];
        String classpath = args[22];
        String pythonpath = args[23];

        String traceFlag = args[24];
        String extraeFile = args[25];
        String traceHost = args[26];
        String traceTaskDependencies = args[27];

        String storageConf = args[28];
        TaskExecution executionType = TaskExecution.valueOf(args[29].toUpperCase());

        boolean persistentC = Boolean.parseBoolean(args[30]);

        String pythonInterpreter = args[31];
        String pythonVersion = args[32];
        String pythonVirtualEnvironment = args[33];
        String pythonPropagateVirtualEnvironment = args[34];
        String pythonExtraeFile = args[35];
        String pythonMpiWorker = args[36];
        String pythonWorkerCache = args[37];
        String pythonCacheProfiler = args[38];

        boolean ear = Boolean.parseBoolean(args[39]);
        boolean dataProvenance = Boolean.parseBoolean(args[40]);

        final JavaParams javaParams = new JavaParams(classpath);
        final PythonParams pyParams = new PythonParams(pythonInterpreter, pythonVersion, pythonVirtualEnvironment,
            pythonPropagateVirtualEnvironment, pythonpath, pythonExtraeFile, pythonMpiWorker, pythonWorkerCache,
            pythonCacheProfiler);
        final CParams cParams = new CParams(classpath);
        final RParams rParams = new RParams(classpath);

        // Print arguments
        if (WORKER_LOGGER.isDebugEnabled()) {
            WORKER_LOGGER.debug("maxSnd: " + String.valueOf(maxSnd));
            WORKER_LOGGER.debug("maxRcv: " + String.valueOf(maxRcv));

            WORKER_LOGGER.debug("WorkerName: " + workerIP);
            WORKER_LOGGER.debug("WorkerPort: " + String.valueOf(wPort));
            WORKER_LOGGER.debug("MasterName: " + mName);
            WORKER_LOGGER.debug("MasterPort: " + String.valueOf(mPort));
            WORKER_LOGGER.debug("StreamingPort: " + String.valueOf(streamingPort));

            WORKER_LOGGER.debug("Computing Units CPU: " + String.valueOf(computingUnitsCPU));
            WORKER_LOGGER.debug("Computing Units GPU: " + String.valueOf(computingUnitsGPU));
            WORKER_LOGGER.debug("Computing Units FPGA: " + String.valueOf(computingUnitsFPGA));
            WORKER_LOGGER.debug("User defined CPU Map: " + cpuMap);
            WORKER_LOGGER.debug("User defined GPU Map: " + gpuMap);
            WORKER_LOGGER.debug("User defined FPGA Map: " + fpgaMap);
            WORKER_LOGGER.debug("Limit Of Tasks: " + String.valueOf(limitOfTasks));

            WORKER_LOGGER.debug("IO Executors: " + String.valueOf(ioExecNum));

            WORKER_LOGGER.debug("App uuid: " + appUuid);
            WORKER_LOGGER.debug("Language: " + lang);
            WORKER_LOGGER.debug("WorkingDir:" + workingDir);
            WORKER_LOGGER.debug("Install Dir: " + installDir);

            WORKER_LOGGER.debug("Tracing: " + traceFlag);
            WORKER_LOGGER.debug("Extrae config File: " + extraeFile);
            WORKER_LOGGER.debug("Python extrae config File: " + pythonExtraeFile);
            WORKER_LOGGER.debug("Host: " + traceHost);
            WORKER_LOGGER.debug("Tracing Task Dependencies: " + traceTaskDependencies);

            WORKER_LOGGER.debug("LibraryPath: " + libPath);
            WORKER_LOGGER.debug("Classpath: " + classpath);
            WORKER_LOGGER.debug("Pythonpath: " + pythonpath);

            WORKER_LOGGER.debug("StorageConf: " + storageConf);
            WORKER_LOGGER.debug("executionType: " + executionType);

            WORKER_LOGGER.debug("Persistent c: " + persistentC);

            WORKER_LOGGER.debug("Python interpreter: " + pythonInterpreter);
            WORKER_LOGGER.debug("Python version: " + pythonVersion);
            WORKER_LOGGER.debug("Python virtual environment: " + pythonVirtualEnvironment);
            WORKER_LOGGER.debug("Python propagate virtual environment: " + pythonPropagateVirtualEnvironment);
            WORKER_LOGGER.debug("Python use MPI worker: " + pythonMpiWorker);
            WORKER_LOGGER.debug("Python use worker cache: " + pythonWorkerCache);
            WORKER_LOGGER.debug("Python use cache profiler: " + pythonCacheProfiler);

            WORKER_LOGGER.debug("Ear: " + ear);
            WORKER_LOGGER.debug("Provenance: " + dataProvenance);

            WORKER_LOGGER.debug("Remove Sanbox WD: " + REMOVE_WD);
        }

        // Configure storage
        System.setProperty(COMPSsConstants.STORAGE_CONF, storageConf);

        // Configure tracing
        System.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, extraeFile);
        System.setProperty(COMPSsConstants.EXTRAE_WORKING_DIR, workingDir);

        System.setProperty(COMPSsConstants.TRACING_TASK_DEPENDENCIES, traceTaskDependencies);

        /*
         * ***********************************************************************************************************
         * LAUNCH THE WORKER
         *************************************************************************************************************/

        // todo: nm: pass the lang to the nio constructor
        NIOWorker nw = new NIOWorker(debug, maxSnd, maxRcv, workerIP, mName, mPort, streamingPort, computingUnitsCPU,
            computingUnitsGPU, computingUnitsFPGA, cpuMap, gpuMap, fpgaMap, limitOfTasks, ioExecNum, appUuid, traceFlag,
            traceHost, traceTaskDependencies, storageConf, executionType, persistentC, workingDir, installDir, appDir,
            javaParams, pyParams, cParams, rParams, lang, ear, dataProvenance);

        NIOMessageHandler mh = new NIOMessageHandler(nw);

        // Initialize the Transfer Manager
        WORKER_LOGGER.debug("  Initializing the TransferManager structures...");
        try {
            TM.init(NIO_EVENT_MANAGER_CLASS, null, mh);
        } catch (CommException ce) {
            WORKER_LOGGER.error("Error initializing Transfer Manager on worker " + nw.getHostName(), ce);
            // Shutdown the Worker since the error it is not recoverable
            nw.shutdown(null);
            return;
        }

        // Start the Transfer Manager thread (starts the EventManager)
        // WORKER_LOGGER.debug(" Starting TransferManager Thread");
        // TM.start();
        try {
            TM.startServer(new NIONode(null, wPort));
        } catch (CommException ce) {
            WORKER_LOGGER.error("Error starting TransferManager Server at Worker" + nw.getHostName(), ce);
            nw.shutdown(null);
            return;
        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEventEnd(TraceEvent.START);
        }

        /*
         * ***********************************************************************************************************
         * JOIN AND END
         *************************************************************************************************************/
        // Wait for the Transfer Manager thread to finish (the shutdown is received on that thread)
        try {
            TM.join();
        } catch (InterruptedException ie) {
            WORKER_LOGGER.warn("TransferManager interrupted", ie);
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void unhandeledError(Connection c) {
        WORKER_LOGGER.fatal("Unhandeled error in connection " + c.hashCode() + ". Raising Runtime Exception...");
        throw new RuntimeException("Unhandeled error in connection " + c.hashCode());

    }

    @Override
    public void handleCancellingTaskCommandError(Connection c, CommandCancelTask commandCancelTask) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error receiving tracing generate done. Not handeled");

    }

    @Override
    public void handleDataReceivedCommandError(Connection c, CommandDataReceived commandDataReceived) {
        if (commandDataReceived.canRetry()) {
            commandDataReceived.increaseRetries();
            resendCommand((NIONode) c.getNode(), commandDataReceived);
        } else {
            WORKER_LOGGER.warn("Error sending data received after retries. Nothing else to do.");
        }

    }

    @Override
    public void handleExecutorShutdownCommandError(Connection c, CommandExecutorShutdown commandExecutorShutdown) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error receiving executor command done. Not handeled");// TODO Auto-generated method stub

    }

    @Override
    public void handleExecutorShutdownCommandACKError(Connection c,
        CommandExecutorShutdownACK commandExecutorShutdownACK) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error sending executor shutdown ACK. Not handeled");

    }

    @Override
    public void handleTaskDoneCommandError(Connection c, CommandNIOTaskDone commandNIOTaskDone) {
        if (commandNIOTaskDone.canRetry()) {
            commandNIOTaskDone.increaseRetries();
            sendNIOTaskDoneCommandSequence(commandNIOTaskDone);
        } else {
            WORKER_LOGGER.warn("Error sending task done after retries. Nothing else to do.");
        }

    }

    @Override
    public void handleNewTaskCommandError(Connection c, CommandNewTask commandNewTask) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error receiving new task command. Not handeled");

    }

    @Override
    public void handleShutdownCommandError(Connection c, CommandShutdown commandShutdown) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error receiving new task command. Not handeled");

    }

    @Override
    public void handleShutdownACKCommandError(Connection c, CommandShutdownACK commandShutdownACK) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error sending eshutdown ACK. Not handeled");

    }

    @Override
    public void handleTracingGenerateDoneCommandError(Connection c,
        CommandGenerateAnalysisFilesDone commandGenerateAnalysisFilesDone) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error sending tracing generate done. Not handeled");

    }

    @Override
    public void handleTracingGenerateCommandError(Connection c,
        CommandGenerateAnalysisFiles commandGenerateAnalysisFiles) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error receiving tracing generate command. Not handeled");

    }

    @Override
    public void handleGenerateWorkerDebugCommandError(Connection c,
        CommandGenerateDebugFiles commandGenerateDebugFiles) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error receiving generate worker debug command. Not handeled");

    }

    @Override
    public void handleGenerateWorkerDebugDoneCommandError(Connection c,
        CommandGenerateDebugFilesDone commandGenerateDebugFilesDone) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error sending  generate worker debug done. Not handeled");

    }

    @Override
    public void receivedRemoveObsoletes(NIONode node, List<String> obsoletes) {
        // Remove obsoletes
        long obsoletesTimeStart = 0L;
        long obsoletesTimeEnd = 0L;
        if (IS_TIMER_COMPSS_ENABLED) {
            obsoletesTimeStart = System.nanoTime();
        }
        if (obsoletes != null) {
            removeObsolete(obsoletes);
        }
        if (IS_TIMER_COMPSS_ENABLED) {
            obsoletesTimeEnd = System.nanoTime();
            final float obsoletesTimeElapsed = (obsoletesTimeEnd - obsoletesTimeStart) / (float) 1_000_000;
            TIMER_LOGGER.info("[TIMER] Erasing obsoletes for command : " + obsoletesTimeElapsed + " ms");
        }

    }

    @Override
    public void handleRemoveObsoletesCommandError(Connection c, CommandRemoveObsoletes commandRemoveObsoletes) {
        // Nothing to do at worker
        WORKER_LOGGER.warn("Error receiving remove obsoletes command. Not handeled");

    }

    @Override
    public void workerPongReceived(String nodeName) {
        // not necessary, handled only on the master
    }

    @Override
    public void handleNodeIsDownError(String nodeName) {
        // not necessary, handled only on the master
    }

    @Override
    public COMPSsRuntime getRuntimeAPI() {
        return null;
    }

    @Override
    public LoaderAPI getLoaderAPI() {
        return null;
    }

    @Override
    public void idleReservedResourcesDetected(ResourceDescription resources) {
        // NIO Adaptor does not support remote resource updates
    }

    @Override
    public void reactivatedReservedResourcesDetected(ResourceDescription resources) {
        // NIO Adaptor does not support remote resource updates
    }

    @Override
    public String getEnvironmentScript() {
        return null;
    }

    @Override
    public boolean getEar() {
        return this.ear;
    }

    @Override
    public boolean getDataProvenance() {
        return this.dataProvenance;
    }

    /**
     * / Generates the tracing files in the Analysis folder.
     */
    public void generateTracingFiles() {
        NIOTracer.fini(new HashMap<>());

        String packagePath = this.getAnalysisDir();
        if (!packagePath.endsWith(File.separator)) {
            packagePath += File.separator;
        }
        packagePath += this.hostName + NIOTracer.PACKAGE_SUFFIX;
        NIOTracer.generatePackage(packagePath);
    }

    public Set<String> generateProfilingFiles() {
        return null;
    }

}
