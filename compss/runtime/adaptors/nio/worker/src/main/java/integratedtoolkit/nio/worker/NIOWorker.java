package integratedtoolkit.nio.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;
import es.bsc.comm.Connection;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import integratedtoolkit.ITConstants;
import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTaskResult;
import integratedtoolkit.nio.NIOURI;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOMessageHandler;
import integratedtoolkit.nio.commands.CommandDataReceived;
import integratedtoolkit.nio.commands.CommandExecutorShutdownACK;
import integratedtoolkit.nio.commands.CommandShutdownACK;
import integratedtoolkit.nio.commands.CommandTaskDone;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.nio.commands.workerFiles.CommandWorkerDebugFilesDone;
import integratedtoolkit.nio.dataRequest.DataRequest;
import integratedtoolkit.nio.dataRequest.WorkerDataRequest;
import integratedtoolkit.nio.dataRequest.WorkerDataRequest.TransferringTask;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.worker.components.DataManager;
import integratedtoolkit.nio.worker.components.ExecutionManager;
import integratedtoolkit.nio.worker.exceptions.InvalidMapException;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.util.ThreadPrintStream;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.Tracer;


public class NIOWorker extends NIOAgent {

    // General configuration attributes
    private static final int MAX_RETRIES = 5;

    // Logger
    private static final Logger WORKER_LOGGER = LogManager.getLogger(Loggers.WORKER);
    private static final boolean WORKER_LOGGER_DEBUG = WORKER_LOGGER.isDebugEnabled();
    private static final String EXECUTION_MANAGER_ERR = "Error starting ExecutionManager";
    private static final String DATA_MANAGER_ERROR = "Error starting DataManager";
    private static final String ERROR_INCORRECT_NUM_PARAMS = "Error: Incorrect number of parameters";

    // JVM Flag for WorkingDir removal
    private static boolean removeWDFlagDefined = System.getProperty(ITConstants.IT_WORKER_REMOVE_WD) != null
            && !System.getProperty(ITConstants.IT_WORKER_REMOVE_WD).isEmpty();
    private static boolean removeWD = removeWDFlagDefined ? Boolean.valueOf(System.getProperty(ITConstants.IT_WORKER_REMOVE_WD)) : true;

    // Application dependent attributes
    private static boolean isWorkerDebugEnabled;

    private final String deploymentId;
    private final String lang;
    private final String host;

    private final String workingDir;
    private final String installDir;

    private final String appDir;
    private final String libraryPath;
    private final String classpath;
    private final String pythonpath;

    // Storage attributes
    private static String storageConf;
    private static String executionType;

    private static boolean persistentC;
    // Internal components
    private final ExecutionManager executionManager;
    private final DataManager dataManager;

    // Processes to capture out/err of each job
    private static ThreadPrintStream out;
    private static ThreadPrintStream err;
    public static final String SUFFIX_OUT = ".out";
    public static final String SUFFIX_ERR = ".err";

    static {
        try {
            out = new ThreadPrintStream(SUFFIX_OUT, System.out);
            err = new ThreadPrintStream(SUFFIX_ERR, System.err);
            System.setErr(err);
            System.setOut(out);
        } catch (Exception e) {
            WORKER_LOGGER.error("Exception", e);
        }
    }


    public NIOWorker(int snd, int rcv, String hostName, int masterPort, int computingUnitsCPU, int computingUnitsGPU, String cpuMap,
            String gpuMap, int limitOfTasks, String appUuid, String lang, String workingDir, String installDir, String appDir,
            String libPath, String classpath, String pythonpath) {

        super(snd, rcv, masterPort);

        // Log worker creation
        WORKER_LOGGER.info("NIO Worker init");

        // Set attributes
        this.deploymentId = appUuid;
        this.lang = lang;
        this.host = hostName;
        this.workingDir = (workingDir.endsWith(File.separator) ? workingDir : workingDir + File.separator);
        this.installDir = (installDir.endsWith(File.separator) ? installDir : installDir + File.separator);
        this.appDir = appDir.equals("null") ? "" : appDir;
        this.libraryPath = libPath.equals("null") ? "" : libPath;
        this.classpath = classpath.equals("null") ? "" : classpath;
        this.pythonpath = pythonpath.equals("null") ? "" : pythonpath;

        // Set master node to null (will be set afterwards to the right value)
        this.masterNode = null;

        // Start DataManager
        this.dataManager = new DataManager();
        try {
            this.dataManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(DATA_MANAGER_ERROR, ie);
        }

        // Start Execution Manager
        ExecutionManager em = null;
        try {
            em = new ExecutionManager(this, computingUnitsCPU, computingUnitsGPU, cpuMap, gpuMap, limitOfTasks);
        } catch (InvalidMapException ime) {
            ErrorManager.fatal(ime);
            return;
        } finally {
            this.executionManager = em;
        }

        if (tracing_level == NIOTracer.BASIC_MODE) {
            NIOTracer.enablePThreads();
        }

        try {
            this.executionManager.init();
        } catch (InitializationException ie) {
            ErrorManager.error(EXECUTION_MANAGER_ERR, ie);
        }

        if (tracing_level == NIOTracer.BASIC_MODE) {
            NIOTracer.disablePThreads();
        }

    }

    @Override
    public void setWorkerIsReady(String nodeName) {
        // Implemented on NIOAdaptor to notify that the worker is up and ready
    }

    @Override
    public void setMaster(NIONode master) {
        if (masterNode == null) {
            masterNode = new NIONode(master.getIp(), masterPort);
        }
    }

    @Override
    public boolean isMyUuid(String uuid) {
        return uuid.equals(this.deploymentId);
    }

    public static boolean isWorkerDebugEnabled() {
        return isWorkerDebugEnabled;
    }

    public static String getExecutionType() {
        return executionType;
    }

    public static boolean isTracingEnabled() {
        return NIOTracer.isActivated();
    }

    public static String getStorageConf() {
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            return "null";
        } else {
            return storageConf;
        }
    }

    public static boolean isPersistentCEnabled() {
        return persistentC;
    }

    public ExecutionManager getExecutionManager() {
        return this.executionManager;
    }

    public String getLang() {
        return this.lang;
    }

    @Override
    public String getWorkingDir() {
        return workingDir;
    }

    public String getInstallDir() {
        return this.installDir;
    }

    public String getAppDir() {
        return this.appDir;
    }

    public String getLibPath() {
        return this.libraryPath;
    }

    public String getClasspath() {
        return this.classpath;
    }

    public String getPythonpath() {
        return this.pythonpath;
    }

    @Override
    public void receivedNewTask(NIONode master, NIOTask task, List<String> obsoleteFiles) {
        WORKER_LOGGER.info("Received Job " + task);

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.WORKER_RECEIVED_NEW_TASK.getId(), NIOTracer.Event.WORKER_RECEIVED_NEW_TASK.getType());
        }

        // Remove obsolete
        if (obsoleteFiles != null) {
            removeObsolete(obsoleteFiles);
        }

        // Demand files
        WORKER_LOGGER.info("Checking parameters");
        TransferringTask tt = new TransferringTask(task);
        int i = 0;
        for (NIOParam param : task.getParams()) {
            i++;
            if (param.getData() != null) {
                // Parameter has associated data
                WORKER_LOGGER.debug("- Checking transfers for data of parameter " + (String) param.getValue());

                switch (param.getType()) {
                    case OBJECT_T:
                        askForObject(param, i, tt);
                        break;
                    case PSCO_T:
                        askForPSCO(param);
                        break;
                    case EXTERNAL_OBJECT_T:
                        // Nothing to do since external parameters send their ID directly
                        break;
                    case FILE_T:
                        askForFile(param, i, tt);
                        break;
                    default:
                        // OTHERS: Strings or basic types
                        // In any case, there is nothing to do for these type of parameters
                        break;
                }
            } else {
                // OUT parameter. Has no associated data. Decrease the parameter counter (we already have it)
                tt.decreaseParams();
            }
        }

        // Request the transfers
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(tt.getTask().getTaskId(), NIOTracer.getTaskTransfersType());
        }
        requestTransfers();
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getTaskTransfersType());
        }

        if (tt.getParams() == 0) {
            executeTask(tt.getTask());
        }
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.WORKER_RECEIVED_NEW_TASK.getType());
        }
    }

    private void askForPSCO(NIOParam param) {
        String pscoId = (String) param.getValue();
        WORKER_LOGGER.debug("   - " + pscoId + " registered as PSCO.");
        // The replica must have been ordered by the master so the real object must be
        // catched or can be retrieved by the ID

        // Try if parameter is in cache
        WORKER_LOGGER.debug("   - Checking if " + pscoId + " is in cache.");
        boolean inCache = dataManager.checkPresence(pscoId);
        if (!inCache) {
            WORKER_LOGGER.debug("   - Retrieving psco " + pscoId + " from Storage");
            // Get Object from its ID
            Object obj = null;
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
            }
            try {
                obj = StorageItf.getByID(pscoId);
            } catch (StorageException e) {
                WORKER_LOGGER.error("Cannot getByID PSCO " + pscoId, e);
            } finally {
                if (NIOTracer.isActivated()) {
                    NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
                }
            }
            storeObject(pscoId, obj);
        }
        WORKER_LOGGER.debug("   - PSCO with id " + pscoId + " stored");
    }

    private void askForObject(NIOParam param, int index, TransferringTask tt) {
        WORKER_LOGGER.debug("   - " + (String) param.getValue() + " registered as object.");

        boolean askTransfer = false;

        // Try if parameter is in cache
        WORKER_LOGGER.debug("   - Checking if " + (String) param.getValue() + " is in cache.");
        boolean catched = dataManager.checkPresence((String) param.getValue());
        if (!catched) {
            // Try if any of the object locations is in cache
            boolean locationsInCache = false;
            WORKER_LOGGER.debug("   - Checking if " + (String) param.getValue() + " locations are catched");
            for (NIOURI loc : param.getData().getSources()) {
                if (dataManager.checkPresence(loc.getPath())) {
                    // Object found
                    WORKER_LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") location found in cache.");
                    try {
                        if (param.isPreserveSourceData()) {
                            WORKER_LOGGER.debug(
                                    "   - Parameter " + index + "(" + (String) param.getValue() + ") preserves sources. CACHE-COPYING");
                            Object o = Serializer.deserialize(loc.getPath());
                            storeObject((String) param.getValue(), o);
                        } else {
                            WORKER_LOGGER
                                    .debug("   - Parameter " + index + "(" + (String) param.getValue() + ") erases sources. CACHE-MOVING");
                            Object o = dataManager.getObject(loc.getPath());
                            dataManager.remove(loc.getPath());
                            storeObject((String) param.getValue(), o);
                        }
                        locationsInCache = true;
                    } catch (IOException ioe) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        WORKER_LOGGER.error("IOException", ioe);
                    } catch (ClassNotFoundException e) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        WORKER_LOGGER.error("ClassNotFoundException", e);
                    }
                    // Stop looking for locations
                    break;
                }
            }

            if (!locationsInCache) {
                // Try if any of the object locations is in the host
                boolean existInHost = false;
                WORKER_LOGGER.debug("   - Checking if " + (String) param.getValue() + " locations are in host");
                NIOURI loc = param.getData().getURIinHost(host);
                if (loc != null) {
                    WORKER_LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") found at host.");
                    try {
                        File source = new File(workingDir + File.separator + loc.getPath());
                        File target = new File(workingDir + File.separator + param.getValue().toString());
                        if (param.isPreserveSourceData()) {
                            WORKER_LOGGER
                                    .debug("   - Parameter " + index + "(" + (String) param.getValue() + ") preserves sources. COPYING");
                            WORKER_LOGGER.debug("         Source: " + source);
                            WORKER_LOGGER.debug("         Target: " + target);
                            Files.copy(source.toPath(), target.toPath());
                        } else {
                            WORKER_LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") erases sources. MOVING");
                            WORKER_LOGGER.debug("         Source: " + source);
                            WORKER_LOGGER.debug("         Target: " + target);
                            if (!source.renameTo(target)) {
                                WORKER_LOGGER
                                        .error("Error renaming file from " + source.getAbsolutePath() + " to " + target.getAbsolutePath());
                            }
                        }
                        // Move object to cache
                        Object o = Serializer.deserialize((String) param.getValue());
                        storeObject((String) param.getValue(), o);
                        existInHost = true;
                    } catch (IOException ioe) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        WORKER_LOGGER.error("IOException", ioe);
                    } catch (ClassNotFoundException e) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        WORKER_LOGGER.error("ClassNotFoundException", e);
                    }
                }

                if (!existInHost) {
                    // We must transfer the file
                    askTransfer = true;
                }
            }
        }

        // Request the transfer if needed
        askForTransfer(askTransfer, param, index, tt);
    }

    private void askForFile(NIOParam param, int index, TransferringTask tt) {
        WORKER_LOGGER.debug("   - " + (String) param.getValue() + " registered as file.");

        boolean locationsInHost = false;
        boolean askTransfer = false;

        // Try if parameter is in the host
        WORKER_LOGGER.debug("   - Checking if file " + (String) param.getValue() + " exists.");
        File f = new File(param.getValue().toString());
        if (!f.exists()) {
            // Try if any of the locations is in the same host
            WORKER_LOGGER.debug("   - Checking if " + (String) param.getValue() + " exists in worker");
            NIOURI loc = param.getData().getURIinHost(host);
            if (loc != null) {
                // Data is already present at host
                WORKER_LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") found at host.");
                try {
                    File source = new File(loc.getPath());
                    File target = new File(param.getValue().toString());
                    if (param.isPreserveSourceData()) {
                        WORKER_LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") preserves sources. COPYING");
                        WORKER_LOGGER.debug("         Source: " + source);
                        WORKER_LOGGER.debug("         Target: " + target);
                        Files.copy(source.toPath(), target.toPath());
                    } else {
                        WORKER_LOGGER.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") erases sources. MOVING");
                        WORKER_LOGGER.debug("         Source: " + source);
                        WORKER_LOGGER.debug("         Target: " + target);
                        try {
                            Files.move(source.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
                        } catch (AtomicMoveNotSupportedException amnse) {
                            WORKER_LOGGER.warn(
                                    "WARN: AtomicMoveNotSupportedException. File cannot be atomically moved. Trying to move without atomic");
                            Files.move(source.toPath(), target.toPath());
                        }
                    }
                    locationsInHost = true;
                } catch (IOException ioe) {
                    WORKER_LOGGER.error("IOException", ioe);
                }
            }

            if (!locationsInHost) {
                // We must transfer the file
                askTransfer = true;
            }
        } else {
            // Check if it is not currently transferred
            if (getDataRequests(param.getData().getName()) != null) {
                askTransfer = true;
            }
        }

        // Request the transfer if needed
        askForTransfer(askTransfer, param, index, tt);
    }

    private void askForTransfer(boolean askTransfer, NIOParam param, int index, TransferringTask tt) {
        // Request the transfer if needed
        if (askTransfer) {
            WORKER_LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
            DataRequest dr = new WorkerDataRequest(tt, param.getType(), param.getData(), (String) param.getValue());
            addTransferRequest(dr);
        } else {
            // If no transfer, decrease the parameter counter
            // (we already have it)
            WORKER_LOGGER.info("- Parameter " + index + "(" + (String) param.getValue() + ") already exists.");
            tt.decreaseParams();
        }
    }

    @Override
    protected void handleDataToSendNotAvailable(Connection c, Data d) {
        ErrorManager.warn("Data " + d.getName() + "in this worker " + this.getHostName() + " could not be sent to master.");
        c.finishConnection();
    }

    // This is called when the master couldn't send a data to the worker.
    // The master abruptly finishes the connection. The NIOMessageHandler
    // handles this as an error, which treats with its function handleError,
    // and notifies the worker in this case.
    public void handleRequestedDataNotAvailableError(List<DataRequest> failedRequests, String dataId) {
        for (DataRequest dr : failedRequests) { // For every task pending on this request, flag it as an error
            WorkerDataRequest wdr = (WorkerDataRequest) dr;
            wdr.getTransferringTask().decreaseParams();

            // Mark as an error task. When all the params've been consumed, sendTaskDone unsuccessful
            wdr.getTransferringTask().setError(true);
            if (wdr.getTransferringTask().getParams() == 0) {
                sendTaskDone(wdr.getTransferringTask().getTask(), false);
            }

            // Create job*_[NEW|RESUBMITTED|RESCHEDULED].[out|err]
            // If we don't create this when the task fails to retrieve a value,
            // the master will try to get the out of this job, and it will get blocked.
            // Same for the worker when sending, throwing an error when trying
            // to read the job out, which wouldn't exist

            String baseJobPath = workingDir + File.separator + "jobs" + File.separator + "job"
                    + wdr.getTransferringTask().getTask().getJobId() + "_" + wdr.getTransferringTask().getTask().getHist();
            File fout = new File(baseJobPath + ".out");
            File ferr = new File(baseJobPath + ".err");
            if (!fout.exists() || !ferr.exists()) {
                FileOutputStream fos = null;
                try {
                    String errorMessage = "Worker closed because the data " + dataId + " couldn't be retrieved.";
                    fos = new FileOutputStream(fout);
                    fos.write(errorMessage.getBytes());
                    fos.close();
                    fos = new FileOutputStream(ferr);
                    fos.write(errorMessage.getBytes());
                    fos.close();
                } catch (IOException e) {
                    WORKER_LOGGER.error("IOException", e);
                } finally {
                    if (fos != null) {
                        try {
                            fos.close();
                        } catch (IOException e) {
                            WORKER_LOGGER.error("IOException", e);
                        }
                    }
                }
            }
        }

    }

    @Override
    public void receivedValue(Destination type, String dataId, Object object, List<DataRequest> achievedRequests) {
        if (type == Transfer.Destination.OBJECT) {
            WORKER_LOGGER.info("Received data " + dataId + " with associated object " + object);
            storeObject(dataId, object);
        } else {
            WORKER_LOGGER.info("Received data " + dataId);
        }
        for (DataRequest dr : achievedRequests) {
            WorkerDataRequest wdr = (WorkerDataRequest) dr;
            wdr.getTransferringTask().decreaseParams();
            if (NIOTracer.isActivated()) {
                NIOTracer.emitDataTransferEvent(NIOTracer.TRANSFER_END);
            }
            if (wdr.getTransferringTask().getParams() == 0) {
                if (!wdr.getTransferringTask().getError()) {
                    executeTask(wdr.getTransferringTask().getTask());
                } else {
                    sendTaskDone(wdr.getTransferringTask().getTask(), false);
                }
            }
        }
    }

    public void sendTaskDone(NIOTask nt, boolean successful) {
        int taskID = nt.getJobId();

        // Notify task done
        int retries = 0;
        Connection c = null;
        while (retries < MAX_RETRIES) {
            try {
                c = TM.startConnection(masterNode);
                if (c == null) {
                    throw new Exception("Nullable connection");
                }
                break;
            } catch (Exception e) {
                if (retries >= MAX_RETRIES) {
                    WORKER_LOGGER.error("Exception sending Task notification", e);
                    return;
                } else {
                    try {
                        Thread.sleep(10);
                    } catch (InterruptedException e1) {
                        Thread.currentThread().interrupt();
                    }
                    retries++;
                }
            }
        }

        NIOTaskResult tr = new NIOTaskResult(taskID, nt.getParams());
        if (WORKER_LOGGER.isDebugEnabled()) {
            WORKER_LOGGER.debug("TASK RESULT FOR TASK ID " + taskID);
            WORKER_LOGGER.debug(tr);
        }
        CommandTaskDone cmd = new CommandTaskDone(this, tr, successful);
        c.sendCommand(cmd);

        if (isWorkerDebugEnabled) {
            c.sendDataFile(workingDir + File.separator + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist() + ".out");
            c.sendDataFile(workingDir + File.separator + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist() + ".err");
        } else {
            if (!successful) {
                c.sendDataFile(workingDir + File.separator + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist() + ".out");
                c.sendDataFile(workingDir + File.separator + "jobs" + File.separator + "job" + nt.getJobId() + "_" + nt.getHist() + ".err");
            }
        }
        c.finishConnection();
    }

    // Check if this task is ready to execute
    private void executeTask(NIOTask task) {
        if (isWorkerDebugEnabled) {
            WORKER_LOGGER.debug("Enqueueing job " + task.getJobId() + " for execution.");
        }

        // Execute the job
        executionManager.enqueue(task);

        // Notify the master that the data has been transfered
        // The message is sent after the task enqueue because the connection can
        // have N pending task transfer and will wait until they
        // are finished to send all the answers (blocking the task execution)
        if (isWorkerDebugEnabled) {
            WORKER_LOGGER.debug("Notifying presence of all data for job " + task.getJobId() + ".");
        }

        CommandDataReceived cdr = new CommandDataReceived(this, task.getTransferGroupId());
        for (int retries = 0; retries < MAX_RETRIES; retries++) {
            if (tryNofiyDataReceived(cdr)) {
                return;
            } else {
                try {
                    Thread.sleep(10);
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
    }

    private boolean tryNofiyDataReceived(CommandDataReceived cdr) {
        Connection c = TM.startConnection(masterNode);
        c.sendCommand(cdr);
        c.finishConnection();

        return true;
    }

    // Remove obsolete files and objects
    public void removeObsolete(List<String> obsolete) {
        try {
            for (String name : obsolete) {
                if (name.startsWith(File.separator)) {
                    WORKER_LOGGER.debug("Removing file " + name);
                    File f = new File(name);
                    if (!f.delete()) {
                        WORKER_LOGGER.error("Error removing file " + f.getAbsolutePath());
                    }
                } else {
                    removeFromCache(name);
                }
            }
        } catch (Exception e) {
            WORKER_LOGGER.error("Exception", e);
        }
    }

    public void receivedUpdateSources(Connection c) {

    }

    public void shutdownExecutionManager(Connection closingConnection) {
        // Stop the Execution Manager
        executionManager.stop();

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

        // Stop the Data Manager
        dataManager.stop();

        // Finish the main thread
        if (closingConnection != null) {
            closingConnection.sendCommand(new CommandShutdownACK());
            closingConnection.finishConnection();
        }

        TM.shutdown(closingConnection);

        // End storage
        String storageConf = System.getProperty(ITConstants.IT_STORAGE_CONF);
        if (storageConf != null && !storageConf.equals("") && !storageConf.equals("null")) {
            try {
                StorageItf.finish();
            } catch (StorageException e) {
                WORKER_LOGGER.error("Error releasing storage library: " + e.getMessage(), e);
            }
        }

        // Remove workingDir
        if (removeWD) {
            WORKER_LOGGER.debug("Erasing Worker Sandbox WorkingDir: " + this.workingDir);
            try {
                removeFolder(this.workingDir);
            } catch (IOException ioe) {
                WORKER_LOGGER.error("Exception", ioe);
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
                for (File child : f.listFiles()) {
                    remove(child);
                }
            }
            Files.delete(f.toPath());
        }
    }

    @Override
    public Object getObject(String name) throws SerializedObjectException {
        String realName = name.substring(name.lastIndexOf('/') + 1);
        return dataManager.getObject(realName);
    }

    public Object getPersistentObject(String id) throws StorageException {
        // Get PSCO if cached
        if (dataManager.checkPresence(id)) {
            return dataManager.getObject(id);
        }

        // If there was any problem on transfer, try to get it now by id from
        // any other host (done by storage getById)
        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
        }

        Object obj = null;
        try {
            obj = StorageItf.getByID(id);
            dataManager.storeObject(id, obj);
        } catch (StorageException e) {
            throw e;
        } finally {
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
            }
        }

        return obj;
    }

    @Override
    public String getObjectAsFile(String s) {
        // This method should never be called in the worker side
        WORKER_LOGGER.warn("getObjectAsFile has been called in the worker side!");

        return null;
    }

    public void storeObject(String name, Object value) {
        dataManager.storeObject(name, value);
    }

    public void storePersistentObject(String id, Object value) {
        dataManager.storeObject(id, value);
    }

    public void removeFromCache(String name) {
        dataManager.remove(name);
    }

    public static void registerOutputs(String path) {
        err.registerThread(path);
        out.registerThread(path);
    }

    public static void unregisterOutputs() {
        err.unregisterThread();
        out.unregisterThread();
    }

    @Override
    public void receivedTaskDone(Connection c, NIOTaskResult tr, boolean successful) {
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

    public String getHostName() {
        return this.host;
    }

    @Override
    public void waitUntilTracingPackageGenerated() {
        // Nothing to do

    }

    @Override
    public void notifyTracingPackageGeneration() {
        // Nothing to do
    }

    @Override
    public void waitUntilWorkersDebugInfoGenerated() {
        // Nothing to do
    }

    @Override
    public void notifyWorkersDebugInfoGeneration() {
        // Nothing to do
    }

    @Override
    public void generateWorkersDebugInfo(Connection c) {
        // Freeze output
        String outSource = workingDir + File.separator + "log" + File.separator + "worker_" + host + ".out";
        String outTarget = workingDir + File.separator + "log" + File.separator + "static_" + "worker_" + host + ".out";
        if (new File(outSource).exists()) {
            try {
                Files.copy(new File(outSource).toPath(), new File(outTarget).toPath());
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception", e);
            }
        } else {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(outTarget);
                fos.write("Empty file".getBytes());
                fos.close();
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception", e);
            } finally {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (Exception e) {
                        WORKER_LOGGER.error("Exception", e);
                    }
                }
            }
        }

        // Freeze error
        String errSource = workingDir + File.separator + "log" + File.separator + "worker_" + host + ".err";
        String errTarget = workingDir + File.separator + "log" + File.separator + "static_" + "worker_" + host + ".err";
        if (new File(errSource).exists()) {
            try {
                Files.copy(new File(errSource).toPath(), new File(errTarget).toPath());
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception", e);
            }
        } else {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(errTarget);
                fos.write("Empty file".getBytes());
                fos.close();
            } catch (Exception e) {
                WORKER_LOGGER.error("Exception", e);
            } finally {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (Exception e) {
                        WORKER_LOGGER.error("Exception", e);
                    }
                }
            }
        }

        // End
        c.sendCommand(new CommandWorkerDebugFilesDone());
        c.finishConnection();
    }

    public static void main(String[] args) {
        // Check arguments length
        if (args.length != (NUM_PARAMS_NIO_WORKER)) {
            if (WORKER_LOGGER_DEBUG) {
                WORKER_LOGGER.debug("Received parameters: ");
                for (int i = 0; i < args.length; ++i) {
                    WORKER_LOGGER.debug("Param " + i + ":  " + args[i]);
                }
            }
            ErrorManager.fatal(ERROR_INCORRECT_NUM_PARAMS);
        }

        // Parse arguments
        isWorkerDebugEnabled = Boolean.valueOf(args[0]);

        int maxSnd = Integer.parseInt(args[1]);
        int maxRcv = Integer.parseInt(args[2]);
        String workerIP = args[3];
        int wPort = Integer.parseInt(args[4]);
        int mPort = Integer.parseInt(args[5]);

        int computingUnitsCPU = Integer.parseInt(args[6]);
        int computingUnitsGPU = Integer.parseInt(args[7]);
        String cpuMap = args[8];
        String gpuMap = args[9];
        int limitOfTasks = Integer.parseInt(args[10]);

        String appUuid = args[11];
        String lang = args[12];
        String workingDir = args[13];
        String installDir = args[14];
        String appDir = args[15];
        String libPath = args[16];
        String classpath = args[17];
        String pythonpath = args[18];

        String trace = args[19];
        String extraeFile = args[20];
        String host = args[21];

        storageConf = args[22];
        executionType = args[23];

        persistentC = Boolean.parseBoolean(args[24]);

        // Print arguments
        if (isWorkerDebugEnabled) {
            WORKER_LOGGER.debug("maxSnd: " + String.valueOf(maxSnd));
            WORKER_LOGGER.debug("maxRcv: " + String.valueOf(maxRcv));

            WORKER_LOGGER.debug("WorkerName: " + workerIP);
            WORKER_LOGGER.debug("WorkerPort: " + String.valueOf(wPort));
            WORKER_LOGGER.debug("MasterPort: " + String.valueOf(mPort));

            WORKER_LOGGER.debug("Computing Units CPU: " + String.valueOf(computingUnitsCPU));
            WORKER_LOGGER.debug("Computing Units GPU: " + String.valueOf(computingUnitsGPU));
            WORKER_LOGGER.debug("User defined CPU Map: " + cpuMap);
            WORKER_LOGGER.debug("User defined GPU Map: " + gpuMap);
            WORKER_LOGGER.debug("Limit Of Tasks: " + String.valueOf(limitOfTasks));

            WORKER_LOGGER.debug("App uuid: " + appUuid);
            WORKER_LOGGER.debug("WorkingDir:" + workingDir);
            WORKER_LOGGER.debug("Install Dir: " + installDir);

            WORKER_LOGGER.debug("Tracing: " + trace);
            WORKER_LOGGER.debug("Extrae config File: " + extraeFile);
            WORKER_LOGGER.debug("Host: " + host);

            WORKER_LOGGER.debug("LibraryPath: " + libPath);
            WORKER_LOGGER.debug("Classpath: " + classpath);
            WORKER_LOGGER.debug("Pythonpath: " + pythonpath);

            WORKER_LOGGER.debug("StorageConf: " + storageConf);
            WORKER_LOGGER.debug("executionType: " + executionType);

            WORKER_LOGGER.debug("Persistent c: " + persistentC);

            WORKER_LOGGER.debug("Remove Sanbox WD: " + removeWD);
        }

        // Configure storage
        System.setProperty(ITConstants.IT_STORAGE_CONF, storageConf);
        try {
            if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
                WORKER_LOGGER.warn("No storage configuration file passed");
            } else {
                StorageItf.init(storageConf);
            }
        } catch (StorageException e) {
            ErrorManager.fatal("Error loading storage configuration file: " + storageConf, e);
        }

        // Configure tracing
        System.setProperty(ITConstants.IT_EXTRAE_CONFIG_FILE, extraeFile);
        tracing_level = Integer.parseInt(trace);
        if (tracing_level > 0) {
            NIOTracer.init(tracing_level);
            NIOTracer.emitEvent(NIOTracer.Event.START.getId(), NIOTracer.Event.START.getType());

            try {
                tracingID = Integer.parseInt(host);
                NIOTracer.setWorkerInfo(installDir, workerIP, workingDir, tracingID);
            } catch (Exception e) {
                WORKER_LOGGER.error("No valid hostID provided to the tracing system. Provided ID: " + host);
            }
        }

        /*
         * ***********************************************************************************************************
         * LAUNCH THE WORKER
         *************************************************************************************************************/
        NIOWorker nw = new NIOWorker(maxSnd, maxRcv, workerIP, mPort, computingUnitsCPU, computingUnitsGPU, cpuMap, gpuMap, limitOfTasks,
                appUuid, lang, workingDir, installDir, appDir, libPath, classpath, pythonpath);

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
        WORKER_LOGGER.debug("  Starting TransferManager Thread");
        TM.start();
        try {
            TM.startServer(new NIONode(null, wPort));
        } catch (CommException ce) {
            WORKER_LOGGER.error("Error starting TransferManager Server at Worker" + nw.getHostName(), ce);
            nw.shutdown(null);
            return;
        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.START.getType());
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

}
