package integratedtoolkit.nio.worker;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.StandardCopyOption;
import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;
import es.bsc.comm.exceptions.CommException;
import es.bsc.comm.Connection;
import es.bsc.comm.nio.NIONode;
import es.bsc.comm.stage.Transfer;
import es.bsc.comm.stage.Transfer.Destination;
import integratedtoolkit.ITConstants;
import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOParam;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.NIOTaskResult;
import integratedtoolkit.nio.NIOURI;
import integratedtoolkit.nio.commands.CommandDataReceived;
import integratedtoolkit.nio.commands.Data;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOMessageHandler;
import integratedtoolkit.nio.commands.CommandShutdownACK;
import integratedtoolkit.nio.commands.CommandTaskDone;
import integratedtoolkit.nio.commands.workerFiles.CommandWorkerDebugFilesDone;
import integratedtoolkit.nio.dataRequest.DataRequest;
import integratedtoolkit.nio.dataRequest.WorkerDataRequest;
import integratedtoolkit.nio.dataRequest.WorkerDataRequest.TransferringTask;
import integratedtoolkit.nio.exceptions.SerializedObjectException;
import integratedtoolkit.nio.worker.components.DataManager;
import integratedtoolkit.nio.worker.components.ExecutionManager;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableCoresException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableGPUsException;
import integratedtoolkit.nio.NIOTracer;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.Tracer;


public class NIOWorker extends NIOAgent {

    // General configuration attributes
    private static final int MAX_RETRIES = 5;

    // Logger
    private static final Logger wLogger = LogManager.getLogger(Loggers.WORKER);
    private static final boolean wLoggerDebug = wLogger.isDebugEnabled();
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
    private static String executionType;

    // Internal components
    private final ExecutionManager executionManager;
    private final DataManager dataManager;

    // Processes to capture out/err of each job
    private static ThreadPrintStream out;
    private static ThreadPrintStream err;
    public static final String SUFFIX_OUT = ".out";
    public static final String SUFFIX_ERR = ".err";
    
    // Bound Resources
    private int[] boundCoreUnits;
    private int[] boundGPUs;

    static {
        try {
            out = new ThreadPrintStream(SUFFIX_OUT, System.out);
            err = new ThreadPrintStream(SUFFIX_ERR, System.err);
            System.setErr(err);
            System.setOut(out);
        } catch (Exception e) {
            wLogger.error("Exception", e);
        }
    }


    public NIOWorker(int numJobThreads, int snd, int rcv, int masterPort, String appUuid, String lang, String hostName, String workingDir,
            String installDir, String appDir, String libPath, String classpath, String pythonpath, int numGPUs) {

        super(snd, rcv, masterPort);

        // Log worker creation
        wLogger.info("NIO Worker init");

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
        
        // Set every resource assigned job to -1 (no job assigned to that CU)
        this.boundCoreUnits = new int[numJobThreads];
        for (int i = 0; i < numJobThreads; i++){
        	boundCoreUnits[i] = -1;
        }

        this.boundGPUs = new int[numGPUs];
        for (int i = 0; i < numGPUs; i++){
        	boundGPUs[i] = -1;
        }
        
        
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
        this.executionManager = new ExecutionManager(this, numJobThreads);

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
        // implemented on NIOAdaptor to notify that the worker is up and ready
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
    
    /**
     * Bind numCUs core units to the job
     * 
     * @param jobId
     * @param numCUs
     * @return
     * @throws UnsufficientAvailableCoresException
     */
    public int[] bindCoreUnits(int jobId, int numCUs) throws UnsufficientAvailableCoresException {
    	int assignedCoreUnits[] = new int[numCUs];
		boolean done = false;
		int numAssignedCores = 0;
	
		// Assign free CUs to the job
		synchronized(boundCoreUnits){
		    for (int coreId = 0; coreId < boundCoreUnits.length && !done; ++coreId) {
				if (boundCoreUnits[coreId] == -1) {		
					boundCoreUnits[coreId] = jobId;
    				assignedCoreUnits[numAssignedCores] = coreId;
    				numAssignedCores++;
				}
				done = (numAssignedCores == numCUs);
			}	
		}
		
		// If the job doesn't have all the CUs it needs, it cannot run on occupied ones
		// Raise exception
		if (!done) {
		    throw new UnsufficientAvailableCoresException("Not enough available cores for task execution");
		}

		return assignedCoreUnits;
    }
    
    /**
     * Release core units occupied by the job
     * 
     * @param jobId
     */
    public void releaseCoreUnits(int jobId){
    	synchronized(boundCoreUnits){
			for (int coreId = 0; coreId < boundCoreUnits.length; coreId++){
				if (boundCoreUnits[coreId] == jobId) {
					boundCoreUnits[coreId] = -1;
				}
			}
    	}
    }

    
    
    /**
     * Bind numGPUs core units to the job
     * 
     * @param jobId
     * @param numGPUs
     * @return
     * @throws UnsufficientAvailableGPUsException
     */
    public int[] bindGPUs(int jobId, int numGPUs) throws UnsufficientAvailableGPUsException {
    	int assignedGPUs[] = new int[numGPUs];
		boolean done = false;
		if (numGPUs == 0){
			done = true;
		}
		int numAssignedGPUs = 0;
	
		// Assign free GPUUs to the job
		synchronized(boundGPUs){
		    for (int GPUId = 0; GPUId < boundGPUs.length && !done; ++GPUId) {
				if (boundGPUs[GPUId] == -1) {		
					boundGPUs[GPUId] = jobId;
    				assignedGPUs[numAssignedGPUs] = GPUId;
    				numAssignedGPUs++;
				}
				done = (numAssignedGPUs == numGPUs);
			}	
		}
		
		// If the job doesn't have all the CUs it needs, it cannot run on occupied ones
		// Raise exception
		if (!done) {
		    throw new UnsufficientAvailableGPUsException("Not enough available GPUs for task execution");
		}
		
		return assignedGPUs;
    }
    
    /**
     * Release GPUs occupied by the job
     * 
     * @param jobId
     */
    public void releaseGPUs(int jobId){
    	synchronized(boundGPUs){
			for (int GPUId = 0; GPUId < boundGPUs.length; GPUId++){
				if (boundGPUs[GPUId] == jobId) {
					boundGPUs[GPUId] = -1;
				}
			}
    	}
    }    
    
    @Override
    public void receivedNewTask(NIONode master, NIOTask task, LinkedList<String> obsoleteFiles) {
        wLogger.info("Received Job " + task);

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.Event.RECEIVED_NEW_TASK.getId(), NIOTracer.Event.RECEIVED_NEW_TASK.getType());
        }

        // Remove obsolete
        if (obsoleteFiles != null) {
            removeObsolete(obsoleteFiles);
        }

        // Demand files
        wLogger.info("Checking parameters");
        TransferringTask tt = new TransferringTask(task);
        int i = 0;
        for (NIOParam param : task.getParams()) {
            i++;
            if (param.getData() != null) {
                // Parameter has associated data
                wLogger.debug("- Checking transfers for data of parameter " + (String) param.getValue());

                switch (param.getType()) {
                    case PSCO_T:
                        askForPSCO(param);
                        break;
                    case OBJECT_T:
                    case EXTERNAL_PSCO_T:
                        askForObject(param, i, tt);
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
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.RECEIVED_NEW_TASK.getType());
        }
    }
    
    private void askForPSCO(NIOParam param) {
        String pscoId = (String) param.getValue();
        wLogger.debug("   - " + pscoId + " registered as PSCO.");
        // The replica must have been ordered by the master so the real object must be
        // catched or can be retrieved by the ID

        // Try if parameter is in cache
        wLogger.debug("   - Checking if " + pscoId + " is in cache.");
        boolean inCache = dataManager.checkPresence(pscoId);
        if (!inCache) {
            wLogger.debug("   - Retrieving psco " + pscoId + " from Storage");
            // Get Object from its ID
            Object obj = null;
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(Tracer.Event.STORAGE_GETBYID.getId(), Tracer.Event.STORAGE_GETBYID.getType());
            }
            try {
                obj = StorageItf.getByID(pscoId);
            } catch (StorageException e) {
                wLogger.error("Cannot getByID PSCO " + pscoId, e);
            } finally {
                if (NIOTracer.isActivated()) {
                    NIOTracer.emitEvent(Tracer.EVENT_END, Tracer.Event.STORAGE_GETBYID.getType());
                }
            }
            storeObject(pscoId, obj);
        }
        wLogger.debug("   - PSCO with id " + pscoId + " stored");
    }
    
    private void askForObject(NIOParam param, int index, TransferringTask tt) {
        wLogger.debug("   - " + (String) param.getValue() + " registered as object.");

        boolean askTransfer = false;
        
        // Try if parameter is in cache
        wLogger.debug("   - Checking if " + (String) param.getValue() + " is in cache.");
        boolean catched = dataManager.checkPresence((String) param.getValue());
        if (!catched) {
            // Try if any of the object locations is in cache
            boolean locationsInCache = false;
            wLogger.debug("   - Checking if " + (String) param.getValue() + " locations are catched");
            for (NIOURI loc : param.getData().getSources()) {
                if (dataManager.checkPresence(loc.getPath())) {
                    // Object found
                    wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") location found in cache.");
                    try {
                        if (param.isPreserveSourceData()) {
                            wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue()
                                    + ") preserves sources. CACHE-COPYING");
                            Object o = Serializer.deserialize(loc.getPath());
                            storeObject((String) param.getValue(), o);
                        } else {
                            wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue()
                                    + ") erases sources. CACHE-MOVING");
                            Object o = dataManager.getObject(loc.getPath());
                            dataManager.remove(loc.getPath());
                            storeObject((String) param.getValue(), o);
                        }
                        locationsInCache = true;
                    } catch (IOException ioe) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        wLogger.error("IOException", ioe);
                    } catch (ClassNotFoundException e) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        wLogger.error("ClassNotFoundException", e);
                    }
                    // Stop looking for locations
                    break;
                }
            }

            if (!locationsInCache) {
                // Try if any of the object locations is in the host
                boolean existInHost = false;
                wLogger.debug("   - Checking if " + (String) param.getValue() + " locations are in host");
                NIOURI loc = param.getData().getURIinHost(host);
                if (loc != null) {
                    wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") found at host.");
                    try {
                        File source = new File(workingDir + File.separator + loc.getPath());
                        File target = new File(workingDir + File.separator + param.getValue().toString());
                        if (param.isPreserveSourceData()) {
                            wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue()
                                    + ") preserves sources. COPYING");
                            wLogger.debug("         Source: " + source);
                            wLogger.debug("         Target: " + target);
                            Files.copy(source.toPath(), target.toPath());
                        } else {
                            wLogger.debug(
                                    "   - Parameter " + index + "(" + (String) param.getValue() + ") erases sources. MOVING");
                            wLogger.debug("         Source: " + source);
                            wLogger.debug("         Target: " + target);
                            if (!source.renameTo(target)) {
                                wLogger.error("Error renaming file from " + source.getAbsolutePath() 
                                    + " to " + target.getAbsolutePath());
                            }
                        }
                        // Move object to cache
                        Object o = Serializer.deserialize((String) param.getValue());
                        storeObject((String) param.getValue(), o);
                        existInHost = true;
                    } catch (IOException ioe) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        wLogger.error("IOException", ioe);
                    } catch (ClassNotFoundException e) {
                        // If exception is raised, locationsInCache remains false. We log the exception
                        // and try host files
                        wLogger.error("ClassNotFoundException", e);
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
        wLogger.debug("   - " + (String) param.getValue() + " registered as file.");

        boolean locationsInHost = false;
        boolean askTransfer = false;

        // Try if parameter is in the host
        wLogger.debug("   - Checking if file " + (String) param.getValue() + " exists.");
        File f = new File(param.getValue().toString());
        if (!f.exists()) {
            // Try if any of the locations is in the same host
            wLogger.debug("   - Checking if " + (String) param.getValue() + " exists in worker");
            NIOURI loc = param.getData().getURIinHost(host);
            if (loc != null) {
                // Data is already present at host
                wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") found at host.");
                try {
                    File source = new File(loc.getPath());
                    File target = new File(param.getValue().toString());
                    if (param.isPreserveSourceData()) {
                        wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") preserves sources. COPYING");
                        wLogger.debug("         Source: " + source);
                        wLogger.debug("         Target: " + target);
                        Files.copy(source.toPath(), target.toPath());
                    } else {
                        wLogger.debug("   - Parameter " + index + "(" + (String) param.getValue() + ") erases sources. MOVING");
                        wLogger.debug("         Source: " + source);
                        wLogger.debug("         Target: " + target);
                        Files.move(source.toPath(), target.toPath(), StandardCopyOption.ATOMIC_MOVE);
                    }
                    locationsInHost = true;
                } catch (IOException ioe) {
                    wLogger.error("IOException", ioe);
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
            wLogger.info(
                    "- Parameter " + index + "(" + (String) param.getValue() + ") does not exist, requesting data transfer");
            DataRequest dr = new WorkerDataRequest(tt, param.getType(), param.getData(), (String) param.getValue());
            addTransferRequest(dr);
        } else {
            // If no transfer, decrease the parameter counter
            // (we already have it)
            wLogger.info("- Parameter " + index + "(" + (String) param.getValue() + ") already exists.");
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
    public void handleRequestedDataNotAvailableError(LinkedList<DataRequest> failedRequests, String dataId) {
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
                    wLogger.error("IOException", e);
                } finally {
                    if (fos != null) {
                        try {
                            fos.close();
                        } catch (IOException e) {
                            wLogger.error("IOException", e);
                        }
                    }
                }
            }
        }

    }

    @Override
    public void receivedValue(Destination type, String dataId, Object object, LinkedList<DataRequest> achievedRequests) {   	
        if (type == Transfer.Destination.OBJECT) {
            wLogger.info("Received data " + dataId + " with associated object " + object);
            storeObject(dataId, object);
        } else {
            wLogger.info("Received data " + dataId);
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
                c = tm.startConnection(masterNode);
                break;
            } catch (Exception e) {
                if (retries >= MAX_RETRIES) {
                    wLogger.error("Exception sending Task notification", e);
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
        if (wLogger.isDebugEnabled()) {
            wLogger.debug("TASK RESULT FOR TASK ID " + taskID);
            wLogger.debug(tr);
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
            wLogger.debug("Enqueueing job " + task.getJobId() + " for execution.");
        }

        // Execute the job
        executionManager.enqueue(task);

        // Notify the master that the data has been transfered
        // The message is sent after the task enqueue because the connection can
        // have N pending task transfer and will wait until they
        // are finished to send all the answers (blocking the task execution)
        if (isWorkerDebugEnabled) {
            wLogger.debug("Notifying presence of all data for job " + task.getJobId() + ".");
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
        Connection c = tm.startConnection(masterNode);
        c.sendCommand(cdr);
        c.finishConnection();

        return true;
    }

    // Remove obsolete files and objects
    public void removeObsolete(LinkedList<String> obsolete) {
        try {
            for (String name : obsolete) {
                if (name.startsWith(File.separator)) {
                    File f = new File(name);
                    if (!f.delete()) {
                        wLogger.error("Error removing file " + f.getAbsolutePath());
                    }
                } else {
                    removeFromCache(name);
                }
            }
        } catch (Exception e) {
            wLogger.error("Exception", e);
        }
    }

    public void receivedUpdateSources(Connection c) {

    }

    // Shutdown the worker, at this point there are no active transfers
    public void shutdown(Connection closingConnection) {
        wLogger.debug("Entering shutdown method on worker");
        try {
            // Stop the Execution Manager
            executionManager.stop();

            // Stop the Data Manager
            dataManager.stop();

            // Finish the main thread
            if (closingConnection != null) {
                closingConnection.sendCommand(new CommandShutdownACK());
                closingConnection.finishConnection();
            }
            tm.shutdown(closingConnection);

            // End storage
            String storageConf = System.getProperty(ITConstants.IT_STORAGE_CONF);
            if (storageConf != null && !storageConf.equals("") && !storageConf.equals("null")) {
                try {
                    StorageItf.finish();
                } catch (StorageException e) {
                    wLogger.error("Error releasing storage library: " + e.getMessage(), e);
                }
            }

            // Remove workingDir
            if (removeWD) {
                wLogger.debug("Erasing Worker Sandbox WorkingDir");
                String wDir = workingDir.substring(0, workingDir.indexOf(deploymentId) + deploymentId.length());
                removeFolder(wDir);
            }
        } catch (Exception e) {
            wLogger.error("Exception", e);
        }
        wLogger.debug("Finish shutdown method on worker");
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
        wLogger.warn("getObjectAsFile has been called in the worker side!");

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
                wLogger.error("Exception", e);
            }
        } else {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(outTarget);
                fos.write("Empty file".getBytes());
                fos.close();
            } catch (Exception e) {
                wLogger.error("Exception", e);
            } finally {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (Exception e) {
                        wLogger.error("Exception", e);
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
                wLogger.error("Exception", e);
            }
        } else {
            FileOutputStream fos = null;
            try {
                fos = new FileOutputStream(errTarget);
                fos.write("Empty file".getBytes());
                fos.close();
            } catch (Exception e) {
                wLogger.error("Exception", e);
            } finally {
                if (fos != null) {
                    try {
                        fos.close();
                    } catch (Exception e) {
                        wLogger.error("Exception", e);
                    }
                }
            }
        }

        // End
        c.sendCommand(new CommandWorkerDebugFilesDone());
        c.finishConnection();
    }

    public static void main(String[] args) {
        /*
         * **************************************
         * Get arguments
         **************************************/
        if (args.length != NUM_PARAMS_NIO_WORKER) {
            if (wLoggerDebug) {
                wLogger.debug("Received parameters: ");
                for (int i = 0; i < args.length; ++i) {
                    wLogger.debug("Param " + i + ":  " + args[i]);
                }
            }

            ErrorManager.fatal(ERROR_INCORRECT_NUM_PARAMS);
        }

        isWorkerDebugEnabled = Boolean.valueOf(args[0]);

        int jobThreads = new Integer(args[1]);
        int maxSnd = new Integer(args[2]);
        int maxRcv = new Integer(args[3]);

        String workerIP = args[4];
        int wPort = new Integer(args[5]);
        int mPort = new Integer(args[6]);

        String appUuid = args[7];
        String lang = args[8];
        String workingDir = args[9];
        String installDir = args[10];
        String appDir = args[11];
        String libPath = args[12];
        String classpath = args[13];
        String pythonpath = args[14];

        String trace = args[15];
        String extraeFile = args[16];
        String host = args[17];

        String storageConf = args[18];
        executionType = args[19];
        
        int numGPUs = Integer.parseInt(args[20]);
        
        
        /*
         * ************************************** 
         * Print args
         **************************************/
        if (isWorkerDebugEnabled) {
            wLogger.debug("jobThreads: " + String.valueOf(jobThreads));
            wLogger.debug("maxSnd: " + String.valueOf(maxSnd));
            wLogger.debug("maxRcv: " + String.valueOf(maxRcv));

            wLogger.debug("WorkerName: " + workerIP);
            wLogger.debug("WorkerPort: " + String.valueOf(wPort));
            wLogger.debug("MasterPort: " + String.valueOf(mPort));

            wLogger.debug("App uuid: " + appUuid);
            wLogger.debug("WorkingDir:" + workingDir);
            wLogger.debug("Install Dir: " + installDir);

            wLogger.debug("Tracing: " + trace);
            wLogger.debug("Extrae config File: " + extraeFile);
            wLogger.debug("Host: " + host);

            wLogger.debug("LibraryPath: " + libPath);
            wLogger.debug("Classpath: " + classpath);
            wLogger.debug("Pythonpath: " + pythonpath);

            wLogger.debug("StorageConf: " + storageConf);
            wLogger.debug("executionType: " + executionType);
            
            wLogger.debug("Gpus per node: " + String.valueOf(numGPUs));

            wLogger.debug("Remove Sanbox WD: " + removeWD);
        }

        /*
         * ************************************** 
         * Configure Storage
         **************************************/
        System.setProperty(ITConstants.IT_STORAGE_CONF, storageConf);
        try {
            if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
                wLogger.warn("No storage configuration file passed");
            } else {
                StorageItf.init(storageConf);
            }
        } catch (StorageException e) {
            ErrorManager.fatal("Error loading storage configuration file: " + storageConf, e);
        }

        /*
         * ************************************** 
         * Configure tracing
         **************************************/
        System.setProperty(ITConstants.IT_EXTRAE_CONFIG_FILE, extraeFile);
        tracing_level = Integer.parseInt(trace);

        // Initialize tracing system
        if (tracing_level > 0) {
            NIOTracer.init(tracing_level);
            NIOTracer.emitEvent(NIOTracer.Event.START.getId(), NIOTracer.Event.START.getType());

            try {
                tracingID = Integer.parseInt(host);
                NIOTracer.setWorkerInfo(installDir, workerIP, workingDir, tracingID);
            } catch (Exception e) {
                wLogger.error("No valid hostID provided to the tracing system. Provided ID: " + host);
            }
        }

        /*
         * ************************************** 
         * LAUNCH THE WORKER
         **************************************/
        NIOWorker nw = new NIOWorker(jobThreads, maxSnd, maxRcv, mPort, appUuid, lang, workerIP, workingDir, installDir, appDir, libPath,
                classpath, pythonpath, numGPUs);

        NIOMessageHandler mh = new NIOMessageHandler(nw);

        // Initialize the Transfer Manager
        wLogger.debug("  Initializing the TransferManager structures...");
        try {
            tm.init(NIOEventManagerClass, null, mh);
        } catch (CommException ce) {
            wLogger.error("Error initializing Transfer Manager on worker " + nw.getHostName(), ce);
            // Shutdown the Worker since the error it is not recoverable
            nw.shutdown(null);
            return;
        }

        // Start the Transfer Manager thread (starts the EventManager)
        wLogger.debug("  Starting TransferManager Thread");
        tm.start();
        try {
            tm.startServer(new NIONode(null, wPort));
        } catch (CommException ce) {
            wLogger.error("Error starting TransferManager Server at Worker" + nw.getHostName(), ce);
            nw.shutdown(null);
            return;
        }

        if (NIOTracer.isActivated()) {
            NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.Event.START.getType());
        }

        /*
         * ************************************** 
         * JOIN AND END
         **************************************/
        // Wait for the Transfer Manager thread to finish (the shutdown is received on that thread)
        try {
            tm.join();
        } catch (InterruptedException ie) {
            wLogger.warn("TransferManager interrupted", ie);
            Thread.currentThread().interrupt();
        }
    }

}
