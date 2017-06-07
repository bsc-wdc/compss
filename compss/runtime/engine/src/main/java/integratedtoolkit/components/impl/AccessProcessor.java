package integratedtoolkit.components.impl;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.monitor.impl.GraphGenerator;
import integratedtoolkit.exceptions.CannotLoadException;
import integratedtoolkit.components.impl.TaskProducer;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.data.AccessParams;
import integratedtoolkit.types.data.AccessParams.AccessMode;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataAccessId.RAccessId;
import integratedtoolkit.types.data.DataAccessId.RWAccessId;
import integratedtoolkit.types.data.DataAccessId.WAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.ResultFile;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.request.ap.TransferRawFileRequest;
import integratedtoolkit.types.request.ap.AlreadyAccessedRequest;
import integratedtoolkit.types.request.ap.GetResultFilesRequest;
import integratedtoolkit.types.request.ap.DeleteFileRequest;
import integratedtoolkit.types.request.ap.EndOfAppRequest;
import integratedtoolkit.types.request.ap.GetLastRenamingRequest;
import integratedtoolkit.types.request.ap.TaskEndNotification;
import integratedtoolkit.types.request.ap.IsObjectHereRequest;
import integratedtoolkit.types.request.ap.NewVersionSameValueRequest;
import integratedtoolkit.types.request.ap.RegisterDataAccessRequest;
import integratedtoolkit.types.request.ap.SetObjectVersionValueRequest;
import integratedtoolkit.types.request.ap.ShutdownRequest;
import integratedtoolkit.types.request.ap.APRequest;
import integratedtoolkit.types.request.ap.TaskAnalysisRequest;
import integratedtoolkit.types.request.ap.TasksStateRequest;
import integratedtoolkit.types.request.ap.TransferObjectRequest;
import integratedtoolkit.types.request.ap.TransferOpenFileRequest;
import integratedtoolkit.types.request.ap.UnblockResultFilesRequest;
import integratedtoolkit.types.request.ap.BarrierRequest;
import integratedtoolkit.types.request.ap.WaitForTaskRequest;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Tracer;


/**
 * Component to handle the tasks accesses to files and object
 * 
 */
public class AccessProcessor implements Runnable, TaskProducer {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_OBJECT_LOAD_FROM_STORAGE = "ERROR: Cannot load object from storage (file or PSCO)";
    private static final String ERROR_QUEUE_OFFER = "ERROR: AccessProcessor queue offer error on ";

    // Other super-components
    protected TaskDispatcher<?, ?, ?> taskDispatcher;

    // Subcomponents
    protected TaskAnalyser taskAnalyser;
    protected DataInfoProvider dataInfoProvider;

    // Processor thread
    private static Thread processor;
    private static boolean keepGoing;

    // Tasks to be processed
    protected LinkedBlockingQueue<APRequest> requestQueue;


    /**
     * Creates a new Access Processor instance
     * 
     * @param td
     */
    public AccessProcessor(TaskDispatcher<?, ?, ?> td) {
        taskDispatcher = td;

        // Start Subcomponents
        taskAnalyser = new TaskAnalyser();
        dataInfoProvider = new DataInfoProvider();

        taskAnalyser.setCoWorkers(dataInfoProvider);
        requestQueue = new LinkedBlockingQueue<>();

        keepGoing = true;
        processor = new Thread(this);
        processor.setName("Access Processor");
        if (Tracer.basicModeEnabled()) {
            Tracer.enablePThreads();
        }
        processor.start();
        if (Tracer.basicModeEnabled()) {
            Tracer.disablePThreads();
        }
    }

    /**
     * Sets the GraphGenerator co-worker
     * 
     * @param gm
     */
    public void setGM(GraphGenerator gm) {
        this.taskAnalyser.setGM(gm);
    }

    @Override
    public void run() {
        while (keepGoing) {
            APRequest request = null;
            try {
                request = requestQueue.take();

                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.getAPRequestEvent(request.getRequestType().name()).getId(), Tracer.getRuntimeEventsType());
                }
                request.process(this, taskAnalyser, dataInfoProvider, taskDispatcher);
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }

            } catch (ShutdownException se) {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                LOGGER.error("Exception", e);
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
            }
        }

        LOGGER.info("AccessProcessor shutdown");
    }

    /**
     * App : new Method Task
     * 
     * @param appId
     * @param methodClass
     * @param methodName
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param parameters
     * @return
     */
    public int newTask(Long appId, String signature, boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed,
            boolean hasTarget, boolean hasReturn, Parameter[] parameters) {

        Task currentTask = new Task(appId, signature, isPrioritary, numNodes, isReplicated, isDistributed, hasTarget, hasReturn,
                parameters);

        if (!requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new method task");
        }
        return currentTask.getId();
    }

    /**
     * App : new Service task
     * 
     * @param appId
     * @param namespace
     * @param service
     * @param port
     * @param operation
     * @param priority
     * @param hasTarget
     * @param parameters
     * @return
     */
    public int newTask(Long appId, String namespace, String service, String port, String operation, boolean priority, boolean hasTarget,
            Parameter[] parameters) {

        Task currentTask = new Task(appId, namespace, service, port, operation, priority, hasTarget, parameters);

        if (!requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new service task");
        }
        return currentTask.getId();
    }

    // Notification thread (JM)
    @Override
    public void notifyTaskEnd(Task task) {
        if (!requestQueue.offer(new TaskEndNotification(task))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "notify task end");
        }
    }

    /**
     * Notifies a main access to a given file @sourceLocation in mode @fap
     * 
     * @param sourceLocation
     * @param fap
     * @param destDir
     * @return
     */
    public DataLocation mainAccessToFile(DataLocation sourceLocation, AccessParams.FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("File not accessed before, returning the same location");
            return sourceLocation;
        }

        // Tell the DM that the application wants to access a file.
        DataAccessId faId = registerDataAccess(fap);
        DataLocation tgtLocation = sourceLocation;

        if (fap.getMode() != AccessMode.W) {
            // Wait until the last writer task for the file has finished
            LOGGER.debug("File " + faId.getDataId() + " mode contains R, waiting until the last writer has finished");
            waitForTask(faId.getDataId(), AccessMode.R);
            if (destDir == null) {
                tgtLocation = transferFileOpen(faId);
            } else {
                DataInstanceId daId;
                if (fap.getMode() == AccessMode.R) {
                    RAccessId ra = (RAccessId) faId;
                    daId = ra.getReadDataInstance();
                } else {
                    RWAccessId ra = (RWAccessId) faId;
                    daId = ra.getReadDataInstance();
                }

                String rename = daId.getRenaming();
                String path = DataLocation.Protocol.FILE_URI.getSchema() + destDir + rename;
                try {
                    SimpleURI uri = new SimpleURI(path);
                    tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
                }

                transferFileRaw(faId, tgtLocation);
            }
        }

        if (fap.getMode() != AccessMode.R) {
            // Mode contains W
            LOGGER.debug("File " + faId.getDataId() + " mode contains W, register new writer");
            DataInstanceId daId;
            if (fap.getMode() == AccessMode.RW) {
                RWAccessId ra = (RWAccessId) faId;
                daId = ra.getWrittenDataInstance();
            } else {
                WAccessId ra = (WAccessId) faId;
                daId = ra.getWrittenDataInstance();
            }
            String rename = daId.getRenaming();
            String path = DataLocation.Protocol.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + rename;
            try {
                SimpleURI uri = new SimpleURI(path);
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            Comm.registerLocation(rename, tgtLocation);
        }

        if (DEBUG) {
            LOGGER.debug("File " + faId.getDataId() + " located on " + tgtLocation.toString());
        }
        return tgtLocation;
    }

    /**
     * Returns if the value with hashCode @hashCode is valid or obsolete
     * 
     * @param hashCode
     * @return
     */
    public boolean isCurrentRegisterValueValid(int hashCode) {
        LOGGER.debug("Checking if value of object with hashcode " + hashCode + " is valid");

        Semaphore sem = new Semaphore(0);
        IsObjectHereRequest request = new IsObjectHereRequest(hashCode, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "valid object value");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        // Log response and return
        boolean isValid = request.getResponse();
        if (DEBUG) {
            if (isValid) {
                LOGGER.debug("Value of object with hashcode " + hashCode + " is valid");
            } else {
                LOGGER.debug("Value of object with hashcode " + hashCode + " is NOT valid");
            }
        }

        return isValid;
    }

    /**
     * Notifies a main access to an object @obj
     * 
     * @param obj
     * @param hashCode
     * @return
     */
    public Object mainAcessToObject(Object obj, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        AccessParams.ObjectAccessParams oap = new AccessParams.ObjectAccessParams(AccessMode.RW, obj, hashCode);
        DataAccessId oaId = registerDataAccess(oap);
        DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId() + " with renaming " + wRename);
        }
        waitForTask(oaId.getDataId(), AccessMode.RW);

        // TODO: Check if the object was already piggybacked in the task notification

        // Ask for the object
        if (DEBUG) {
            LOGGER.debug("Request object transfer " + oaId.getDataId() + " with renaming " + wRename);
        }
        Object oUpdated = obtainObject(oaId);

        if (DEBUG) {
            LOGGER.debug("Object retrieved. Set new version to: " + wRename);
        }
        setObjectVersionValue(wRename, oUpdated);
        return oUpdated;
    }

    /**
     * Notifies a main access to an external object {@code id}
     * 
     * @param fileName
     * @param id
     * @param hashCode
     * @return
     */
    public String mainAcessToExternalObject(String id, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to external object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        AccessParams.ObjectAccessParams oap = new AccessParams.ObjectAccessParams(AccessMode.RW, id, hashCode);
        DataAccessId oaId = registerDataAccess(oap);
        DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        if (DEBUG) {
            LOGGER.debug("Waiting for last writer of " + oaId.getDataId() + " with renaming " + wRename);
        }
        waitForTask(oaId.getDataId(), AccessMode.RW);

        // TODO: Check if the object was already piggybacked in the task notification
        
        String lastRenaming = ((DataAccessId.RWAccessId) oaId).getReadDataInstance().getRenaming();
        String newId = Comm.getData(lastRenaming).getId();

        return Protocol.PERSISTENT_URI.getSchema() + newId;
    }

    /**
     * Notification for no more tasks
     * 
     * @param appId
     */
    public void noMoreTasks(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new EndOfAppRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "no more tasks");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("All tasks finished");
    }

    /**
     * Returns whether the @loc has already been accessed or not
     * 
     * @param loc
     * @return
     */
    private boolean alreadyAccessed(DataLocation loc) {
        Semaphore sem = new Semaphore(0);
        AlreadyAccessedRequest request = new AlreadyAccessedRequest(loc, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "already accessed location");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Barrier
     * 
     * @param appId
     */
    public void barrier(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new BarrierRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for all tasks");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("Barrier: End of waited all tasks");
    }

    /**
     * Synchronism for an specific task
     * 
     * @param dataId
     * @param mode
     */
    private void waitForTask(int dataId, AccessMode mode) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new WaitForTaskRequest(dataId, mode, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.info("End of waited task for data " + dataId);
    }

    /**
     * Registers a new data access
     * 
     * @param access
     * @return
     */
    private DataAccessId registerDataAccess(AccessParams access) {
        Semaphore sem = new Semaphore(0);
        RegisterDataAccessRequest request = new RegisterDataAccessRequest(access, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data access");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Registers a new version of file/object with the same value
     * 
     * @param rRenaming
     * @param wRenaming
     */
    public void newVersionSameValue(String rRenaming, String wRenaming) {
        NewVersionSameValueRequest request = new NewVersionSameValueRequest(rRenaming, wRenaming);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new version");
        }
    }

    /**
     * Sets a new value to a specific version of a file/object
     * 
     * @param renaming
     * @param value
     */
    public void setObjectVersionValue(String renaming, Object value) {
        SetObjectVersionValueRequest request = new SetObjectVersionValueRequest(renaming, value);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new object version value");
        }
    }

    /**
     * Returns the last version of a file/object with code @code
     * 
     * @param code
     * @return
     */
    public String getLastRenaming(int code) {
        Semaphore sem = new Semaphore(0);
        GetLastRenamingRequest request = new GetLastRenamingRequest(code, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get last renaming");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Unblock result files
     * 
     * @param resFiles
     */
    public void unblockResultFiles(List<ResultFile> resFiles) {
        UnblockResultFilesRequest request = new UnblockResultFilesRequest(resFiles);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unblock result files");
        }
    }

    /**
     * Shutdown request
     * 
     */
    public void shutdown() {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new ShutdownRequest(sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "shutdown");
        }

        // Wait for response
        sem.acquireUninterruptibly();
    }

    /**
     * Returns a string with the description of the tasks in the graph
     *
     * @return description of the current tasks in the graph
     */
    public String getCurrentTaskState() {
        Semaphore sem = new Semaphore(0);
        TasksStateRequest request = new TasksStateRequest(sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get current task state");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return (String) request.getResponse();
    }

    /**
     * Marks a location for deletion
     * 
     * @param loc
     */
    public void markForDeletion(DataLocation loc) {
        if (!requestQueue.offer(new DeleteFileRequest(loc))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }
    }

    /**
     * Adds a request for file raw transfer
     * 
     * @param faId
     * @param location
     */
    private void transferFileRaw(DataAccessId faId, DataLocation location) {
        Semaphore sem = new Semaphore(0);
        TransferRawFileRequest request = new TransferRawFileRequest((RAccessId) faId, location, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file raw");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Raw file transferred");
    }

    /**
     * Adds a request for open file transfer
     * 
     * @param faId
     * @return
     */
    private DataLocation transferFileOpen(DataAccessId faId) {
        Semaphore sem = new Semaphore(0);
        TransferOpenFileRequest request = new TransferOpenFileRequest(faId, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file open");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Open file transferred");
        return request.getLocation();
    }

    /**
     * Adds a request to obtain an object from a worker to the master
     * 
     * @param oaId
     * @return
     */
    private Object obtainObject(DataAccessId oaId) {
        LOGGER.debug("Obtain object with id " + oaId);
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferObjectRequest tor = new TransferObjectRequest(oaId, sem);
        if (!requestQueue.offer(tor)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "obtain object");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        // Get response
        Object oUpdated = tor.getResponse();
        if (oUpdated == null) {
            /*
             * The Object didn't come from a WS but was transferred from a worker, we load it from its storage (file or
             * persistent)
             */
            LogicalData ld = tor.getLogicalDataTarget();
            try {
                ld.loadFromStorage();
                oUpdated = ld.getValue();
            } catch (CannotLoadException e) {
                LOGGER.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
                ErrorManager.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
            }
        }

        return oUpdated;
    }

    /**
     * Adds a request to retrieve the result files from the workers to the master
     * 
     * @param appId
     */
    public void getResultFiles(Long appId) {
        Semaphore sem = new Semaphore(0);
        GetResultFilesRequest request = new GetResultFilesRequest(appId, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get result files");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        UnblockResultFilesRequest urfr = new UnblockResultFilesRequest(request.getBlockedData());
        if (!requestQueue.offer(urfr)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unlock result files");
        }
    }

}
