package integratedtoolkit.components.impl;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.monitor.impl.GraphGenerator;
import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;

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
import integratedtoolkit.types.request.ap.WaitForAllTasksRequest;
import integratedtoolkit.types.request.ap.WaitForTaskRequest;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Tracer;


public class AccessProcessor implements Runnable, TaskProducer {

    protected static final String ERROR_OBJECT_LOAD_FROM_STORAGE = "ERROR: Cannot load object from storage (file or PSCO)";
    private static final String ERROR_QUEUE_OFFER = "ERROR: AccessProcessor queue offer error on ";

    // Other super-components
    protected TaskDispatcher<?, ?> taskDispatcher;
    // Subcomponents
    protected TaskAnalyser taskAnalyser;
    protected DataInfoProvider dataInfoProvider;
    // Processor thread
    private static Thread processor;
    private static boolean keepGoing;
    // Tasks to be processed
    protected LinkedBlockingQueue<APRequest> requestQueue;
    // Component logger
    private static final Logger logger = LogManager.getLogger(Loggers.TP_COMP);
    private static int CHANGES = 1;
    int changes = CHANGES;


    public AccessProcessor(TaskDispatcher<?, ?> td) {
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
                logger.error("Exception", e);
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
            }
        }

        logger.info("AccessProcessor shutdown");
    }

    // App : new Method Task
    public int newTask(Long appId, String methodClass, String methodName, boolean mustBeReplicated, boolean priority, boolean hasTarget, 
            Parameter[] parameters) {
        
        Task currentTask = new Task(appId, methodClass, methodName, mustBeReplicated, priority, hasTarget, parameters);

        if (!requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new method task");
        }
        return currentTask.getId();
    }

    // App : new Service task
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

    public DataLocation mainAccessToFile(DataLocation sourceLocation, AccessParams.FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            logger.debug("File not accessed before, returning the same location");
            return sourceLocation;
        }

        // Tell the DM that the application wants to access a file.
        DataAccessId faId = registerDataAccess(fap);
        DataLocation tgtLocation = sourceLocation;

        if (fap.getMode() != AccessMode.W) {
            // Wait until the last writer task for the file has finished
            logger.debug("File " + faId.getDataId() + " mode contains R, waiting until the last writer has finished");
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
            logger.debug("File " + faId.getDataId() + " mode contains W, register new writer");
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

        logger.debug("File " + faId.getDataId() + " located on " + tgtLocation.toString());
        return tgtLocation;
    }

    public boolean isCurrentRegisterValueValid(int hashCode) {
        Semaphore sem = new Semaphore(0);
        IsObjectHereRequest request = new IsObjectHereRequest(hashCode, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "valid object value");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }

        return request.getResponse();
    }

    public Object mainAcessToObject(Object o, int hashCode, String destDir) {
        // Tell the DIP that the application wants to access an object
        AccessParams.ObjectAccessParams oap = new AccessParams.ObjectAccessParams(AccessMode.RW, o, hashCode);
        DataAccessId oaId = registerDataAccess(oap);
        DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        waitForTask(oaId.getDataId(), AccessMode.RW);
        logger.debug("Task creator of object with hash code " + hashCode + " is finished");

        // TODO: Check if the object was already piggybacked in the task notification
        // Ask for the object
        Object oUpdated = obtainObject(oaId);

        setObjectVersionValue(wRename, oUpdated);
        return oUpdated;
    }

    // App
    public void noMoreTasks(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new EndOfAppRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "no more tasks");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        logger.info("All tasks finished");
    }

    // App
    private boolean alreadyAccessed(DataLocation loc) {
        Semaphore sem = new Semaphore(0);
        AlreadyAccessedRequest request = new AlreadyAccessedRequest(loc, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "already accessed location");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        return request.getResponse();
    }

    // App
    public void waitForAllTasks(Long appId) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new WaitForAllTasksRequest(appId, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for all tasks");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        logger.info("Barrier: End of waited all tasks");
    }

    // App
    private void waitForTask(int dataId, AccessMode mode) {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new WaitForTaskRequest(dataId, mode, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        logger.info("End of waited task for data " + dataId);
    }

    // App
    private DataAccessId registerDataAccess(AccessParams access) {
        Semaphore sem = new Semaphore(0);
        RegisterDataAccessRequest request = new RegisterDataAccessRequest(access, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data access");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        return request.getResponse();
    }

    // App
    public void newVersionSameValue(String rRenaming, String wRenaming) {
        NewVersionSameValueRequest request = new NewVersionSameValueRequest(rRenaming, wRenaming);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new version");
        }
    }

    // App
    public void setObjectVersionValue(String renaming, Object value) {
        SetObjectVersionValueRequest request = new SetObjectVersionValueRequest(renaming, value);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new object version value");
        }
    }

    // App
    public String getLastRenaming(int code) {
        Semaphore sem = new Semaphore(0);
        GetLastRenamingRequest request = new GetLastRenamingRequest(code, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get last renaming");
        }

        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        return request.getResponse();
    }

    // App
    public void unblockResultFiles(List<ResultFile> resFiles) {
        UnblockResultFilesRequest request = new UnblockResultFilesRequest(resFiles);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unblock result files");
        }
    }

    // App / Shutdown thread
    public void shutdown() {
        Semaphore sem = new Semaphore(0);
        if (!requestQueue.offer(new ShutdownRequest(sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "shutdown");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
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
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        return (String) request.getResponse();
    }

    public void markForDeletion(DataLocation loc) {
        if (!requestQueue.offer(new DeleteFileRequest(loc))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }
    }

    // App
    private void transferFileRaw(DataAccessId faId, DataLocation location) {
        Semaphore sem = new Semaphore(0);
        TransferRawFileRequest request = new TransferRawFileRequest((RAccessId) faId, location, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file raw");
        }

        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }

        logger.debug("Raw file transferred");
    }

    // App
    private DataLocation transferFileOpen(DataAccessId faId) {
        Semaphore sem = new Semaphore(0);
        TransferOpenFileRequest request = new TransferOpenFileRequest(faId, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file open");
        }

        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }

        logger.debug("Open file transferred");
        return request.getLocation();
    }

    private Object obtainObject(DataAccessId oaId) {
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferObjectRequest tor = new TransferObjectRequest(oaId, sem);
        if (!requestQueue.offer(tor)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "obtain object");
        }

        // Wait for completion
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }

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
            } catch (Exception e) {
                logger.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
                ErrorManager.fatal(ERROR_OBJECT_LOAD_FROM_STORAGE + ": " + ((ld == null) ? "null" : ld.getName()), e);
            }
        }

        return oUpdated;
    }

    public void getResultFiles(Long appId) {
        Semaphore sem = new Semaphore(0);
        GetResultFilesRequest request = new GetResultFilesRequest(appId, sem);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get result files");
        }
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        UnblockResultFilesRequest urfr = new UnblockResultFilesRequest(request.getBlockedData());
        if (!requestQueue.offer(urfr)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unlock result files");
        }
    }

}
