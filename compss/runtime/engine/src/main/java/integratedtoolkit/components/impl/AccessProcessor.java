package integratedtoolkit.components.impl;

import integratedtoolkit.comm.Comm;
import integratedtoolkit.components.monitor.impl.GraphGenerator;
import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

import integratedtoolkit.ITConstants;
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
import integratedtoolkit.util.Serializer;
import integratedtoolkit.util.Tracer;


public class AccessProcessor implements Runnable, TaskProducer {

    protected static final String ERROR_OBJECT_DESERIALIZE = "ERROR: Cannot deserialize object from file";

    // Other supercomponent
    protected TaskDispatcher taskDispatcher;    
    // Subcomponents
    protected TaskAnalyser taskAnalyser;
    protected DataInfoProvider dataInfoProvider;
    // Processor thread
    private static Thread processor;
    private static boolean keepGoing;
    // Tasks to be processed
    protected LinkedBlockingQueue<APRequest> requestQueue;
    // Component logger
    private static final Logger logger = Logger.getLogger(Loggers.TP_COMP);
    private static int CHANGES = 1;
    int changes = CHANGES;

        // Tracing
    protected static boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;
    

    public AccessProcessor(TaskDispatcher td) {
        taskDispatcher = td;
        
        //Start Subcomponents
        taskAnalyser = new TaskAnalyser();
        dataInfoProvider = new DataInfoProvider();
        taskAnalyser.setCoWorkers(dataInfoProvider);

        taskAnalyser.setCoWorkers(dataInfoProvider);
        requestQueue = new LinkedBlockingQueue<APRequest>();

        keepGoing = true;
        processor = new Thread(this);
        processor.setName("Access Processor");
        if (Tracer.basicModeEnabled()){
            Tracer.enablePThreads();
        }
        processor.start();
        if (Tracer.basicModeEnabled()){
            Tracer.disablePThreads();
        }
    }
    
    public void setGM(GraphGenerator gm) {
    	this.taskAnalyser.setGM(gm);
    }

    public void run() {
        while (keepGoing) {
            APRequest request = null;
            try {
                request = requestQueue.take();
                if (tracing){
                    Tracer.emitEvent(Tracer.getAPRequestEvent(request.getRequestType().name()).getId(), Tracer.getRuntimeEventsType());
                }
                request.process(this, taskAnalyser, dataInfoProvider, taskDispatcher);
                if (tracing){
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
            } catch (InterruptedException e) {
            	logger.error("Exception", e);
                if (tracing){
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                continue;
            } catch (ShutdownException se) {
                if (tracing){
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                break;
            } catch (Exception e) {
                if (tracing){
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
                }
                logger.error("Exception", e);
            }
        }
        logger.info("AccessProcessor shutdown");
    }

    // App
    public int newTask(Long appId, String methodClass, String methodName, boolean priority, boolean hasTarget, Parameter[] parameters) {
        Task currentTask = new Task(appId, methodClass, methodName, priority, hasTarget, parameters);

        requestQueue.offer(new TaskAnalysisRequest(currentTask));
        return currentTask.getId();
    }

    // App
    public int newTask(Long appId, String namespace, String service, String port, String operation, boolean priority,
            boolean hasTarget, Parameter[] parameters) {

        Task currentTask = new Task(appId, namespace, service, port, operation, priority, hasTarget, parameters);

        requestQueue.offer(new TaskAnalysisRequest(currentTask));
        return currentTask.getId();
    }

    // Notification thread (JM)
    @Override
    public void notifyTaskEnd(Task task) {
        requestQueue.offer(new TaskEndNotification(task));
    }

    public DataLocation mainAccessToFile(DataLocation sourceLocation, AccessParams.FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(sourceLocation);

        if (!alreadyAccessed) {
            logger.debug("File not accessed before returning the same location");
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
                tgtLocation = DataLocation.getLocation(Comm.appHost, destDir + rename);

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
            Comm.registerLocation(rename, tgtLocation);
        }

        logger.debug("File " + faId.getDataId() + " located on " + tgtLocation.toString());
        return tgtLocation;
    }

    public boolean isCurrentRegisterValueValid(int hashCode) {
        Semaphore sem = new Semaphore(0);
        IsObjectHereRequest request = new IsObjectHereRequest(hashCode, sem);
        requestQueue.offer(request);
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

        DataInstanceId rdId = ((DataAccessId.RWAccessId) oaId).getReadDataInstance();
        String rRename = rdId.getRenaming();
        DataInstanceId wId = ((DataAccessId.RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Wait until the last writer task for the object has finished
        waitForTask(oaId.getDataId(), AccessMode.RW);
        logger.debug("Task creator of object with hash code " + hashCode + " is finished");

        // TODO: Check if the object was already piggybacked in the task notification
        // Ask for the object
        Object oUpdated = obtainObject(oaId);

        if (oUpdated == null) {
            /* The Object didn't come from a WS but was transferred from a worker
             * Deserialize the object from the file
             */
            try {
                oUpdated = Serializer.deserialize(destDir + rRename);
            } catch (Exception e) {
                logger.fatal(ERROR_OBJECT_DESERIALIZE + ": " + destDir + rRename, e);
                System.exit(1);
            }
        }
        setObjectVersionValue(wRename, oUpdated);
        return oUpdated;
    }

    // App
    public void noMoreTasks(Long appId) {
        Semaphore sem = new Semaphore(0);
        requestQueue.offer(new EndOfAppRequest(appId, sem));
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
        requestQueue.offer(request);
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
        requestQueue.offer(new WaitForAllTasksRequest(appId, sem));
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
        requestQueue.offer(new WaitForTaskRequest(dataId, mode, sem));
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
        requestQueue.offer(request);
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
        requestQueue.offer(request);
    }

    // App
    public void setObjectVersionValue(String renaming, Object value) {
        SetObjectVersionValueRequest request = new SetObjectVersionValueRequest(renaming, value);
        requestQueue.offer(request);
    }

    // App
    public String getLastRenaming(int code) {
        Semaphore sem = new Semaphore(0);
        GetLastRenamingRequest request = new GetLastRenamingRequest(code, sem);
        requestQueue.offer(request);

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
        requestQueue.offer(request);
    }

    // App / Shutdown thread
    public void shutdown() {
        Semaphore sem = new Semaphore(0);
        requestQueue.offer(new ShutdownRequest(sem));
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
        requestQueue.offer(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        return (String) request.getResponse();
    }

    public void markForDeletion(DataLocation loc) {
        requestQueue.offer(new DeleteFileRequest(loc));
    }

    // App
    private void transferFileRaw(DataAccessId faId, DataLocation location) {
        Semaphore sem = new Semaphore(0);
        TransferRawFileRequest request = new TransferRawFileRequest((RAccessId) faId, location, sem);
        requestQueue.offer(request);

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
        requestQueue.offer(request);

        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }

        logger.debug("Open file transferred");
        return request.getLocation();
    }

    private Object obtainObject(DataAccessId oaId) {
        Semaphore sem = new Semaphore(0);
        TransferObjectRequest tor = new TransferObjectRequest(oaId, sem);
        requestQueue.offer(tor);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        return tor.getResponse();
    }

    public void getResultFiles(Long appId) {
        Semaphore sem = new Semaphore(0);
        GetResultFilesRequest request = new GetResultFilesRequest(appId, sem);
        requestQueue.offer(request);
        try {
            sem.acquire();
        } catch (InterruptedException e) {
            // Nothing to do
        }
        UnblockResultFilesRequest urfr = new UnblockResultFilesRequest(request.getBlockedData());
        requestQueue.offer(urfr);
    }

}
