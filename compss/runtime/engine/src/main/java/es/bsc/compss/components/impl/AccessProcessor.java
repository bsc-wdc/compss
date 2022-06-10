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
package es.bsc.compss.components.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.checkpoint.CheckpointManager;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.exceptions.CannotLoadException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.ReduceTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.accessparams.BindingObjectAccessParams;
import es.bsc.compss.types.data.accessparams.FileAccessParams;
import es.bsc.compss.types.data.accessparams.ObjectAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.request.ap.APRequest;
import es.bsc.compss.types.request.ap.AlreadyAccessedRequest;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.CancelApplicationTasksRequest;
import es.bsc.compss.types.request.ap.CancelTaskGroupRequest;
import es.bsc.compss.types.request.ap.CloseTaskGroupRequest;
import es.bsc.compss.types.request.ap.DeleteAllApplicationDataRequest;
import es.bsc.compss.types.request.ap.DeleteBindingObjectRequest;
import es.bsc.compss.types.request.ap.DeleteFileRequest;
import es.bsc.compss.types.request.ap.DeregisterObject;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.FinishDataAccessRequest;
import es.bsc.compss.types.request.ap.GetLastRenamingRequest;
import es.bsc.compss.types.request.ap.GetResultFilesRequest;
import es.bsc.compss.types.request.ap.IsObjectHereRequest;
import es.bsc.compss.types.request.ap.OpenTaskGroupRequest;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.ap.RegisterRemoteCollectionDataRequest;
import es.bsc.compss.types.request.ap.RegisterRemoteFileDataRequest;
import es.bsc.compss.types.request.ap.RegisterRemoteObjectDataRequest;
import es.bsc.compss.types.request.ap.SetObjectVersionValueRequest;
import es.bsc.compss.types.request.ap.ShutdownNotificationRequest;
import es.bsc.compss.types.request.ap.ShutdownRequest;
import es.bsc.compss.types.request.ap.SnapshotRequest;
import es.bsc.compss.types.request.ap.TaskAnalysisRequest;
import es.bsc.compss.types.request.ap.TaskEndNotification;
import es.bsc.compss.types.request.ap.TasksStateRequest;
import es.bsc.compss.types.request.ap.TransferBindingObjectRequest;
import es.bsc.compss.types.request.ap.TransferObjectRequest;
import es.bsc.compss.types.request.ap.TransferOpenDirectoryRequest;
import es.bsc.compss.types.request.ap.TransferOpenFileRequest;
import es.bsc.compss.types.request.ap.TransferRawFileRequest;
import es.bsc.compss.types.request.ap.UnblockResultFilesRequest;
import es.bsc.compss.types.request.ap.WaitForDataReadyToDeleteRequest;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.Classpath;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;
import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;

import java.util.List;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Component to handle the tasks accesses to files and object.
 */
public class AccessProcessor implements Runnable, CheckpointManager.User {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String CHECKPOINTER_REL_PATH = File.separator + "Runtime" + File.separator + "checkpointer";

    private static final String ERR_LOAD_CHECKPOINTER = "Error loading checkpoint manager";
    private static final String ERROR_OBJECT_LOAD_FROM_STORAGE =
        "ERROR: Cannot load object from storage (file or PSCO)";
    private static final String ERROR_QUEUE_OFFER = "ERROR: AccessProcessor queue offer error on ";

    // Other super-components
    private final TaskDispatcher taskDispatcher;

    // Subcomponents
    private final TaskAnalyser taskAnalyser;
    private final DataInfoProvider dataInfoProvider;
    private final CheckpointManager checkpointManager;

    // Processor thread
    private static Thread processor;
    private static boolean keepGoing;
    private Semaphore shutdownSemaphore;

    // Tasks to be processed
    protected LinkedBlockingQueue<APRequest> requestQueue;


    /**
     * Creates a new Access Processor instance.
     *
     * @param td Associated TaskDispatcher component.
     */
    public AccessProcessor(TaskDispatcher td) {
        this.taskDispatcher = td;

        // Start Subcomponents
        this.taskAnalyser = new TaskAnalyser();
        this.dataInfoProvider = new DataInfoProvider();

        loadCheckpointingPoliciesJars();
        this.checkpointManager = constructCheckpointManager();
        if (this.checkpointManager == null) {
            ErrorManager.fatal(ERR_LOAD_CHECKPOINTER);
        }

        this.taskAnalyser.setCoWorkers(dataInfoProvider, checkpointManager);
        this.requestQueue = new LinkedBlockingQueue<>();

        keepGoing = true;
        processor = new Thread(this);
        processor.setName("Access Processor");
        if (Tracer.isActivated()) {
            Tracer.enablePThreads(1);
        }
        processor.start();
    }

    /**
     * Sets the GraphGenerator co-worker.
     *
     * @param gm co-worker.
     */
    public void setGM(GraphGenerator gm) {
        this.taskAnalyser.setGM(gm);
    }

    @Override
    public void run() {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.AP_THREAD_ID);
            Tracer.disablePThreads(1);
        }
        while (keepGoing) {
            APRequest request = null;
            try {
                request = this.requestQueue.take();
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(request.getEvent());
                }
                request.process(this, this.taskAnalyser, this.dataInfoProvider, this.taskDispatcher);
            } catch (ShutdownException se) {
                se.getSemaphore().release();
                break;
            } catch (Exception e) {
                ErrorManager.error("Exception", e);
            } finally {
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TraceEventType.RUNTIME);
                }
            }

        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.AP_THREAD_ID);
        }

        LOGGER.info("AccessProcessor shutdown");
    }

    /**
     * Application: new Method Task.
     *
     * @param app Application.
     * @param monitor Task monitor.
     * @param lang Application language.
     * @param signature Task signature.
     * @param isPrioritary Whether the task has priority or not.
     * @param numNodes Number of nodes.
     * @param isReduce Whether the task is of type reduce.
     * @param reduceChunkSize The size of the chunks to be reduced.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed round-robin or not.
     * @param numReturns Number of task returns.
     * @param hasTarget Whether the task has a target object or not.
     * @param parameters Task parameters.
     * @param onFailure OnFailure mechanisms.
     * @param timeOut Time for a task timeOut.
     * @return Task Id.
     */
    public int newTask(Application app, TaskMonitor monitor, Lang lang, String signature, boolean isPrioritary,
        int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, int numReturns, List<Parameter> parameters, OnFailure onFailure, long timeOut) {

        Task currentTask;

        if (isReduce) {
            if (reduceChunkSize >= 2) {
                currentTask = new ReduceTask(app, lang, signature, isPrioritary, numNodes, isReduce, reduceChunkSize,
                    isReplicated, isDistributed, hasTarget, numReturns, parameters, monitor, onFailure, timeOut);
            } else {
                ErrorManager.warn("Requesting to create task with chunk_size smaller than 2. Executing as simple task");
                currentTask = new Task(app, lang, signature, isPrioritary, numNodes, isReduce, isReplicated,
                    isDistributed, hasTarget, numReturns, parameters, monitor, onFailure, timeOut);
            }
        } else {
            currentTask = new Task(app, lang, signature, isPrioritary, numNodes, isReduce, isReplicated, isDistributed,
                hasTarget, numReturns, parameters, monitor, onFailure, timeOut);
        }
        TaskMonitor registeredMonitor = currentTask.getTaskMonitor();
        registeredMonitor.onCreation();

        LOGGER.debug("Requesting analysis of Task " + currentTask.getId());
        if (!this.requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new method task");
        }

        return currentTask.getId();
    }

    /**
     * Application: new Service task.
     *
     * @param app Application.
     * @param monitor Task monitor.
     * @param namespace Service namespace.
     * @param service Service name.
     * @param port Service port.
     * @param operation Service operation.
     * @param priority Whether the task has priority or not.
     * @param hasTarget Whether the task has a target object or not.
     * @param numReturns Number of returns of the task.
     * @param parameters Task parameters.
     * @param onFailure OnFailure mechanisms.
     * @param timeOut Time for a task timeOut.
     * @return Task Id.
     */
    public int newTask(Application app, TaskMonitor monitor, String namespace, String service, String port,
        String operation, boolean priority, boolean isReduce, int reduceChunkSize, boolean hasTarget, int numReturns,
        List<Parameter> parameters, OnFailure onFailure, long timeOut) {

        Task currentTask = new Task(app, namespace, service, port, operation, priority, hasTarget, numReturns,
            parameters, monitor, onFailure, timeOut);

        TaskMonitor registeredMonitor = currentTask.getTaskMonitor();
        registeredMonitor.onCreation();

        LOGGER.debug("Requesting analysis of new service Task " + currentTask.getId());
        if (!this.requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new service task");
        }

        return currentTask.getId();
    }

    /**
     * Application: new HTTP task.
     *
     * @param app Application.
     * @param monitor Task monitor.
     * @param priority Whether the task has priority or not.
     * @param hasTarget Whether the task has a target object or not.
     * @param numReturns Number of returns of the task.
     * @param parameters Task parameters.
     * @param onFailure OnFailure mechanisms.
     * @param timeOut Time for a task timeOut.
     * @return Task Id.
     */
    public int newTask(Application app, TaskMonitor monitor, String declareMethodFullyQualifiedName, boolean priority,
        boolean isReduce, int reduceChunkSize, boolean hasTarget, int numReturns, List<Parameter> parameters,
        OnFailure onFailure, long timeOut) {

        Task currentTask = new Task(app, declareMethodFullyQualifiedName, priority, hasTarget, numReturns, parameters,
            monitor, onFailure, timeOut);

        TaskMonitor registeredMonitor = currentTask.getTaskMonitor();
        registeredMonitor.onCreation();

        LOGGER.debug("Requesting analysis of new HTTP Task " + currentTask.getId());

        if (!this.requestQueue.offer(new TaskAnalysisRequest(currentTask))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new HTTP task");
        }
        return currentTask.getId();
    }

    /**
     * Notifies the end of the given abstract task.
     *
     * @param task Ended task.
     */
    public void notifyTaskEnd(AbstractTask task) {
        if (!this.requestQueue.offer(new TaskEndNotification(task))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "notify task end");
        }
    }

    /**
     * Marks an access to a file as finished.
     *
     * @param sourceLocation File location.
     * @param fap File Access parameters.
     * @param destDir Destination file location.
     */
    public void finishAccessToFile(DataLocation sourceLocation, FileAccessParams fap, String destDir) {
        boolean alreadyAccessed = alreadyAccessed(fap.getApp(), sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("File not accessed before. Nothing to do");
            return;
        }

        // Tell the DM that the application wants to access a file.
        finishDataAccess(fap);

    }

    private void finishDataAccess(AccessParams fap) {
        if (!this.requestQueue.offer(new FinishDataAccessRequest(fap))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "finishing data access");
        }
    }

    /**
     * Notifies a main access to a given file {@code sourceLocation} in mode {@code fap}.
     *
     * @param app application accessing the file
     * @param sourceLocation File location.
     * @param fap File Access Parameters.
     * @param destDir Destination file.
     * @return Final location.
     */
    public DataLocation mainAccessToFile(Application app, DataLocation sourceLocation, FileAccessParams fap,
        String destDir) {
        boolean alreadyAccessed = alreadyAccessed(app, sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("File not accessed before, returning the same location");
            return sourceLocation;
        }

        // Tell the DM that the application wants to access a file.
        DataAccessId faId = registerDataAccess(fap, AccessMode.R);
        DataLocation tgtLocation = sourceLocation;

        if (fap.getMode() != AccessMode.W) {
            // Wait until the last writer task for the file has finished
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
                String path = ProtocolType.FILE_URI.getSchema() + destDir + rename;
                try {
                    SimpleURI uri = new SimpleURI(path);
                    tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
                }

                transferFileRaw(faId, tgtLocation);
            }
        }

        if (fap.getMode() != AccessMode.R && fap.getMode() != AccessMode.C) {
            // Mode contains W
            LOGGER.debug("File " + faId.getDataId() + " mode contains W, register new writer");
            DataInstanceId daId;
            if (fap.getMode() == AccessMode.RW || fap.getMode() == AccessMode.CV) {
                RWAccessId ra = (RWAccessId) faId;
                daId = ra.getWrittenDataInstance();
            } else {
                WAccessId ra = (WAccessId) faId;
                daId = ra.getWrittenDataInstance();
            }
            String rename = daId.getRenaming();
            String path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + rename;
            try {
                SimpleURI uri = new SimpleURI(path);
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            Comm.registerLocation(rename, tgtLocation);
        }

        if (DEBUG) {
            LOGGER.debug(
                "File " + faId.getDataId() + " located on " + (tgtLocation != null ? tgtLocation.toString() : "null"));
        }
        return tgtLocation;
    }

    /**
     * Notifies a main access to a given file {@code sourceLocation} in mode {@code fap}.
     *
     * @param app application accessing the directory
     * @param sourceLocation Directory location.
     * @param fap File Access Parameters.
     * @param destDir Destination directory.
     * @return Final location.
     */
    public DataLocation mainAccessToDirectory(Application app, DataLocation sourceLocation, FileAccessParams fap,
        String destDir) {
        boolean alreadyAccessed = alreadyAccessed(app, sourceLocation);

        if (!alreadyAccessed) {
            LOGGER.debug("Directory not accessed before, returning the same location");
            return sourceLocation;
        }

        // Tell the DM that the application wants to access a file.
        DataAccessId faId = registerDataAccess(fap, AccessMode.R);
        DataLocation tgtLocation = sourceLocation;

        if (fap.getMode() != AccessMode.W) {
            tgtLocation = transferDirectoryOpen(faId);
        }

        if (fap.getMode() != AccessMode.R && fap.getMode() != AccessMode.C) {
            // Mode contains W
            LOGGER.debug("File " + faId.getDataId() + " mode contains W, register new writer");
            DataInstanceId daId;
            if (fap.getMode() == AccessMode.RW || fap.getMode() == AccessMode.CV) {
                RWAccessId ra = (RWAccessId) faId;
                daId = ra.getWrittenDataInstance();
            } else {
                WAccessId ra = (WAccessId) faId;
                daId = ra.getWrittenDataInstance();
            }
            String rename = daId.getRenaming();
            String path = ProtocolType.DIR_URI.getSchema() + Comm.getAppHost().getTempDirPath() + rename;
            try {
                SimpleURI uri = new SimpleURI(path);
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
            }
            Comm.registerLocation(rename, tgtLocation);
        }

        if (DEBUG) {
            LOGGER.debug("Directory " + faId.getDataId() + " located on " + tgtLocation.toString());
        }
        return tgtLocation;
    }

    /**
     * Returns whether the value with hashCode {@code hashCode} is valid or obsolete.
     *
     * @param hashCode Object hashcode.
     * @return {@code true} if the object is valid, {@code false} otherwise.
     */
    public boolean isCurrentRegisterValueValid(int hashCode) {
        LOGGER.debug("Checking if value of object with hashcode " + hashCode + " is valid");

        Semaphore sem = new Semaphore(0);
        IsObjectHereRequest request = new IsObjectHereRequest(hashCode, sem);
        if (!this.requestQueue.offer(request)) {
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
     * Notifies a main access to an object {@code obj}.
     *
     * @param app application accessing the object.
     * @param obj Object.
     * @param hashCode Object hashcode.
     * @return Synchronized object.
     */
    public Object mainAccessToObject(Application app, Object obj, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        ObjectAccessParams oap = new ObjectAccessParams(app, AccessMode.RW, obj, hashCode);
        DataAccessId oaId = registerDataAccess(oap, AccessMode.RW);
        DataInstanceId wId = ((RWAccessId) oaId).getWrittenDataInstance();
        String wRename = wId.getRenaming();

        // Ask for the object
        if (DEBUG) {
            LOGGER.debug("Request object transfer " + oaId.getDataId() + " with renaming " + wRename);
        }
        Object oUpdated = obtainObject(oaId);

        if (DEBUG) {
            LOGGER.debug("Object retrieved. Set new version to: " + wRename);
        }

        setObjectVersionValue(wRename, oUpdated);
        finishDataAccess(oap);

        return oUpdated;
    }

    /**
     * Notifies a main access to an external PSCO {@code id}.
     *
     * @param app application accessing the external PSCO.
     * @param id PSCO Id.
     * @param hashCode Object hashcode.
     * @return Location containing final the PSCO Id.
     */
    public String mainAccessToExternalPSCO(Application app, String id, int hashCode) {
        if (DEBUG) {
            LOGGER.debug("Requesting main access to external object with hash code " + hashCode);
        }

        // Tell the DIP that the application wants to access an object
        ObjectAccessParams oap = new ObjectAccessParams(app, AccessMode.RW, id, hashCode);
        DataAccessId oaId = registerDataAccess(oap, AccessMode.RW);

        // TODO: Check if the object was already piggybacked in the task notification
        String lastRenaming = ((RWAccessId) oaId).getReadDataInstance().getRenaming();
        String newId = Comm.getData(lastRenaming).getPscoId();

        return ProtocolType.PERSISTENT_URI.getSchema() + newId;
    }

    private String obtainBindingObject(RAccessId oaId) {
        // TODO: Add transfer request similar than java object
        LOGGER.debug("[AccessProcessor] Obtaining binding object with id " + oaId);
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferBindingObjectRequest tor = new TransferBindingObjectRequest(oaId, sem);
        if (!this.requestQueue.offer(tor)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "obtain object");
        }

        // Wait for response
        sem.acquireUninterruptibly();
        BindingObject bo = BindingObject.generate(tor.getTargetName());
        return bo.getName();
    }

    /**
     * Notifies a main access to an external binding object.
     *
     * @param app application accessing the binding object.
     * @param bo Binding object.
     * @param hashCode Binding object's hashcode.
     * @return Location containing the binding's object final path.
     */
    public String mainAccessToBindingObject(Application app, BindingObject bo, int hashCode) {
        if (DEBUG) {
            LOGGER.debug(
                "Requesting main access to binding object with bo " + bo.toString() + " and hash code " + hashCode);
        }

        // Defaut access is read because the binding object is removed after accessing it
        // Tell the DIP that the application wants to access an object
        BindingObjectAccessParams oap = new BindingObjectAccessParams(app, AccessMode.R, bo, hashCode);
        DataAccessId oaId = registerDataAccess(oap, AccessMode.RW);

        String bindingObjectID = obtainBindingObject((RAccessId) oaId);

        finishDataAccess(oap);

        return bindingObjectID;
    }

    /**
     * Barrier for group.
     *
     * @param app Application .
     * @param groupName Name of the task group
     * @throws COMPSsException Exception thrown by user
     */
    public void barrierGroup(Application app, String groupName) throws COMPSsException {
        BarrierGroupRequest bgr = new BarrierGroupRequest(app, groupName);
        if (!requestQueue.offer(bgr)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for all tasks");
        }

        bgr.waitForCompletion();

        LOGGER.info("Group barrier: End of tasks of group " + groupName);
    }

    /**
     * Barrier.
     *
     * @param app Application .
     */
    public void barrier(Application app) {
        BarrierRequest br = new BarrierRequest(app);
        if (!this.requestQueue.offer(br)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for all tasks");
        }

        try {
            br.waitForCompletion();
        } catch (COMPSsException ce) {
            // This exception should be forwarded through the API
        }

        LOGGER.info("Barrier: End of waited all tasks");
    }

    /**
     * Notification for no more tasks.
     *
     * @param app Application.
     */
    public void noMoreTasks(Application app) {
        EndOfAppRequest eoar = new EndOfAppRequest(app);
        if (!this.requestQueue.offer(eoar)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "no more tasks");
        }

        try {
            eoar.waitForCompletion();
        } catch (COMPSsException ce) {
            // This exception should be forwarded through the API
        }

        LOGGER.info("All tasks finished");
    }

    /**
     * Returns whether the @{code loc} has already been accessed or not.
     *
     * @param app application querying the data access
     * @param loc Location.
     * @return {@code true} if the location has been accessed, {@code false} otherwise.
     */
    public boolean alreadyAccessed(Application app, DataLocation loc) {
        Semaphore sem = new Semaphore(0);
        AlreadyAccessedRequest request = new AlreadyAccessedRequest(app, loc, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "already accessed location");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Cancellation of all tasks of an application.
     *
     * @param app Application .
     */
    public void cancelApplicationTasks(Application app) {
        Long appId = app.getId();
        LOGGER.info("Cancelled all remaining tasks for application with id " + appId);

        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new CancelApplicationTasksRequest(app, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }
        // Wait for response
        LOGGER.debug("Waiting for finishing tasks cancellation " + appId);
        sem.acquireUninterruptibly();

        LOGGER.info("Tasks cancelled for application with id " + appId);
    }

    /**
     * Cancellation of the remaining tasks of a group.
     *
     * @param app Application.
     * @param groupName name of the group whose tasks will be cancelled.
     */
    public void cancelTaskGroup(Application app, String groupName) {
        Long appId = app.getId();
        LOGGER.info("Cancel remaining tasks for application " + appId + " and group " + groupName);

        Semaphore sem = new Semaphore(0);
        if (!this.requestQueue.offer(new CancelTaskGroupRequest(app, groupName, sem))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "wait for task");
        }
        // Wait for response
        LOGGER.debug("Waiting for cancellation of tasks in group " + groupName);
        sem.acquireUninterruptibly();

        LOGGER.info("Tasks cancelled for group " + groupName);
    }

    /**
     * Registers a new data access and waits for it to be available.
     *
     * @param access Access parameters.
     * @param taskMode Access mode to register the data access.
     * @return The registered access Id.
     */
    private DataAccessId registerDataAccess(AccessParams access, AccessMode taskMode) {
        RegisterDataAccessRequest request = new RegisterDataAccessRequest(access, taskMode);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data access");
        }

        // Wait for response
        request.waitForCompletion();
        DataAccessId daId = request.getAccessId();
        return daId;
    }

    /**
     * Sets a new value to a specific version of a file/object.
     *
     * @param renaming Renaming version.
     * @param value New value.
     */
    public void setObjectVersionValue(String renaming, Object value) {
        SetObjectVersionValueRequest request = new SetObjectVersionValueRequest(renaming, value);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new object version value");
        }
    }

    /**
     * Sets the task group to assign to all the following tasks.
     *
     * @param groupName Name of the task group
     * @param app Application.
     */
    public void setCurrentTaskGroup(String groupName, boolean implicitBarrier, Application app) {
        OpenTaskGroupRequest request = new OpenTaskGroupRequest(groupName, implicitBarrier, app);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "new task group");
        }
    }

    /**
     * Closes the current task group.
     *
     * @param app Application.
     */
    public void closeCurrentTaskGroup(Application app) {
        CloseTaskGroupRequest request = new CloseTaskGroupRequest(app);
        if (!requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "closure of task group");
        }
    }

    /**
     * Returns the last version of a file/object with code {@code code}.
     *
     * @param code File code.
     * @return Renaming of the last version.
     */
    public String getLastRenaming(int code) {
        Semaphore sem = new Semaphore(0);
        GetLastRenamingRequest request = new GetLastRenamingRequest(code, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get last renaming");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Unblock result files.
     *
     * @param resFiles List of result files to unblock.
     */
    public void unblockResultFiles(List<ResultFile> resFiles) {
        UnblockResultFilesRequest request = new UnblockResultFilesRequest(resFiles);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unblock result files");
        }
    }

    /**
     * Shutdown request.
     */
    public void shutdown() {
        shutdownSemaphore = new Semaphore(0);
        if (!this.requestQueue.offer(new ShutdownRequest(shutdownSemaphore))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "shutdown");
        }

        // Wait for response
        shutdownSemaphore.acquireUninterruptibly();

    }

    /**
     * Returns a string with the description of the tasks in the graph.
     *
     * @return The description of the current tasks in the graph.
     */
    public String getCurrentTaskState() {
        Semaphore sem = new Semaphore(0);
        TasksStateRequest request = new TasksStateRequest(sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get current task state");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Marks a location for deletion.
     *
     * @param app Application requesting the file deletion
     * @param loc Location to delete.
     * @param applicationDelete {@literal true}, if the application requested the data deletion; {@literal false}
     *            otherwise
     */
    public void markForDeletion(Application app, DataLocation loc, boolean enableReuse, boolean applicationDelete) {
        LOGGER.debug("Marking data " + loc + " for deletion");
        Semaphore sem = new Semaphore(0);

        // No need to wait if data is noReuse
        if (enableReuse) {
            Semaphore semWait = new Semaphore(0);
            WaitForDataReadyToDeleteRequest request = new WaitForDataReadyToDeleteRequest(app, loc, sem, semWait);
            // Wait for data to be ready for deletion
            if (!this.requestQueue.offer(request)) {
                ErrorManager.error(ERROR_QUEUE_OFFER + "wait for data ready to delete");
            }

            // Wait for response
            LOGGER.debug("Waiting for ready to delete request response...");
            sem.acquireUninterruptibly();

            int nPermits = request.getNumPermits();
            if (nPermits > 0) {
                LOGGER.debug("Waiting for " + nPermits + " tasks to finish...");
                semWait.acquireUninterruptibly(nPermits);
            }
        }
        // Request to delete data
        LOGGER.debug("Sending delete request response for " + loc);
        if (!this.requestQueue.offer(new DeleteFileRequest(app, loc, sem, !enableReuse, applicationDelete))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }

        // No need to wait if no reuse
        if (enableReuse) {
            // Wait for response
            LOGGER.debug("Waiting for delete request response...");
            sem.acquireUninterruptibly();
            LOGGER.debug("Data " + loc + " deleted.");
        }

    }

    /**
     * Marks a BindingObject for its deletion.
     *
     * @param code BindingObject code.
     */
    public void markForBindingObjectDeletion(int code) {
        if (!this.requestQueue.offer(new DeleteBindingObjectRequest(code))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }
    }

    /**
     * Adds a request for file raw transfer.
     *
     * @param faId Data Access Id.
     * @param location File location.
     */
    private void transferFileRaw(DataAccessId faId, DataLocation location) {
        Semaphore sem = new Semaphore(0);
        RAccessId faRId = (RAccessId) faId;
        TransferRawFileRequest request = new TransferRawFileRequest(faRId, location, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file raw");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Raw file transferred");
    }

    /**
     * Adds a request for open file transfer.
     *
     * @param faId Data Access Id.
     * @return Location of the transferred open file.
     */
    private DataLocation transferFileOpen(DataAccessId faId) {
        Semaphore sem = new Semaphore(0);
        TransferOpenFileRequest request = new TransferOpenFileRequest(faId, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer file open");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Open file transferred");
        return request.getLocation();
    }

    /**
     * Adds a request for open file transfer.
     *
     * @param faId Data Access Id.
     * @return Location of the transferred open file.
     */
    private DataLocation transferDirectoryOpen(DataAccessId faId) {
        Semaphore sem = new Semaphore(0);
        TransferOpenDirectoryRequest req = new TransferOpenDirectoryRequest(faId, sem);
        if (!this.requestQueue.offer(req)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "transfer directory open");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        LOGGER.debug("Open directory transferred");
        return req.getLocation();
    }

    /**
     * Adds a request to obtain an object from a worker to the master.
     *
     * @param oaId Data Access Id.
     * @return The synchronized object value.
     */
    private Object obtainObject(DataAccessId oaId) {
        // Ask for the object
        Semaphore sem = new Semaphore(0);
        TransferObjectRequest tor = new TransferObjectRequest(oaId, sem);
        if (!this.requestQueue.offer(tor)) {
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
            LogicalData ld = tor.getTargetData();
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
     * Adds a request to retrieve the result files from the workers to the master.
     *
     * @param app Application.
     */
    public void getResultFiles(Application app) {
        Semaphore sem = new Semaphore(0);
        GetResultFilesRequest request = new GetResultFilesRequest(app, sem);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "get result files");
        }

        // Wait for response
        sem.acquireUninterruptibly();

        UnblockResultFilesRequest urfr = new UnblockResultFilesRequest(request.getBlockedData());
        if (!this.requestQueue.offer(urfr)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "unlock result files");
        }
    }

    /**
     * Unregisters the given object.
     *
     * @param o Object to unregister.
     */
    public void deregisterObject(Object o) {
        if (DEBUG) {
            LOGGER.debug("Deregistering object " + o.hashCode());
        }
        if (!this.requestQueue.offer(new DeregisterObject(o))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "deregister object");
        }
    }

    /**
     * Registers a data value as available on remote locations.
     *
     * @param app application accessing the object.
     * @param code code identifying the object
     * @param dataId name of the data associated to the object
     */
    public void registerRemoteObject(Application app, int code, String dataId) {
        RegisterRemoteObjectDataRequest request = new RegisterRemoteObjectDataRequest(app, code, dataId);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data");
        }
    }

    /**
     * Registers a data value as available on remote locations.
     *
     * @param app application accessing the file.
     * @param loc location of the file being accessed
     * @param dataId name of the data associated to the file
     */
    public void registerRemoteFile(Application app, DataLocation loc, String dataId) {
        RegisterRemoteFileDataRequest request = new RegisterRemoteFileDataRequest(app, loc, dataId);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data");
        }
    }

    /**
     * Registers a collection data as available on remote locations.
     *
     * @param app application accessing the file.
     * @param collection collection identifier
     * @param dataId name of the data associated to the file
     */
    public void registerRemoteCollection(Application app, String collection, String dataId) {
        RegisterRemoteCollectionDataRequest request = new RegisterRemoteCollectionDataRequest(app, collection, dataId);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "register data");
        }
    }

    /**
     * Removes all the information related to data bound to a specific application.
     *
     * @param app application whose values are to be removed
     */
    public void deleteAllApplicationDataRequest(Application app) {
        Long appId = app.getId();
        DeleteAllApplicationDataRequest request = new DeleteAllApplicationDataRequest(app);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "delete all data from application " + appId);
        }
    }

    /**
     * Snapshot.
     *
     * @param app Application .
     */
    public void snapshot(Application app) {
        if (!this.requestQueue.offer(new SnapshotRequest(app))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "snapshot");
        }
    }

    @Override
    public void addCheckpointRequest(APRequest apRequest, String errorMessage) {
        if (!requestQueue.offer(apRequest)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + errorMessage);
        }
    }

    @Override
    public void allAvailableDataCheckpointed() {
        if (!this.requestQueue.offer(new ShutdownNotificationRequest(shutdownSemaphore))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "shutdown");
        }
    }

    /**
     * Loads the checkpoint policy.
     */
    private static void loadCheckpointingPoliciesJars() {
        LOGGER.info("Loading checkpointers...");
        String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);

        if (compssHome == null || compssHome.isEmpty()) {
            LOGGER.warn("WARN: COMPSS_HOME not defined, no checkpointers loaded.");
            return;
        }

        Classpath.loadJarsInPath(compssHome + CHECKPOINTER_REL_PATH, LOGGER);
    }

    /**
     * Constructs the checkpoint Manager setting the parameters.
     */
    private CheckpointManager constructCheckpointManager() {
        CheckpointManager checkpointer = null;
        try {
            String parameters = System.getProperty(COMPSsConstants.CHECKPOINT_PARAMS);
            HashMap<String, String> paramsMap = new HashMap<>();
            if (parameters != null && !parameters.equals("")) {
                if (DEBUG) {
                    LOGGER.debug("Reading Checkpointing policy parameters  " + parameters);
                }
                int index = parameters.indexOf("avoid.checkpoint");
                List<String> params;

                if (index != -1) {
                    params = new ArrayList<>(Arrays.asList(parameters.substring(0, index).split(",")));
                    String avoidTasks = parameters.substring(index);
                    params.add(avoidTasks);

                } else {
                    params = new ArrayList<>(Arrays.asList(parameters.split(",")));
                }
                for (String param : params) {
                    String[] values = param.split(":");
                    if (values[0].equals("period.time")) {
                        if (values[1].endsWith("h")) {
                            values[1] = String.valueOf(
                                Integer.parseInt(values[1].substring(0, values[1].length() - 1)) * 3600 * 1000);
                        } else if (values[1].endsWith("m")) {
                            values[1] = String
                                .valueOf(Integer.parseInt(values[1].substring(0, values[1].length() - 1)) * 60 * 1000);
                        } else if (values[1].endsWith("s")) {
                            values[1] =
                                String.valueOf(Integer.parseInt(values[1].substring(0, values[1].length() - 1)) * 1000);
                        } else {
                            values[1] = String.valueOf(Integer.parseInt(values[1]) * 60 * 1000);
                        }
                    }
                    paramsMap.put(values[0], values[1]);
                }
            }
            String cpFQN = System.getProperty(COMPSsConstants.CHECKPOINT_POLICY);
            if (cpFQN == null || cpFQN.isEmpty()) {
                cpFQN = COMPSsConstants.DEFAULT_CHECKPOINT;
            }
            Class<?> cpClass = Class.forName(cpFQN);
            Constructor<?> cpCnstr = cpClass.getConstructor(HashMap.class, AccessProcessor.class);
            checkpointer = (CheckpointManager) cpCnstr.newInstance(paramsMap, this);
            if (DEBUG) {
                LOGGER.debug("Loaded checkpointer " + checkpointer);
            }
        } catch (Exception e) {
            ErrorManager.fatal(ERR_LOAD_CHECKPOINTER, e);
        }
        return checkpointer;
    }
}
