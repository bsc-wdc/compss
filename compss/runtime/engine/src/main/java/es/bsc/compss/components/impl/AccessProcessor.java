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
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.checkpoint.CheckpointManager;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.ReduceTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataParams;
import es.bsc.compss.types.data.DataParams.ObjectData;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.access.DirectoryMainAccess;
import es.bsc.compss.types.data.access.FileMainAccess;
import es.bsc.compss.types.data.access.ObjectMainAccess;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.accessparams.DirectoryAccessParams;
import es.bsc.compss.types.data.accessparams.FileAccessParams;
import es.bsc.compss.types.data.accessparams.ObjectAccessParams;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.request.ap.APRequest;
import es.bsc.compss.types.request.ap.AlreadyAccessedRequest;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.CancelApplicationTasksRequest;
import es.bsc.compss.types.request.ap.CancelTaskGroupRequest;
import es.bsc.compss.types.request.ap.CloseTaskGroupRequest;
import es.bsc.compss.types.request.ap.DataGetLastVersionRequest;
import es.bsc.compss.types.request.ap.DeleteAllApplicationDataRequest;
import es.bsc.compss.types.request.ap.DeleteBindingObjectRequest;
import es.bsc.compss.types.request.ap.DeleteFileRequest;
import es.bsc.compss.types.request.ap.DeregisterObject;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.FinishDataAccessRequest;
import es.bsc.compss.types.request.ap.GetResultFilesRequest;
import es.bsc.compss.types.request.ap.IsObjectHereRequest;
import es.bsc.compss.types.request.ap.OpenTaskGroupRequest;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.ap.RegisterRemoteDataRequest;
import es.bsc.compss.types.request.ap.ShutdownNotificationRequest;
import es.bsc.compss.types.request.ap.ShutdownRequest;
import es.bsc.compss.types.request.ap.SnapshotRequest;
import es.bsc.compss.types.request.ap.TaskAnalysisRequest;
import es.bsc.compss.types.request.ap.TaskEndNotification;
import es.bsc.compss.types.request.ap.TasksStateRequest;
import es.bsc.compss.types.request.ap.UnblockResultFilesRequest;
import es.bsc.compss.types.request.ap.WaitForDataReadyToDeleteRequest;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
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
     * Marks an access to a data as finished.
     *
     * @param ap Access parameters.
     */
    public void finishDataAccess(AccessParams ap, DataInstanceId generatedDaId) {
        if (!this.requestQueue.offer(new FinishDataAccessRequest(ap, generatedDaId))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "finishing data access");
        }
    }

    /**
     * Notifies a main access {@code fma} to a given file.
     *
     * @param fma File Access.
     * @return Final location.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public DataLocation mainAccessToFile(FileMainAccess<?, ?> fma) throws ValueUnawareRuntimeException {
        FileAccessParams fap = fma.getParameters();
        fap.checkAccessValidity(this);

        // Tell the DM that the application wants to access a file.
        // Wait until the last writer task for the file has finished.
        DataAccessId faId = registerDataAccess(fap, AccessMode.R);

        DataLocation tgtLocation = fap.getLocation();
        if (faId == null) { // If fiId is null data is cancelled returning null location
            ErrorManager.warn("No version available. Returning null");
            try {
                String path = ProtocolType.FILE_URI.getSchema() + "null";
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), new SimpleURI(path));
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION, e);
            }
        } else {
            if (faId.isRead()) {
                tgtLocation = fma.fetchForOpen(faId);
            }

            if (faId.isWrite()) {
                // Mode contains W
                LOGGER.debug("File " + faId.getDataId() + " mode contains W, register new writer");
                WritingDataAccessId wdaId = (WritingDataAccessId) faId;
                DataInstanceId daId = wdaId.getWrittenDataInstance();
                String rename = daId.getRenaming();
                String path = ProtocolType.FILE_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + rename;
                try {
                    SimpleURI uri = new SimpleURI(path);
                    tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
                }
                Comm.registerLocation(rename, tgtLocation);
            }
            if (DEBUG) {
                LOGGER.debug("File " + faId.getDataId() + " located on "
                    + (tgtLocation != null ? tgtLocation.toString() : "null"));
            }
        }
        return tgtLocation;
    }

    /**
     * Notifies a main access {@code sourceLocation} to a given directory.
     *
     * @param dma Directory Access Description.
     * @return Final location.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public DataLocation mainAccessToDirectory(DirectoryMainAccess dma) throws ValueUnawareRuntimeException {
        DirectoryAccessParams dap = dma.getParameters();
        dap.checkAccessValidity(this);
        // Tell the DM that the application wants to access a file.
        DataAccessId daId = registerDataAccess(dap, AccessMode.R);

        DataLocation tgtLocation = dap.getLocation();
        if (daId == null) { // If fiId is null data is cancelled returning null location
            ErrorManager.warn("No version available. Returning null");
            try {
                String path = ProtocolType.DIR_URI.getSchema() + "null";
                tgtLocation = DataLocation.createLocation(Comm.getAppHost(), new SimpleURI(path));
            } catch (Exception e) {
                ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION, e);
            }
        } else {
            if (daId.isRead()) {
                tgtLocation = dma.fetchForOpen(daId);
            }
            if (daId.isWrite()) {
                LOGGER.debug("Data " + daId.getDataId() + " mode contains W, register new writer");
                WritingDataAccessId wdaId = (WritingDataAccessId) daId;
                DataInstanceId diId = wdaId.getWrittenDataInstance();
                String rename = diId.getRenaming();
                String path = ProtocolType.DIR_URI.getSchema() + Comm.getAppHost().getWorkingDirectory() + rename;
                try {
                    SimpleURI uri = new SimpleURI(path);
                    tgtLocation = DataLocation.createLocation(Comm.getAppHost(), uri);
                } catch (Exception e) {
                    ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
                }
                Comm.registerLocation(rename, tgtLocation);
            }

            if (DEBUG) {
                LOGGER.debug("Directory " + daId.getDataId() + " located on " + tgtLocation.toString());
            }
        }
        return tgtLocation;
    }

    /**
     * Returns whether the value with hashCode {@code hashCode} is valid or obsolete.
     *
     * @param data Description of the object to check
     * @return {@code true} if the object is valid, {@code false} otherwise.
     */
    public boolean isCurrentRegisterValueValid(ObjectData data) {
        LOGGER.debug("Checking if value of " + data.getDescription() + " is valid");

        IsObjectHereRequest request = new IsObjectHereRequest(data);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "valid object value");
        }

        // Log response and return
        boolean isValid = request.getResponse();
        if (DEBUG) {
            if (isValid) {
                LOGGER.debug("Value of " + data.getDescription() + " is valid");
            } else {
                LOGGER.debug("Value of " + data.getDescription() + " is NOT valid");
            }
        }

        return isValid;
    }

    /**
     * Returns the Identifier of the data corresponding to the last version of an dat.
     *
     * @param data Description of the data being accessed.
     * @return data corresponding to the last version of the data.
     */
    public LogicalData getDataLastVersion(DataParams data) {
        // Ask for the object
        DataGetLastVersionRequest odr = new DataGetLastVersionRequest(data);
        if (!this.requestQueue.offer(odr)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "data version query");
        }

        return odr.getData();
    }

    /**
     * Notifies a main access {@code oma} to a given object.
     *
     * @param oma Object Access.
     * @return Final value.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public <T> T mainAccess(ObjectMainAccess<T, ?, ?> oma) throws ValueUnawareRuntimeException {
        ObjectAccessParams<T, ?> oap = oma.getParameters();
        if (DEBUG) {
            LOGGER.debug("Requesting main access to " + oap.getDataDescription());
        }
        oap.checkAccessValidity(this);

        // Tell the DIP that the application wants to access an object
        DataAccessId oaId = registerDataAccess(oap, AccessMode.RW);

        // Ask for the object
        T oUpdated;
        oUpdated = oma.fetchObject(oaId);
        if (oma.isAccessFinishedOnRegistration()) {
            DataInstanceId wId = null;
            if (oaId.isWrite()) {
                wId = ((WritingDataAccessId) oaId).getWrittenDataInstance();
            }
            finishDataAccess(oap, wId);

        }
        return oUpdated;
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
     * Returns whether the @{code data} has already been accessed or not.
     *
     * @param data querying data
     * @return {@code true} if the data has been accessed, {@code false} otherwise.
     */
    public boolean alreadyAccessed(DataParams data) {
        AlreadyAccessedRequest request = new AlreadyAccessedRequest(data);
        if (!this.requestQueue.offer(request)) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "already accessed location");
        }

        // Wait for response
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
     * @param app Application requesting unregistering the object.
     * @param code BindingObject code.
     */
    public void markForBindingObjectDeletion(Application app, int code) {
        if (!this.requestQueue.offer(new DeleteBindingObjectRequest(app, code))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "mark for deletion");
        }
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
     * @param app Application.
     * @param o Object to unregister.
     * @param hashcode code of the object being removed
     */
    public void deregisterObject(Application app, Object o, int hashcode) {
        if (DEBUG) {
            LOGGER.debug("Deregistering object " + hashcode);
        }
        if (!this.requestQueue.offer(new DeregisterObject(app, o, hashcode))) {
            ErrorManager.error(ERROR_QUEUE_OFFER + "deregister object");
        }
    }

    /**
     * Registers a data value as available on remote locations.
     *
     * @param accessedValue the value being accessed by the application
     * @param dataId name of the data associated to the object
     */
    public void registerRemoteData(DataParams accessedValue, String dataId) {
        RegisterRemoteDataRequest request = new RegisterRemoteDataRequest(accessedValue, dataId);
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
        } else {
            request.waitForCompletion();
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
                        } else {
                            if (values[1].endsWith("m")) {
                                values[1] = String.valueOf(
                                    Integer.parseInt(values[1].substring(0, values[1].length() - 1)) * 60 * 1000);
                            } else {
                                if (values[1].endsWith("s")) {
                                    values[1] = String.valueOf(
                                        Integer.parseInt(values[1].substring(0, values[1].length() - 1)) * 1000);
                                } else {
                                    values[1] = String.valueOf(Integer.parseInt(values[1]) * 60 * 1000);
                                }
                            }
                        }
                    }
                    paramsMap.put(values[0], values[1]);
                }
            }
            String cpFQN = System.getProperty(COMPSsConstants.CHECKPOINT_POLICY);
            if (cpFQN == null || cpFQN.isEmpty()) {
                cpFQN = COMPSsDefaults.CHECKPOINT;
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
