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
package es.bsc.compss.components.impl;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.checkpoint.CheckpointBuilder;
import es.bsc.compss.checkpoint.CheckpointManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.ReduceTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.ResultFile;
import es.bsc.compss.types.data.access.MainAccess;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessid.EngineDataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.request.ap.APRequest;
import es.bsc.compss.types.request.ap.AlreadyAccessedRequest;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.CancelApplicationTasksRequest;
import es.bsc.compss.types.request.ap.CancelTaskGroupRequest;
import es.bsc.compss.types.request.ap.CheckpointerRequest;
import es.bsc.compss.types.request.ap.CloseTaskGroupRequest;
import es.bsc.compss.types.request.ap.DataGetLastVersionRequest;
import es.bsc.compss.types.request.ap.DeleteAllApplicationDataRequest;
import es.bsc.compss.types.request.ap.DeleteDataRequest;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.FinishDataAccessRequest;
import es.bsc.compss.types.request.ap.GetResultFilesRequest;
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
import es.bsc.compss.types.request.exceptions.NonExistingValueException;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.List;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Component to handle the tasks accesses to files and object.
 */
public class AccessProcessor extends RequestDispatcher<APRequest> implements CheckpointManager.User {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERR_LOAD_CHECKPOINTER = "Error loading checkpoint manager";

    // Other super-components
    private final TaskDispatcher taskDispatcher;

    // Subcomponents
    private final CheckpointManager checkpointManager;
    private Semaphore shutdownSemaphore;


    /**
     * Creates a new Access Processor instance.
     *
     * @param td Associated TaskDispatcher component.
     */
    public AccessProcessor(TaskDispatcher td) {
        super("AccessProcessor", LOGGER);

        this.taskDispatcher = td;
        // Start Subcomponents
        CheckpointManager cp;
        try {
            cp = CheckpointBuilder.constructCheckpointManager(this);
        } catch (Exception e) {
            ErrorManager.fatal(ERR_LOAD_CHECKPOINTER, e);
            cp = null;
        }
        this.checkpointManager = cp;
        Application.setCP(this.checkpointManager);
        start();
    }

    @Override
    public TraceEvent getThreadEvent() {
        return TraceEvent.AP_THREAD_ID;
    }

    @Override
    public void handleRequest(APRequest request) throws ShutdownException, COMPSsException {
        request.process(this, taskDispatcher);
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

        app.onTaskCreation(currentTask);

        LOGGER.debug("Requesting analysis of Task " + currentTask.getId());
        this.offerRequest(new TaskAnalysisRequest(currentTask), "new method task");
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

        app.onTaskCreation(currentTask);

        LOGGER.debug("Requesting analysis of new HTTP Task " + currentTask.getId());
        this.offerRequest(new TaskAnalysisRequest(currentTask), "new HTTP task");
        return currentTask.getId();
    }

    /**
     * Notifies the end of the given abstract task.
     *
     * @param task Ended task.
     */
    public void notifyTaskEnd(AbstractTask task) {
        this.offerRequest(new TaskEndNotification(task), "notify task end");
    }

    /**
     * Notifies a main access {@code oma} to a given data.
     *
     * @param ma Main Access.
     * @return Final value.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public <T> T mainAccess(MainAccess<T, ?, ?> ma) throws ValueUnawareRuntimeException {
        AccessParams<?> ap = ma.getParameters();
        if (DEBUG) {
            LOGGER.debug("Requesting main access to " + ap.getDataDescription() + " from App " + ma.getApp());
        }

        // Tell the DIP that the application wants to access an object
        EngineDataAccessId daId = registerDataAccess(ma);
        if (daId == null) {
            ErrorManager.warn("No version available. Returning null");
            return ma.getUnavailableValueResponse();
        } else {
            // Ask for the data
            T oUpdated;
            oUpdated = ma.fetch(daId);
            if (ma.isAccessFinishedOnRegistration()) {
                EngineDataInstanceId wId = null;
                if (daId.isWrite()) {
                    wId = ((WritingDataAccessId) daId).getWrittenDataInstance();
                }
                finishDataAccess(ma, wId);

            }
            return oUpdated;
        }
    }

    /**
     * Marks an access to a data as finished.
     *
     * @param ma Access parameters.
     */
    public void finishDataAccess(MainAccess ma, EngineDataInstanceId generatedDaId) {
        this.offerRequest(new FinishDataAccessRequest(ma, generatedDaId), "finishing data access");
    }

    /**
     * Returns the Identifier of the data corresponding to the last version of a data.
     *
     * @param app application obtaining the last access for the data
     * @param data Description of the data being accessed.
     * @return data corresponding to the last version of the data.
     */
    public LogicalData getDataLastVersion(Application app, DataParams data) {
        // Ask for the object
        DataGetLastVersionRequest odr = new DataGetLastVersionRequest(app, data);
        this.offerRequest(odr, "data version query");
        return odr.getData();
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
        this.offerRequest(bgr, "wait for all tasks");
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
        this.offerRequest(br, "wait for all tasks");

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
        this.offerRequest(eoar, "no more tasks");
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
     * @param app application accessing the value
     * @param data querying data
     * @return {@code true} if the data has been accessed, {@code false} otherwise.
     */
    public boolean alreadyAccessed(Application app, DataParams data) {
        AlreadyAccessedRequest request = new AlreadyAccessedRequest(app, data);
        this.offerRequest(request, "already accessed location");

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
        this.offerRequest(new CancelApplicationTasksRequest(app, sem), "wait for task");

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
        this.offerRequest(new CancelTaskGroupRequest(app, groupName, sem), "wait for task");

        // Wait for response
        LOGGER.debug("Waiting for cancellation of tasks in group " + groupName);
        sem.acquireUninterruptibly();

        LOGGER.info("Tasks cancelled for group " + groupName);
    }

    /**
     * Registers a new data access and waits for it to be available.
     *
     * @param access Access done by the main.
     * @return The registered access Id.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    private EngineDataAccessId registerDataAccess(MainAccess access) throws ValueUnawareRuntimeException {
        RegisterDataAccessRequest request = new RegisterDataAccessRequest(access);
        this.offerRequest(request, "register data access");

        // Wait for response
        request.waitForCompletion();
        EngineDataAccessId daId = request.getAccessId();

        return daId;
    }

    /**
     * Sets the task group to assign to all the following tasks.
     *
     * @param groupName Name of the task group
     * @param app Application.
     */
    public void setCurrentTaskGroup(String groupName, Application app) {
        OpenTaskGroupRequest request = new OpenTaskGroupRequest(groupName, app);
        this.offerRequest(request, "new task group");
    }

    /**
     * Closes the current task group.
     *
     * @param app Application.
     */
    public void closeCurrentTaskGroup(Application app) {
        CloseTaskGroupRequest request = new CloseTaskGroupRequest(app);
        this.offerRequest(request, "closure of task group");
    }

    /**
     * Unblock result files.
     *
     * @param resFiles List of result files to unblock.
     */
    public void unblockResultFiles(List<ResultFile> resFiles) {
        UnblockResultFilesRequest request = new UnblockResultFilesRequest(resFiles);
        this.offerRequest(request, "unblock result files");
    }

    /**
     * Shutdown request.
     */
    public void shutdown() {
        shutdownSemaphore = new Semaphore(0);
        this.offerRequest(new ShutdownRequest(shutdownSemaphore), "shutdown");

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
        this.offerRequest(request, "get current task state");

        // Wait for response
        sem.acquireUninterruptibly();

        return request.getResponse();
    }

    /**
     * Marks a location for deletion.
     *
     * @param app application requesting the data removal
     * @param data data to be marked for deletion
     * @param enableReuse {@literal true}, if the application must be able to use the same data name for a new data
     * @param applicationDelete {@literal true}, if the application requested the data deletion; {@literal false}
     *            otherwise
     */
    public void deleteData(Application app, DataParams data, boolean enableReuse, boolean applicationDelete) {
        LOGGER.debug("Marking data " + data.getDescription() + " for deletion");
        boolean delete = true;
        // No need to wait if data is noReuse
        if (enableReuse) {
            WaitForDataReadyToDeleteRequest request = new WaitForDataReadyToDeleteRequest(app, data);
            // Wait for data to be ready for deletion
            this.offerRequest(request, "wait for data ready to delete");
            try {
                request.waitForDataReadiness();
            } catch (ValueUnawareRuntimeException vure) {
                try {
                    data.deleteLocal();
                    LOGGER.info("[DeleteData] Data " + data.getDescription() + " deleted.");
                } catch (Exception e) {
                    LOGGER.error("[DeleteData] Error on deleting " + data.getDescription(), e);
                }
                return;
            } catch (NonExistingValueException ex) {
                delete = false;
            }
        }

        // Request to delete data
        LOGGER.debug("Sending delete request for " + data.getDescription());
        DeleteDataRequest req = new DeleteDataRequest(app, data, applicationDelete);
        this.offerRequest(req, "mark for deletion");

        // No need to wait if no reuse
        if (enableReuse && delete) {
            try {
                data.deleteLocal();
                LOGGER.info("[DeleteData] Data " + data.getDescription() + " deleted.");
            } catch (Exception e) {
                LOGGER.error("[DeleteData] Error on deleting " + data.getDescription(), e);
            }
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
        this.offerRequest(request, "get result files");

        // Wait for response
        sem.acquireUninterruptibly();

        UnblockResultFilesRequest urfr = new UnblockResultFilesRequest(request.getBlockedData());
        this.offerRequest(urfr, "unlock result files");
    }

    /**
     * Registers a data value as available on remote locations.
     *
     * @param app application accessing the value
     * @param accessedValue the value being accessed by the application
     * @param dataId name of the data associated to the object
     */
    public void registerRemoteData(Application app, DataParams accessedValue, String dataId) {
        RegisterRemoteDataRequest request = new RegisterRemoteDataRequest(app, accessedValue, dataId);
        this.offerRequest(request, "register data");
    }

    /**
     * Removes all the information related to data bound to a specific application.
     *
     * @param app application whose values are to be removed
     */
    public void deleteAllApplicationDataRequest(Application app) {
        Long appId = app.getId();
        DeleteAllApplicationDataRequest request = new DeleteAllApplicationDataRequest(app);
        if (this.offerRequest(request, "delete all data from application " + appId)) {
            request.waitForCompletion();
        }
    }

    /**
     * Snapshot.
     *
     * @param app Application .
     */
    public void snapshot(Application app) {
        this.offerRequest(new SnapshotRequest(app), "snapshot");
    }

    @Override
    public void addCheckpointRequest(CheckpointerRequest apRequest, String errorMessage) {
        this.offerRequest(apRequest, errorMessage);
    }

    @Override
    public void allAvailableDataCheckpointed() {
        this.offerRequest(new ShutdownNotificationRequest(shutdownSemaphore), "shutdown");
    }

    /**
     * Stops the internal components of the AP.
     */
    public void shutdownCP() {
        // Before shutting down the AP we need to confirm that all CP copies are done. Notifications arrives through
        // allAvailableDataCheckpointed method
        this.checkpointManager.shutdown();
    }
}
