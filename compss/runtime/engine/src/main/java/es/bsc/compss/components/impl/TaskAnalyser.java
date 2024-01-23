/*
 *  Copyright 2002-2023 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.checkpoint.CheckpointManager;
import es.bsc.compss.components.monitor.impl.GraphHandler;
import es.bsc.compss.components.monitor.impl.NoGraph;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.TaskListener;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.accesses.DataAccessesInfo;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.ObjectParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.util.ErrorManager;

import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StubItf;


/**
 * Class to analyze the data dependencies between tasks.
 */
public class TaskAnalyser {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String TASK_FAILED = "Task failed: ";
    private static final String TASK_CANCELED = "Task canceled: ";

    // Components
    private DataInfoProvider dip;
    private CheckpointManager cp;
    private GraphHandler gh;

    // Map: data Id -> WritersInfo
    private final Map<Integer, DataAccessesInfo> accessesInfo;


    /**
     * Creates a new Task Analyzer instance.
     */
    public TaskAnalyser() {
        this.accessesInfo = new TreeMap<>();
        LOGGER.info("Initialization finished");
        this.gh = new NoGraph();
    }

    /**
     * Sets the TaskAnalyser co-workers.
     *
     * @param dip DataInfoProvider co-worker.
     * @param cp checkpoint manager co-worker.
     */
    public void setCoWorkers(DataInfoProvider dip, CheckpointManager cp) {
        this.dip = dip;
        this.cp = cp;
    }

    /**
     * Sets the graph handler for the detected task and dependencies.
     *
     * @param gh Graph Handler.
     */
    public void setGM(GraphHandler gh) {
        this.gh = gh;
    }

    /**
     * Process the dependencies of a new task {@code currentTask}.
     *
     * @param currentTask Task.
     */
    public void processTask(Task currentTask) {
        TaskDescription description = currentTask.getTaskDescription();
        LOGGER.info("New " + description.getType().toString().toLowerCase() + " task: Name:" + description.getName()
            + "), ID = " + currentTask.getId() + " APP = " + currentTask.getApplication().getId());
        this.gh.startTaskAnalysis(currentTask);

        Application app = currentTask.getApplication();
        app.newTask(currentTask);

        // Check scheduling enforcing data
        int constrainingParam = -1;

        // Process parameters
        boolean taskHasEdge = processTaskParameters(currentTask, constrainingParam);
        registerIntermediateParameter(currentTask);
        markIntermediateParametersToDelete(currentTask);
        this.gh.endTaskAnalysis(currentTask, taskHasEdge);

        // Prepare checkpointer for task
        cp.newTask(currentTask);
    }

    private boolean processTaskParameters(Task currentTask, int constrainingParam) {
        List<Parameter> parameters = currentTask.getParameters();
        boolean taskHasEdge = false;
        for (int paramIdx = 0; paramIdx < parameters.size(); paramIdx++) {
            boolean isConstraining = paramIdx == constrainingParam;
            Parameter param = parameters.get(paramIdx);
            boolean paramHasEdge = registerParameterAccessAndAddDependencies(currentTask, param, isConstraining);
            taskHasEdge = taskHasEdge || paramHasEdge;
        }
        return taskHasEdge;
    }

    private void markIntermediateParametersToDelete(Task task) {
        for (Parameter p : task.getParameterDataToRemove()) {
            if (p.isPotentialDependency()) {
                DependencyParameter dp = (DependencyParameter) p;
                try {
                    dip.deleteData(dp.getAccess().getData());
                } catch (ValueUnawareRuntimeException e) {
                    // If not existing, the parameter was already removed. No need to do anything
                }
            }
        }
    }

    private void registerIntermediateParameter(Task task) {
        for (Parameter p : task.getIntermediateParameters()) {
            registerParameterAccessAndAddDependencies(task, p, false);
        }
    }

    /**
     * Performs an snapshot of the data.
     */
    public void snapshot() {
        cp.snapshot();
    }

    /**
     * Registers a data access from the main code and notifies when the data is available.
     *
     * @param rdar request indicating the data being accessed
     * @return The registered access Id.
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public DataAccessId processMainAccess(RegisterDataAccessRequest rdar) throws ValueUnawareRuntimeException {
        AccessParams access = rdar.getAccessParams();
        if (DEBUG) {
            LOGGER.debug("Registering access " + access.toString() + " from main code");
        }
        DataAccessId daId = dip.registerAccessToExistingData(access);
        if (daId == null) {
            if (DEBUG) {
                LOGGER.debug("Accessing a canceled data from main code. Returning null");
            }
            return null;
        }
        if (DEBUG) {
            LOGGER.debug("Registered access to data " + daId.getDataId() + " from main code");
        }

        if (daId.isRead()) {
            ReadingDataAccessId rdaId = (ReadingDataAccessId) daId;
            DataInstanceId di = rdaId.getReadDataInstance();
            cp.mainAccess(di);

            int dataId = daId.getDataId();
            // Retrieve writers information
            DataAccessesInfo dai = this.accessesInfo.get(dataId);
            if (dai != null) {
                DataInstanceId depInstance;
                if (daId.isWrite()) {
                    depInstance = ((WritingDataAccessId) daId).getWrittenDataInstance();
                } else {
                    depInstance = di;
                }
                dai.mainAccess(rdar, this.gh, depInstance);
            }
        }
        return daId;
    }

    /**
     * Registers the end of execution of task @{code task}.
     *
     * @param aTask Ended task.
     * @param checkpointing {@literal true} if task has been recovered by the checkpoint management
     */
    public void endTask(AbstractTask aTask, boolean checkpointing) {
        int taskId = aTask.getId();
        long start = System.currentTimeMillis();
        if (aTask instanceof Task) {
            Task task = (Task) aTask;
            boolean isFree = task.isFree();
            TaskState taskState = task.getStatus();
            LOGGER.info("Notification received for task " + taskId + " with end status " + taskState);
            // Check status
            if (!isFree) {
                LOGGER.debug("Task " + taskId + " is not registered as free. Waiting for other executions to end");
                return;
            }

            switch (taskState) {
                case FAILED:
                    OnFailure onFailure = task.getOnFailure();
                    if (onFailure == OnFailure.RETRY || onFailure == OnFailure.FAIL) {
                        // Raise error
                        ErrorManager.error(TASK_FAILED + task);
                        return;
                    }
                    if (onFailure == OnFailure.IGNORE || onFailure == OnFailure.CANCEL_SUCCESSORS) {
                        // Show warning
                        ErrorManager.warn(TASK_FAILED + task);
                    }
                    break;
                case CANCELED:
                    // Show warning
                    ErrorManager.warn(TASK_CANCELED + task);
                    break;
                default:
                    // Do nothing
            }

            // Mark parameter accesses
            if (DEBUG) {
                LOGGER.debug("Marking accessed parameters for task " + taskId);
            }

            for (Parameter param : task.getTaskDescription().getParameters()) {
                updateParameter(task, param);
            }

            // When a task can have internal temporal parameters,
            // the not used ones have to be updated to perform the data delete
            for (Parameter param : task.getUnusedIntermediateParameters()) {
                updateParameter(task, param);
            }

            // Free barrier dependencies
            if (DEBUG) {
                LOGGER.debug("Freeing barriers for task " + taskId);
            }

            // Free dependencies
            // Free task data dependencies
            if (DEBUG) {
                LOGGER.debug("Releasing waiting tasks for task " + taskId);
            }
            List<TaskListener> listeners = task.getListeners();
            if (listeners != null) {
                for (TaskListener listener : listeners) {
                    listener.taskFinished();
                }
            }

            // Check if the finished task was the last writer of a file, but only if task generation has finished
            // Task generation is finished if we are on noMoreTasks but we are not on a barrier
            if (DEBUG) {
                LOGGER.debug("Checking result file transfers for task " + taskId);
            }

            Application app = task.getApplication();
            // Release task groups of the task
            app.endTask(task);

            TaskMonitor registeredMonitor = task.getTaskMonitor();
            switch (taskState) {
                case FAILED:
                    registeredMonitor.onFailure();
                    break;
                case CANCELED:
                    registeredMonitor.onCancellation();
                    break;
                default:
                    registeredMonitor.onCompletion();
            }

            // Releases commutative groups dependent and releases all the waiting tasks
            releaseCommutativeGroups(task);

            // If we are not retrieving the checkpoint
            if (!checkpointing) {
                if (DEBUG) {
                    LOGGER.debug("Checkpoint saving task " + taskId);
                }
                cp.endTask(task);
            }
        }

        // Release data dependent tasks
        if (DEBUG) {
            LOGGER.debug("Releasing data dependant tasks for task " + taskId);
        }
        aTask.releaseDataDependents();

        if (DEBUG) {
            long time = System.currentTimeMillis() - start;
            LOGGER.debug("Task " + taskId + " end message processed in " + time + " ms.");
        }
    }

    /**
     * Barrier for group.
     *
     * @param request Barrier group request
     */
    public void barrierGroup(BarrierGroupRequest request) {
        Application app = request.getApp();
        String groupName = request.getGroupName();

        app.reachesGroupBarrier(groupName, request);
        this.gh.groupBarrier(request);
    }

    /**
     * Barrier.
     *
     * @param request Barrier request.
     */
    public void barrier(BarrierRequest request) {
        Application app = request.getApp();

        app.reachesBarrier(request);
        this.gh.barrier(this.accessesInfo);
    }

    /**
     * End of execution barrier.
     *
     * @param request End of execution request.
     */
    public void noMoreTasks(EndOfAppRequest request) {
        Application app = request.getApp();
        app.endReached(request);
        this.gh.endApp();
    }

    /**
     * Deletes the specified data and its renamings.
     *
     * @param data data to be deleted
     * @param applicationDelete whether the user code requested to delete the data ({@literal true}) or was removed by
     *            the runtime ({@literal false})
     * @throws ValueUnawareRuntimeException the runtime is not aware of the data
     */
    public void deleteData(DataParams data, boolean applicationDelete) throws ValueUnawareRuntimeException {
        DataInfo dataInfo = dip.deleteData(data);
        int dataId = dataInfo.getDataId();
        LOGGER.info("Deleting data " + dataId);

        // Deleting checkpointed data that is obsolete, INOUT that has a newest version
        if (applicationDelete) {
            cp.deletedData(dataInfo);
        }

        DataAccessesInfo dai = this.accessesInfo.remove(dataId);
        if (dai != null) {
            switch (dai.getDataType()) {
                case STREAM_T:
                case EXTERNAL_STREAM_T:
                    // No data to delete
                    break;
                case FILE_T:
                    // Remove file data form the list of written files
                    Application app = data.getApp();
                    FileInfo fInfo = (FileInfo) data.getDataInfo();
                    app.removeWrittenFile(fInfo);
                    break;
                default:
                    // Nothing to do for other types
                    break;
            }
        } else {
            LOGGER.warn("Writters info for data " + dataId + " not found.");
        }
    }

    /**
     * Shutdown the component.
     */
    public void shutdown() {
        this.gh.removeCurrentGraph();
        this.cp.shutdown();
    }

    /*
     * *************************************************************************************************************
     * TASK GROUPS PUBLIC METHODS
     ***************************************************************************************************************/
    /**
     * Sets the current task group to assign to tasks.
     *
     * @param app application to which the group belongs.
     * @param groupName Name of the group to set
     */
    public void setCurrentTaskGroup(Application app, String groupName) {
        app.stackTaskGroup(groupName);
        this.gh.openTaskGroup(groupName);
    }

    /**
     * Closes the last task group of an application.
     *
     * @param app Application to which the group belongs to
     */
    public void closeCurrentTaskGroup(Application app) {
        app.popGroup();
        this.gh.closeTaskGroup();
    }

    private void releaseCommutativeGroups(Task task) {
        if (!task.getCommutativeGroupList().isEmpty()) {
            for (CommutativeGroupTask group : task.getCommutativeGroupList()) {
                group.getCommutativeTasks().remove(task);
                group.setStatus(TaskState.FINISHED);
                group.removePredecessor(task);
                if (group.getPredecessors().isEmpty()) {
                    group.releaseDataDependents();
                    // Check if task is being waited
                    List<TaskListener> listeners = group.getListeners();
                    if (listeners != null) {
                        for (TaskListener listener : listeners) {
                            listener.taskFinished();
                        }
                    }
                    if (DEBUG) {
                        LOGGER.debug("Group " + group.getId() + " ended execution");
                        LOGGER.debug("Data dependents of group " + group.getCommutativeIdentifier() + " released ");
                    }
                }
            }
        }
    }

    /*
     * *************************************************************************************************************
     * DATA DEPENDENCY MANAGEMENT PRIVATE METHODS
     ***************************************************************************************************************/
    private boolean registerParameterAccessAndAddDependencies(Task currentTask, Parameter p, boolean isConstraining) {
        boolean hasParamEdge = false;
        if (p.isCollective()) {
            CollectiveParameter cp = (CollectiveParameter) p;
            this.gh.startGroupingEdges();
            for (Parameter content : cp.getElements()) {
                boolean hasCollectionParamEdge =
                    registerParameterAccessAndAddDependencies(currentTask, content, isConstraining);
                hasParamEdge = hasParamEdge || hasCollectionParamEdge;
            }
            this.gh.stopGroupingEdges();
        } else {
            if (p.getType() == DataType.OBJECT_T) {
                ObjectParameter op = (ObjectParameter) p;
                // Check if its PSCO class and persisted to infer its type
                if (op.getValue() instanceof StubItf && ((StubItf) op.getValue()).getID() != null) {
                    op.setType(DataType.PSCO_T);
                }
            }
        }

        // Inform the Data Manager about the new accesses
        DataAccessId daId;
        AccessParams access = p.getAccess();
        if (access != null) {
            daId = dip.registerDataAccess(access);
        } else {
            daId = null;
        }

        if (p.isCollective()) {
            try {
                deleteData(access.getData(), false);
            } catch (ValueUnawareRuntimeException e) {
                // If not existing, the collection was already removed. No need to do anything
            }
        }

        if (daId != null) {
            // Add parameter dependencies
            DependencyParameter dp = (DependencyParameter) p;
            dp.setDataAccessId(daId);
            hasParamEdge = addDependencies(currentTask, isConstraining, dp);
        } else {
            // Basic types do not produce access dependencies
            currentTask.registerFreeParam(p);
        }
        // Return data Id
        return hasParamEdge;
    }

    private boolean addDependencies(Task currentTask, boolean isConstraining, DependencyParameter dp) {
        // Add dependencies to the graph and register output values for future dependencies
        boolean hasParamEdge = false;
        DataAccessId daId = dp.getDataAccessId();
        int dataId = daId.getDataId();
        DataAccessesInfo dai = this.accessesInfo.get(dataId);
        switch (dp.getAccess().getMode()) {
            case R:
                hasParamEdge = checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                break;
            case RW:
                hasParamEdge = checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                registerOutputValues(currentTask, dp, false, dai);
                break;
            case W:
                // Register output values
                registerOutputValues(currentTask, dp, false, dai);
                break;
            case C:
                hasParamEdge = checkInputDependency(currentTask, dp, true, dataId, dai, isConstraining);
                registerOutputValues(currentTask, dp, true, dai);
                break;
            case CV:
                hasParamEdge = checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                registerOutputValues(currentTask, dp, false, dai);
                break;
        }
        return hasParamEdge;
    }

    private boolean checkInputDependency(Task currentTask, DependencyParameter dp, boolean isConcurrent, int dataId,
        DataAccessesInfo dai, boolean isConstraining) {
        if (DEBUG) {
            LOGGER.debug("Checking READ dependency for datum " + dataId + " and task " + currentTask.getId());
        }
        boolean hasEdge = false;
        if (dai != null) {
            hasEdge = dai.readValue(currentTask, dp, isConcurrent, this.gh);
            if (isConstraining) {
                AbstractTask lastWriter = dai.getConstrainingProducer();
                currentTask.setEnforcingTask((Task) lastWriter);
            }
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }
            currentTask.registerFreeParam(dp);
        }
        return hasEdge;
    }

    /**
     * Registers the output values of the task {@code currentTask}.
     *
     * @param currentTask Task.
     * @param dp Dependency Parameter.
     * @param isConcurrent data access was done in concurrent mode
     * @param dai AccessInfo related to the data being accessed
     */
    private void registerOutputValues(Task currentTask, DependencyParameter dp, boolean isConcurrent,
        DataAccessesInfo dai) {
        int currentTaskId = currentTask.getId();
        int dataId = dp.getDataAccessId().getDataId();
        Application app = currentTask.getApplication();

        if (DEBUG) {
            LOGGER.debug("Checking WRITE dependency for datum " + dataId + " and task " + currentTaskId);
        }

        if (dai == null) {
            dai = DataAccessesInfo.createAccessInfo(dp.getType());
            this.accessesInfo.put(dataId, dai);
        }
        dai.writeValue(currentTask, dp, isConcurrent, this.gh);

        // Update file and PSCO lists
        switch (dp.getType()) {
            case DIRECTORY_T:
            case FILE_T:
                FileInfo fInfo = (FileInfo) dp.getAccess().getDataInfo();
                app.addWrittenFile(fInfo);
                break;
            default:
                // Nothing to do with basic types
                // Objects are not checked, their version will be only get if the main accesses them
                break;
        }

        if (DEBUG) {
            LOGGER.debug("New writer for datum " + dataId + " is task " + currentTaskId);
        }
    }

    private void updateParameter(Task task, Parameter p) {
        if (p.isCollective()) {
            CollectiveParameter cParam = (CollectiveParameter) p;
            for (Parameter sp : cParam.getElements()) {
                updateParameter(task, sp);
            }
        }
        if (p.isPotentialDependency()) {
            DependencyParameter dp = (DependencyParameter) p;
            DataAccessId dAccId = dp.getDataAccessId();
            int dataId = dAccId.getDataId();

            DataType type = p.getType();
            if (type != DataType.DIRECTORY_T || type != DataType.STREAM_T || type != DataType.EXTERNAL_STREAM_T) {
                if (DEBUG) {
                    int currentTaskId = task.getId();
                    LOGGER.debug("Removing writters info for datum " + dataId + " and task " + currentTaskId);
                }
                DataAccessesInfo dai = this.accessesInfo.get(dataId);
                if (dai != null) {
                    switch (dp.getDirection()) {
                        case OUT:
                        case INOUT:
                            dai.completedProducer(task, this.gh);
                            break;
                        default:
                            break;
                    }
                }
            }

            if (DEBUG) {
                LOGGER.debug("Treating that data " + dAccId + " has been accessed at " + dp.getDataTarget());
            }

            if ((task.getOnFailure() == OnFailure.CANCEL_SUCCESSORS && (task.getStatus() == TaskState.FAILED))
                || task.getStatus() == TaskState.CANCELED) {
                this.dip.dataAccessHasBeenCanceled(dAccId, task.wasSubmited());
            } else {
                this.dip.dataHasBeenAccessed(dAccId);
            }
        }
    }
}
