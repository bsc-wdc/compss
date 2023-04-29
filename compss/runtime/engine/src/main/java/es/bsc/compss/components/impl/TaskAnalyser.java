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

import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.checkpoint.CheckpointManager;
import es.bsc.compss.components.monitor.impl.EdgeType;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.components.monitor.impl.GraphHandler;
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
import es.bsc.compss.types.data.DataAccessId.Direction;
import es.bsc.compss.types.data.DataAccessId.ReadingDataAccessId;
import es.bsc.compss.types.data.DataAccessId.WritingDataAccessId;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataParams;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
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

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StubItf;


/**
 * Class to analyze the data dependencies between tasks.
 */
public class TaskAnalyser implements GraphHandler {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String TASK_FAILED = "Task failed: ";
    private static final String TASK_CANCELED = "Task canceled: ";

    // Components
    private DataInfoProvider dip;
    private CheckpointManager cp;
    private GraphGenerator gm;

    // Map: data Id -> WritersInfo
    private final Map<Integer, DataAccessesInfo> accessesInfo;

    // List of submitted reduce tasks
    private final List<String> reduceTasksNames;

    // Graph drawing
    private static final boolean IS_DRAW_GRAPH = GraphGenerator.isEnabled();
    private int synchronizationId;
    private boolean taskDetectedAfterSync;


    /**
     * Creates a new Task Analyzer instance.
     */
    public TaskAnalyser() {
        this.accessesInfo = new TreeMap<>();
        this.synchronizationId = 0;
        this.taskDetectedAfterSync = false;
        this.reduceTasksNames = new ArrayList<>();
        LOGGER.info("Initialization finished");
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
     * Sets the graph generator co-worker.
     *
     * @param gm Graph Generator co-worker.
     */
    public void setGM(GraphGenerator gm) {
        this.gm = gm;

        // Add initial synchronization point
        if (IS_DRAW_GRAPH) {
            this.gm.addSynchroToGraph(0);
        }
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

        if (IS_DRAW_GRAPH) {
            addNewTask(currentTask);
        }

        Application app = currentTask.getApplication();
        app.newTask(currentTask);

        // Check scheduling enforcing data
        int constrainingParam = -1;

        // Add reduction task to reduce task list
        if (description.isReduction()) {
            this.reduceTasksNames.add(description.getName());
        }

        // Process parameters
        boolean taskHasEdge = processTaskParameters(currentTask, constrainingParam);
        registerIntermediateParameter(currentTask);
        markIntermediateParametersToDelete(currentTask);

        if (IS_DRAW_GRAPH && !taskHasEdge) {
            // If the graph must be written and the task has no edge due to its parameters,
            // add a direct dependency from last sync to task.
            addEdgeFromMainToTask(currentTask);
        }

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
            return daId;
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
                dai.mainAccess(rdar, this, depInstance);
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
                updateParameterAccess(task, param);
                updateLastWritters(task, param);
            }

            // When a task can have internal temporal parameters,
            // the not used ones have to be updated to perform the data delete
            for (Parameter param : task.getUnusedIntermediateParameters()) {
                updateParameterAccess(task, param);
                updateLastWritters(task, param);
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

        // Addition of missing commutative groups to graph
        if (IS_DRAW_GRAPH) {
            addMissingCommutativeTasksToGraph();
            addNewGroupBarrierToGraph(request);
            // We can draw the graph on a barrier while we wait for tasks
            this.gm.commitGraph(false);
        }
    }

    /**
     * Barrier.
     *
     * @param request Barrier request.
     */
    public void barrier(BarrierRequest request) {
        Application app = request.getApp();

        app.reachesBarrier(request);

        if (IS_DRAW_GRAPH) {
            // Addition of missing commutative groups to graph
            addMissingCommutativeTasksToGraph();
            addNewBarrier();

            // We can draw the graph on a barrier while we wait for tasks
            this.gm.commitGraph(false);
        }
    }

    /**
     * End of execution barrier.
     *
     * @param request End of execution request.
     */
    public void noMoreTasks(EndOfAppRequest request) {
        Application app = request.getApp();
        app.endReached(request);

        if (IS_DRAW_GRAPH) {
            addMissingCommutativeTasksToGraph();
            this.gm.commitGraph(true);
        }
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
                    Application.removeWrittenFileIdFromAllApps(dataId);
                    break;
                case PSCO_T:
                    // Remove PSCO data from the list of written PSCO
                    Application.removeWrittenPSCOIdFromAllApps(dataId);
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
     * Returns the written files and deletes them.
     *
     * @param app Application.
     * @return List of written files of the application.
     */
    public Set<Integer> getAndRemoveWrittenFiles(Application app) {
        return app.getWrittenFileIds();
    }

    /**
     * Shutdown the component.
     */
    public void shutdown() {
        if (IS_DRAW_GRAPH) {
            GraphGenerator.removeTemporaryGraph();
        }
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
     * @param barrier Flag stating if the group has to perform a barrier.
     * @param groupName Name of the group to set
     */
    public void setCurrentTaskGroup(Application app, boolean barrier, String groupName) {
        app.stackTaskGroup(groupName);
        if (IS_DRAW_GRAPH) {
            this.gm.addTaskGroupToGraph(groupName);
            LOGGER.debug("Group " + groupName + " added to graph");
        }
    }

    /**
     * Closes the last task group of an application.
     *
     * @param app Application to which the group belongs to
     */
    public void closeCurrentTaskGroup(Application app) {
        app.popGroup();
        if (IS_DRAW_GRAPH) {
            this.gm.closeGroupInGraph();
        }
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
            if (IS_DRAW_GRAPH) {
                this.gm.startGroupingEdges();
            }
            for (Parameter content : cp.getElements()) {
                boolean hasCollectionParamEdge =
                    registerParameterAccessAndAddDependencies(currentTask, content, isConstraining);
                hasParamEdge = hasParamEdge || hasCollectionParamEdge;
            }
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
            hasEdge = dai.readValue(currentTask, dp, isConcurrent, this);
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
        dai.writeValue(currentTask, dp, isConcurrent, this);

        // Update file and PSCO lists
        switch (dp.getType()) {
            case DIRECTORY_T:
            case FILE_T:
                app.addWrittenFileId(dataId);
                break;
            case PSCO_T:
                app.addWrittenPSCOId(dataId);
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

    private void updateLastWritters(AbstractTask task, Parameter p) {
        if (p.isCollective()) {
            CollectiveParameter cParam = (CollectiveParameter) p;
            for (Parameter sp : cParam.getElements()) {
                updateLastWritters(task, sp);
            }
        }
        DataType type = p.getType();
        if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T || type == DataType.COLLECTION_T
            || type == DataType.DICT_COLLECTION_T) {
            DependencyParameter dp = (DependencyParameter) p;
            int dataId = dp.getDataAccessId().getDataId();
            if (DEBUG) {
                int currentTaskId = task.getId();
                LOGGER.debug("Removing writters info for datum " + dataId + " and task " + currentTaskId);
            }
            DataAccessesInfo dai = this.accessesInfo.get(dataId);
            if (dai != null) {
                switch (dp.getDirection()) {
                    case OUT:
                    case INOUT:
                        dai.completedProducer(task, this);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void updateParameterAccess(Task t, Parameter p) {
        if (p.isCollective()) {
            for (Parameter subParam : ((CollectiveParameter) p).getElements()) {
                updateParameterAccess(t, subParam);
            }
        }
        DataType type = p.getType();
        if (type == DataType.FILE_T || type == DataType.DIRECTORY_T || type == DataType.OBJECT_T
            || type == DataType.PSCO_T || type == DataType.STREAM_T || type == DataType.EXTERNAL_STREAM_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T || type == DataType.COLLECTION_T
            || type == DataType.DICT_COLLECTION_T) {

            DependencyParameter dPar = (DependencyParameter) p;
            DataAccessId dAccId = dPar.getDataAccessId();
            if (DEBUG) {
                LOGGER.debug("Treating that data " + dAccId + " has been accessed at " + dPar.getDataTarget());
            }

            if ((t.getOnFailure() == OnFailure.CANCEL_SUCCESSORS && (t.getStatus() == TaskState.FAILED))
                || t.getStatus() == TaskState.CANCELED) {
                this.dip.dataAccessHasBeenCanceled(dAccId, t.wasSubmited());
            } else {
                this.dip.dataHasBeenAccessed(dAccId);
            }
        }
    }

    /*
     **************************************************************************************************************
     * GRAPH WRAPPERS
     **************************************************************************************************************/

    private void addMissingCommutativeTasksToGraph() {
        this.gm.closeCommutativeGroups();
    }

    @Override
    public void drawTaskInCommutativeGroup(Task task, CommutativeGroupTask group) {
        this.gm.addTaskToCommutativeGroup(task, group.getCommutativeIdentifier().toString());
    }

    @Override
    public void closeCommutativeTasksGroup(CommutativeGroupTask group) {
        this.gm.closeCommutativeGroup(group.getCommutativeIdentifier().toString());
    }

    @Override
    public void drawStandardEdge(Task consumer, DataAccessId daId, AbstractTask producer) {
        // Retrieve common information
        int dataId = daId.getDataId();
        Direction d = daId.getDirection();
        int dataVersion;
        switch (d) {
            case C:
            case R:
                dataVersion = ((RAccessId) daId).getRVersionId();
                break;
            case W:
                dataVersion = ((WAccessId) daId).getWVersionId();
                break;
            default:
                dataVersion = ((RWAccessId) daId).getRVersionId();
                break;
        }

        if (producer != null && producer != consumer) {
            if (producer instanceof Task) {
                addDataEdgeFromTaskToTask((Task) producer, consumer, dataId, dataVersion);
            } else {
                addEdgeFromCommutativeToTask(consumer, dataId, dataVersion, ((CommutativeGroupTask) producer), true);
            }
        } else {
            addDataEdgeFromMainToTask(consumer, dataId, dataVersion);
        }
    }

    @Override
    public void drawStreamEdge(AbstractTask currentTask, Integer dataId, boolean isWrite) {
        String stream = "Stream" + dataId;

        // Add stream node even if it exists
        addStreamToGraph(stream);

        // Add dependency
        addStreamEdge(currentTask, stream, isWrite);
    }

    /**
     * We have detected a new task, register it into the graph. STEPS: Only adds the node.
     *
     * @param task New task.
     */
    private void addNewTask(Task task) {
        // Set the syncId of the task
        task.setSynchronizationId(this.synchronizationId);
        // Update current sync status
        this.taskDetectedAfterSync = true;

        // Add task to graph
        addTaskToGraph(task);
    }

    /**
     * Adds the task to the graph to print.
     *
     * @param task Task to print.
     */
    private void addTaskToGraph(Task task) {
        this.gm.addTaskToGraph(task);
    }

    /**
     * Adds a stream node to the graph to print.
     *
     * @param stream Stream name to print.
     */
    private void addStreamToGraph(String stream) {
        // Add stream to graph
        this.gm.addStreamToGraph(stream);
    }

    /**
     * We will execute a task whose data is produced by another task. STEPS: Add an edge from the previous task or the
     * last synchronization point to the new task.
     *
     * @param source Source task.
     * @param dest Destination task.
     * @param dataId Data causing the dependency.
     * @param dataVersion Data version.
     */
    private void addDataEdgeFromTaskToTask(Task source, Task dest, int dataId, int dataVersion) {
        if (source.getSynchronizationId() == dest.getSynchronizationId()) {
            String src = String.valueOf(source.getId());
            String dst = String.valueOf(dest.getId());
            String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
            this.gm.addEdgeToGraph(src, dst, EdgeType.DATA_DEPENDENCY, dep);
        } else {
            String src = "Synchro" + dest.getSynchronizationId();
            String dst = String.valueOf(dest.getId());
            String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
            this.gm.addEdgeToGraph(src, dst, EdgeType.DATA_DEPENDENCY, dep);
        }
    }

    /**
     * We will execute a task with no predecessors, data must be retrieved from the last synchronization point. STEPS:
     * Add edge from sync to task
     *
     * @param dest Destination task.
     * @param dataId Data causing the dependency.
     * @param dataVersion Data version.
     */
    private void addDataEdgeFromMainToTask(Task dest, int dataId, int dataVersion) {
        String src = "Synchro" + dest.getSynchronizationId();
        String dst = String.valueOf(dest.getId());
        String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
        this.gm.addEdgeToGraph(src, dst, EdgeType.DATA_DEPENDENCY, dep);
    }

    /**
     * We will execute a task with no predecessors. Add edge from sync to task.
     *
     * @param dest Destination task.
     */
    private void addEdgeFromMainToTask(Task dest) {
        String src = "Synchro" + dest.getSynchronizationId();
        String dst = String.valueOf(dest.getId());
        String dep = "";
        this.gm.addEdgeToGraph(src, dst, EdgeType.DATA_DEPENDENCY, dep);
    }

    @Override
    public void addEdgeFromTaskToMain(AbstractTask task, EdgeType edgeType, DataInstanceId accessedData) {
        // Add Sync if any task has been created
        if (this.taskDetectedAfterSync) {
            this.taskDetectedAfterSync = false;

            int oldSyncId = this.synchronizationId;
            this.synchronizationId++;

            this.gm.addSynchroToGraph(this.synchronizationId);

            String oldSync = "Synchro" + oldSyncId;
            String currentSync = "Synchro" + this.synchronizationId;
            this.gm.addEdgeToGraph(oldSync, currentSync, edgeType, "");
        }

        int dataId = accessedData.getDataId();
        int dataVersion = accessedData.getVersionId();
        // Add edge from task to sync
        String dest = "Synchro" + this.synchronizationId;
        if (task instanceof CommutativeGroupTask && !((CommutativeGroupTask) task).getCommutativeTasks().isEmpty()) {
            // Add edge from commutative group to synch
            CommutativeGroupTask commGroupTask = (CommutativeGroupTask) task;
            String src = String.valueOf(commGroupTask.getCommutativeTasks().get(0).getId());
            String groupId = commGroupTask.getCommutativeIdentifier().toString();
            this.gm.addEdgeToGraphFromGroup(src, dest, String.valueOf(dataId) + "v" + String.valueOf(dataVersion),
                groupId, "clusterCommutative", edgeType);
        } else {
            // Add edge from task to sync
            String src = String.valueOf(task.getId());
            this.gm.addEdgeToGraph(src, dest, edgeType, String.valueOf(dataId) + "v" + String.valueOf(dataVersion));
        }
    }

    /**
     * Adds a stream edge between a stream node and a task.
     *
     * @param task Task to write.
     * @param stream Stream to write.
     * @param isWrite Whether the task is writing or reading the stream.L
     */
    private void addStreamEdge(AbstractTask task, String stream, boolean isWrite) {
        // Add dependency
        String taskId = String.valueOf(task.getId());
        if (isWrite) {
            this.gm.addEdgeToGraph(taskId, stream, EdgeType.STREAM_DEPENDENCY, "");
        } else {
            this.gm.addEdgeToGraph(stream, taskId, EdgeType.STREAM_DEPENDENCY, "");
        }
    }

    /**
     * Addition of an edge from the commutative group to a task.
     *
     * @param dest Destination task.
     * @param dataId Id of the data causing the dependency.
     * @param dataVersion Version of the data causing the dependency.
     * @param cgt Commutative task group.
     * @param comToTask Whether the edge should be printed as a group to task or viceversa.
     */
    private void addEdgeFromCommutativeToTask(Task dest, int dataId, int dataVersion, CommutativeGroupTask cgt,
        boolean comToTask) {
        String src = String.valueOf(cgt.getCommutativeTasks().get(0).getId());
        String dst = String.valueOf(dest.getId());
        String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
        String comId = cgt.getCommutativeIdentifier().toString();
        if (comToTask) {
            this.gm.addEdgeToGraphFromGroup(src, dst, dep, comId, "clusterCommutative", EdgeType.DATA_DEPENDENCY);
        } else {
            this.gm.addEdgeToGraphFromGroup(dst, src, dep, comId, "clusterCommutative", EdgeType.DATA_DEPENDENCY);
        }
    }

    /**
     * We have explicitly called the barrier API. STEPS: Add a new synchronization node. Add an edge from last
     * synchronization point to barrier. Add edges from writer tasks to barrier.
     */
    private void addNewBarrier() {
        // Add barrier node
        int oldSync = this.synchronizationId;
        String oldSyncStr = "Synchro" + oldSync;

        // Add barrier node and edge from last sync
        this.synchronizationId++;
        String newSyncStr = "Synchro" + this.synchronizationId;
        this.gm.addBarrierToGraph(this.synchronizationId);
        this.gm.addEdgeToGraph(oldSyncStr, newSyncStr, EdgeType.USER_DEPENDENCY, "");

        // Reset task detection
        this.taskDetectedAfterSync = false;

        // Add edges from writers to barrier
        Set<AbstractTask> uniqueWriters = new HashSet<>();
        for (DataAccessesInfo dai : this.accessesInfo.values()) {
            if (dai != null) {
                // Add data writers
                List<AbstractTask> dataWriters = dai.getDataWriters();
                // Add stream writers
                uniqueWriters.addAll(dataWriters);
            }
        }
        for (AbstractTask writer : uniqueWriters) {
            if (writer != null && writer.getSynchronizationId() == oldSync) {
                String taskId = String.valueOf(writer.getId());
                this.gm.addEdgeToGraph(taskId, newSyncStr, EdgeType.USER_DEPENDENCY, "");
            }
        }
    }

    /**
     * We have explicitly called the barrier group API call. STEPS: Add a new synchronization node. Add an edge from
     * last synchronization point to barrier. Add edges from group tasks to barrier.
     *
     * @param barrier request causing the barrier
     */
    private void addNewGroupBarrierToGraph(BarrierGroupRequest barrier) {
        // Add barrier node
        int oldSync = this.synchronizationId;
        String oldSyncStr = "Synchro" + oldSync;

        // Add barrier node and edge from last sync
        this.synchronizationId++;
        String newSyncStr = "Synchro" + this.synchronizationId;
        this.gm.addBarrierToGraph(this.synchronizationId);
        this.gm.addEdgeToGraph(oldSyncStr, newSyncStr, EdgeType.USER_DEPENDENCY, "");

        // Reset task detection
        this.taskDetectedAfterSync = false;

        int groupsLastTaskID = barrier.getGraphSource();
        if (groupsLastTaskID > 0) {
            String src = String.valueOf(groupsLastTaskID);
            String groupName = barrier.getGroupName();
            this.gm.addEdgeToGraphFromGroup(src, newSyncStr, "", groupName, "clusterTasks", EdgeType.USER_DEPENDENCY);
        }
    }

}
