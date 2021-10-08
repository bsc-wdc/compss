/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.components.monitor.impl.EdgeType;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.components.monitor.impl.GraphHandler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.CommutativeIdentifier;
import es.bsc.compss.types.ReadersInfo;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.TaskGroup;
import es.bsc.compss.types.TaskListener;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.accesses.DataAccessesInfo;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.Direction;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.operation.ResultListener;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.BindingObjectParameter;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.DictCollectionParameter;
import es.bsc.compss.types.parameter.DirectoryParameter;
import es.bsc.compss.types.parameter.ExternalPSCOParameter;
import es.bsc.compss.types.parameter.ExternalStreamParameter;
import es.bsc.compss.types.parameter.FileParameter;
import es.bsc.compss.types.parameter.ObjectParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.parameter.StreamParameter;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.RegisterDataAccessRequest;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;

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
    private GraphGenerator gm;

    // Map: data Id -> WritersInfo
    private final Map<Integer, DataAccessesInfo> accessesInfo;
    // Tasks that are accessed commutatively. Map: data id -> commutative group tasks
    private Map<String, CommutativeGroupTask> commutativeGroup;
    // Tasks that are accessed commutatively and are pending to be drawn in graph. Map: commutative group identifier ->
    // list of tasks from group
    private Map<String, LinkedList<Task>> pendingToDrawCommutative;
    // List of submitted reduce tasks
    private List<String> reduceTasksNames;

    // Graph drawing
    private static final boolean IS_DRAW_GRAPH = GraphGenerator.isEnabled();
    private int synchronizationId;
    private boolean taskDetectedAfterSync;


    /**
     * Creates a new Task Analyzer instance.
     */
    public TaskAnalyser() {
        this.accessesInfo = new TreeMap<>();
        this.commutativeGroup = new TreeMap<>();
        this.pendingToDrawCommutative = new TreeMap<>();
        this.synchronizationId = 0;
        this.taskDetectedAfterSync = false;
        this.reduceTasksNames = new ArrayList<>();
        LOGGER.info("Initialization finished");
    }

    /**
     * Sets the TaskAnalyser co-workers.
     *
     * @param dip DataInfoProvider co-worker.
     */
    public void setCoWorkers(DataInfoProvider dip) {
        this.dip = dip;
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
            + "), ID = " + currentTask.getId());

        if (IS_DRAW_GRAPH) {
            addNewTask(currentTask);
        }

        Application app = currentTask.getApplication();
        app.newTask(currentTask);

        // Check scheduling enforcing data
        int constrainingParam = -1;
        if (description.getType() == TaskType.SERVICE && description.hasTargetObject()) {
            constrainingParam = description.getParameters().size() - 1 - description.getNumReturns();
        }

        // Add reduction task to reduce task list
        if (description.isReduction()) {
            this.reduceTasksNames.add(description.getName());
        }

        // Process parameters
        boolean taskHasEdge = processTaskParameters(app, currentTask, constrainingParam);
        registerIntermediateParameter(app, currentTask);
        markIntermediateParametersToDelete(app, currentTask);

        if (IS_DRAW_GRAPH && !taskHasEdge) {
            // If the graph must be written and the task has no edge due to its parameters,
            // add a direct dependency from last sync to task.
            addEdgeFromMainToTask(currentTask);
        }

    }

    private boolean processTaskParameters(Application app, Task currentTask, int constrainingParam) {
        List<Parameter> parameters = currentTask.getParameters();
        boolean taskHasEdge = false;
        for (int paramIdx = 0; paramIdx < parameters.size(); paramIdx++) {
            boolean isConstraining = paramIdx == constrainingParam;
            boolean paramHasEdge =
                registerParameterAccessAndAddDependencies(app, currentTask, parameters.get(paramIdx), isConstraining);
            taskHasEdge = taskHasEdge || paramHasEdge;
        }
        return taskHasEdge;
    }

    private void markIntermediateParametersToDelete(Application app, Task task) {
        for (Parameter p : task.getParameterDataToRemove()) {
            markParameterToDelete(app, p, true);
        }
    }

    private void registerIntermediateParameter(Application app, Task task) {
        for (Parameter p : task.getIntermediateParameters()) {
            registerParameterAccessAndAddDependencies(app, task, p, false);
        }
    }

    private void markParameterToDelete(Application app, Parameter p, boolean noReuse) {
        switch (p.getType()) {
            case DIRECTORY_T:
                DirectoryParameter dp = (DirectoryParameter) p;
                dip.deleteData(app, dp.getLocation(), noReuse);
                break;
            case FILE_T:
                FileParameter fp = (FileParameter) p;
                dip.deleteData(app, fp.getLocation(), noReuse);
                break;
            case OBJECT_T:
            case PSCO_T:
                ObjectParameter op = (ObjectParameter) p;
                dip.deleteData(op.getCode(), noReuse);
                break;
            case EXTERNAL_PSCO_T:
                ExternalPSCOParameter epscop = (ExternalPSCOParameter) p;
                dip.deleteData(epscop.getCode(), noReuse);
                break;
            case BINDING_OBJECT_T:
                BindingObjectParameter bindingObjectparam = (BindingObjectParameter) p;
                dip.deleteData(bindingObjectparam.getCode(), noReuse);
                break;
            case STREAM_T:
                StreamParameter sp = (StreamParameter) p;
                dip.deleteData(sp.getCode(), noReuse);
                break;
            case EXTERNAL_STREAM_T:
                ExternalStreamParameter esp = (ExternalStreamParameter) p;
                dip.deleteData(app, esp.getLocation(), noReuse);
                break;
            case COLLECTION_T:
                CollectionParameter cp = (CollectionParameter) p;
                dip.deleteCollection(cp.getCollectionId(), true);
                break;
            case DICT_COLLECTION_T:
                DictCollectionParameter dcp = (DictCollectionParameter) p;
                dip.deleteDictCollection(dcp.getDictCollectionId(), true);
                break;
            default:
                // This is a basic type nothing to delete
        }

    }

    /**
     * Registers a data access from the main code and notifies when the data is available.
     *
     * @param rdar request indicating the data being accessed
     * @return The registered access Id.
     */
    public DataAccessId processMainAccess(RegisterDataAccessRequest rdar) {
        AccessParams access = rdar.getAccessParams();
        if (DEBUG) {
            LOGGER.debug("Registering access " + access.toString() + " from main code");
        }
        DataAccessId daId = dip.registerDataAccess(access);
        if (DEBUG) {
            LOGGER.debug("Registered access to data " + daId.getDataId() + " from main code");
        }

        if (access.getMode() != AccessMode.W) {
            int dataId = daId.getDataId();
            // Retrieve writers information
            DataAccessesInfo dai = this.accessesInfo.get(dataId);
            if (dai != null) {
                int dataVersion = 0;
                if (IS_DRAW_GRAPH) {
                    TreeSet<Integer> toPass = new TreeSet<>();
                    toPass.add(dataId);
                    DataInstanceId dii = this.dip.getLastVersions(toPass).get(0);
                    dataVersion = dii.getVersionId();
                }
                dai.mainAccess(rdar, this, dataId, dataVersion);
            }
        }
        return daId;
    }

    /**
     * Registers the end of execution of task @{code task}.
     *
     * @param aTask Ended task.
     */
    public void endTask(AbstractTask aTask) {
        int taskId = aTask.getId();
        long start = System.currentTimeMillis();
        if (aTask instanceof Task) {
            Task task = (Task) aTask;
            boolean isFree = task.isFree();
            TaskState taskState = task.getStatus();
            OnFailure onFailure = task.getOnFailure();
            LOGGER.info("Notification received for task " + taskId + " with end status " + taskState);
            // Check status
            if (!isFree) {
                LOGGER.debug("Task " + taskId + " is not registered as free. Waiting for other executions to end");
                return;
            }
            TaskMonitor registeredMonitor = task.getTaskMonitor();
            switch (taskState) {
                case FAILED:
                    registeredMonitor.onFailure();
                    if (onFailure == OnFailure.RETRY || onFailure == OnFailure.FAIL) {
                        ErrorManager.error(TASK_FAILED + task);
                        return;
                    }
                    if (onFailure == OnFailure.IGNORE || onFailure == OnFailure.CANCEL_SUCCESSORS) {
                        // Show warning
                        ErrorManager.warn(TASK_FAILED + task);
                    }
                    break;
                case CANCELED:
                    registeredMonitor.onCancellation();

                    // Show warning
                    ErrorManager.warn(TASK_CANCELED + task);
                    break;
                default:
                    registeredMonitor.onCompletion();
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

            // Check if the finished task was the last writer of a file, but only if task generation has finished
            // Task generation is finished if we are on noMoreTasks but we are not on a barrier
            if (DEBUG) {
                LOGGER.debug("Checking result file transfers for task " + taskId);
            }

            Application app = task.getApplication();
            if (app.isEnding()) {
                checkResultFileTransfer(task);
            }

            // Release task groups of the task
            app.endTask(task);

            // Releases commutative groups dependent and releases all the waiting tasks
            releaseCommutativeGroups(task);
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
     * Barrier.
     *
     * @param request Barrier request.
     */
    public void barrier(BarrierRequest request) {
        Application app = request.getApp();
        if (IS_DRAW_GRAPH) {
            // Addition of missing commutative groups to graph
            addMissingCommutativeTasksToGraph();
            addNewBarrier();

            // We can draw the graph on a barrier while we wait for tasks
            this.gm.commitGraph();
        }

        app.reachesBarrier(request);

    }

    /**
     * End of execution barrier.
     *
     * @param request End of execution request.
     */
    public void noMoreTasks(EndOfAppRequest request) {
        if (IS_DRAW_GRAPH) {
            addMissingCommutativeTasksToGraph();
            this.gm.commitGraph();
        }
        Application app = request.getApp();
        app.endReached(request);
    }

    /**
     * Deletes the specified data and its renamings.
     *
     * @param dataInfo DataInfo.
     */
    public void deleteData(DataInfo dataInfo) {
        int dataId = dataInfo.getDataId();
        LOGGER.info("Deleting data " + dataId);
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
        TaskGroup tg = app.stackTaskGroup(groupName);
        if (IS_DRAW_GRAPH) {
            this.gm.addTaskGroupToGraph(tg.getName());
            LOGGER.debug("Group " + groupName + " added to graph");
            tg.setGraphDrawn();
        }
    }

    /**
     * Closes the last task group of an application.
     *
     * @param app Application to which the group belongs to
     */
    public void closeCurrentTaskGroup(Application app) {
        TaskGroup tg = app.popGroup();
        if (IS_DRAW_GRAPH) {
            this.gm.closeGroupInGraph();
        }
    }

    private void releaseCommutativeGroups(Task task) {
        if (!task.getCommutativeGroupList().isEmpty()) {
            for (CommutativeGroupTask group : task.getCommutativeGroupList()) {
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

    /**
     * Barrier for group.
     *
     * @param request Barrier group request
     */
    public void barrierGroup(BarrierGroupRequest request) {
        Application app = request.getApp();
        String groupName = request.getGroupName();

        TaskGroup tg = app.getGroup(groupName);
        if (tg != null) {
            // Addition of missing commutative groups to graph
            if (IS_DRAW_GRAPH) {
                addMissingCommutativeTasksToGraph();
                addNewGroupBarrier(tg);
                // We can draw the graph on a barrier while we wait for tasks
                this.gm.commitGraph();
            }
        }
        app.reachesGroupBarrier(tg, request);
    }

    private void addMissingCommutativeTasksToGraph() {
        LinkedList<String> identifiers = new LinkedList<>();
        for (String identifier : this.pendingToDrawCommutative.keySet()) {
            addCommutativeGroupTaskToGraph(identifier);
            identifiers.add(identifier);
        }
        for (String identifier : identifiers) {
            this.pendingToDrawCommutative.remove(identifier);
        }
    }

    /*
     * *************************************************************************************************************
     * DATA DEPENDENCY MANAGEMENT PRIVATE METHODS
     ***************************************************************************************************************/
    private boolean registerParameterAccessAndAddDependencies(Application app, Task currentTask, Parameter p,
        boolean isConstraining) {
        // Conversion: direction -> access mode
        AccessMode am = AccessMode.R;
        switch (p.getDirection()) {
            case IN:
            case IN_DELETE:
                am = AccessMode.R;
                break;
            case OUT:
                am = AccessMode.W;
                break;
            case INOUT:
                am = AccessMode.RW;
                break;
            case CONCURRENT:
                am = AccessMode.C;
                break;
            case COMMUTATIVE:
                am = AccessMode.CV;
                break;
        }

        ReadersInfo readerData = new ReadersInfo(p, currentTask);

        // First DataAccess registered on a commutative group
        DataAccessId firstRegistered = null;

        // Inform the Data Manager about the new accesses
        boolean hasParamEdge = false;
        DataAccessId daId;
        switch (p.getType()) {
            case DIRECTORY_T:
                DirectoryParameter dp = (DirectoryParameter) p;
                // register file access for now, and directory will be accessed as a file
                daId = this.dip.registerFileAccess(app, am, dp.getLocation(), readerData);
                break;
            case FILE_T:
                FileParameter fp = (FileParameter) p;
                daId = this.dip.registerFileAccess(app, am, fp.getLocation(), readerData);
                break;
            case PSCO_T:
                ObjectParameter pscop = (ObjectParameter) p;
                // Check if its PSCO class and persisted to infer its type
                pscop.setType(DataType.PSCO_T);
                daId = this.dip.registerObjectAccess(app, am, pscop.getValue(), pscop.getCode(), readerData);
                break;
            case EXTERNAL_PSCO_T:
                ExternalPSCOParameter externalPSCOparam = (ExternalPSCOParameter) p;
                // Check if its PSCO class and persisted to infer its type
                externalPSCOparam.setType(DataType.EXTERNAL_PSCO_T);
                daId = dip.registerExternalPSCOAccess(app, am, externalPSCOparam.getId(), externalPSCOparam.getCode(),
                    readerData);
                break;
            case BINDING_OBJECT_T:
                BindingObjectParameter bindingObjectparam = (BindingObjectParameter) p;
                // Check if its Binding OBJ and register its access
                bindingObjectparam.setType(DataType.BINDING_OBJECT_T);
                daId = dip.registerBindingObjectAccess(app, am, bindingObjectparam.getBindingObject(),
                    bindingObjectparam.getCode(), readerData);
                break;
            case OBJECT_T:
                ObjectParameter op = (ObjectParameter) p;
                // Check if its PSCO class and persisted to infer its type
                if (op.getValue() instanceof StubItf && ((StubItf) op.getValue()).getID() != null) {
                    op.setType(DataType.PSCO_T);
                }
                daId = this.dip.registerObjectAccess(app, am, op.getValue(), op.getCode(), readerData);
                break;
            case STREAM_T:
                StreamParameter sp = (StreamParameter) p;
                daId = this.dip.registerStreamAccess(app, am, sp.getValue(), sp.getCode(), readerData);
                break;
            case EXTERNAL_STREAM_T:
                ExternalStreamParameter esp = (ExternalStreamParameter) p;
                daId = this.dip.registerExternalStreamAccess(app, am, esp.getLocation(), readerData);
                break;
            case COLLECTION_T:
                CollectionParameter cp = (CollectionParameter) p;
                for (Parameter content : cp.getParameters()) {
                    boolean hasCollectionParamEdge =
                        registerParameterAccessAndAddDependencies(app, currentTask, content, isConstraining);
                    hasParamEdge = hasParamEdge || hasCollectionParamEdge;
                }
                daId = dip.registerCollectionAccess(app, am, cp, readerData);
                DataInfo ci = dip.deleteCollection(cp.getCollectionId(), true);
                deleteData(ci);
                break;
            case DICT_COLLECTION_T:
                DictCollectionParameter dcp = (DictCollectionParameter) p;
                for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                    boolean hasDictCollectionParamEdgeKey =
                        registerParameterAccessAndAddDependencies(app, currentTask, entry.getKey(), isConstraining);
                    boolean hasDictCollectionParamEdgeValue =
                        registerParameterAccessAndAddDependencies(app, currentTask, entry.getValue(), isConstraining);
                    hasParamEdge = hasParamEdge || hasDictCollectionParamEdgeKey || hasDictCollectionParamEdgeValue;
                }
                daId = dip.registerDictCollectionAccess(app, am, dcp, readerData);
                DataInfo dci = dip.deleteDictCollection(dcp.getDictCollectionId(), true);
                deleteData(dci);
                break;
            default:
                // This is a basic type, there are no accesses to register
                daId = null;
                currentTask.registerFreeParam(p);
        }

        if (daId != null) {
            // Add parameter dependencies
            DependencyParameter dp = (DependencyParameter) p;
            if (am == AccessMode.CV) {
                // Register commutative access
                Integer coreId = currentTask.getTaskDescription().getCoreElement().getCoreId();
                CommutativeIdentifier comId = new CommutativeIdentifier(coreId, daId.getDataId());
                CommutativeGroupTask com = null;
                for (CommutativeGroupTask cgt : this.commutativeGroup.values()) {
                    if (cgt.getCommutativeIdentifier().compareTo(comId) == 1) {
                        com = cgt;
                    }
                }
                if (com == null) {
                    firstRegistered = daId;
                    LOGGER.debug(
                        "The FIRST registered daId in the commutative group " + comId.toString() + " is " + daId);
                } else {
                    com.addVersionToList(daId);
                    daId = com.getRegisteredVersion();
                    LOGGER.debug("Registering daId " + daId + " in commutative group " + comId.toString());
                }

                dp.setDataAccessId(daId);
                hasParamEdge = addCommutativeDependencies(currentTask, dp, firstRegistered, coreId);
            } else {
                // Register regular access
                dp.setDataAccessId(daId);
                hasParamEdge = addDependencies(am, currentTask, isConstraining, dp);
            }
        } else {
            // Basic types do not produce access dependencies
        }
        // Return data Id
        return hasParamEdge;
    }

    private boolean addDependencies(AccessMode am, Task currentTask, boolean isConstraining, DependencyParameter dp) {

        // Add dependencies to the graph and register output values for future dependencies
        boolean hasParamEdge = false;
        DataAccessId daId = dp.getDataAccessId();
        int dataId = daId.getDataId();
        DataAccessesInfo dai = this.accessesInfo.get(dataId);
        switch (am) {
            case R:
                checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                hasParamEdge = true;
                break;
            case RW:
                checkInputDependency(currentTask, dp, false, dataId, dai, isConstraining);
                hasParamEdge = true;
                registerOutputValues(currentTask, dp, false, dai);
                break;
            case W:
                // Register output values
                registerOutputValues(currentTask, dp, false, dai);
                break;
            case C:
                checkInputDependency(currentTask, dp, true, dataId, dai, isConstraining);
                hasParamEdge = true;
                registerOutputValues(currentTask, dp, true, dai);
                break;
            case CV:
                // Commutative accesses are processed in addCommutativeDependencies
                break;
        }
        return hasParamEdge;
    }

    private void checkInputDependency(Task currentTask, DependencyParameter dp, boolean isConcurrent, int dataId,
        DataAccessesInfo dai, boolean isConstraining) {
        if (DEBUG) {
            LOGGER.debug("Checking READ dependency for datum " + dataId + " and task " + currentTask.getId());
        }
        if (dai != null) {
            dai.readValue(currentTask, dp, isConcurrent, this);
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
            if (IS_DRAW_GRAPH) {
                // Add edge from last sync point to task
                drawEdges(currentTask, dp, null);
            }
        }
    }

    /**
     * Registers the output values of the task {@code currentTask}.
     *
     * @param currentTask Task.
     * @param dp Dependency Parameter.
     * @param isConcurrent data access was done in concurrent mode
     * @param dai AccessInfo related to the data being accessed
     */
    private void registerOutputValues(AbstractTask currentTask, DependencyParameter dp, boolean isConcurrent,
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

    private boolean addCommutativeDependencies(Task currentTask, DependencyParameter dp, DataAccessId firstRegistered,
        int coreId) {

        // Add dependencies to the graph and register output values for future dependencies
        DataAccessId daId = dp.getDataAccessId();
        CommutativeIdentifier comId = new CommutativeIdentifier(coreId, daId.getDataId());
        CommutativeGroupTask com = null;
        LinkedList<Task> pendingToDraw = null;
        for (CommutativeGroupTask cgt : this.commutativeGroup.values()) {
            if (cgt.getCommutativeIdentifier().compareTo(comId) == 1) {
                com = cgt;
                if (IS_DRAW_GRAPH) {
                    pendingToDraw = this.pendingToDrawCommutative.get(comId.toString());
                }
            }
        }
        if (IS_DRAW_GRAPH) {
            if (pendingToDraw == null) {
                pendingToDraw = new LinkedList<>();
            }
            pendingToDraw.add(currentTask);
            this.pendingToDrawCommutative.put(comId.toString(), pendingToDraw);
        }

        DataAccessesInfo dai = this.accessesInfo.get(daId.getDataId());
        if (com == null) {
            LOGGER.info("Creating a new commutative group " + comId);
            com = new CommutativeGroupTask(currentTask.getApplication(), comId);

            if (IS_DRAW_GRAPH) {
                LOGGER.debug("Checking if previous group in graph");
                checkIfPreviousGroupInGraph(daId.getDataId());
            }
            if (dai != null) {
                List<AbstractTask> predecessors = dai.getDataWriters();
                AbstractTask predecessor = null;
                if (!predecessors.isEmpty()) {
                    predecessor = predecessors.get(0);
                }
                com.setParentDataDependency(predecessor);
                LOGGER.debug("Setting parent data dependency");
            }
            this.commutativeGroup.put(comId.toString(), com);
            com.setRegisteredVersion(firstRegistered);
        }

        com.setFinalVersion(((RWAccessId) daId).getWVersionId());
        boolean hasParamEdge = checkDependencyForCommutative(currentTask, dp, com);

        registerOutputValues(com, dp, false, dai);

        return hasParamEdge;
    }

    private boolean checkDependencyForCommutative(Task currentTask, DependencyParameter dp,
        CommutativeGroupTask commutativeGroup) {

        // Addition of a dependency to the task which generates commutative data
        AbstractTask t = commutativeGroup.getParentDataDependency();
        if (t != null) {
            LOGGER.debug("Adding dependency with parent task of commutative group");
            currentTask.addDataDependency(t, dp);
        }
        if (IS_DRAW_GRAPH) {
            drawEdges(currentTask, dp, t);
        }

        // Addition of a dependency between the task and the commutative group
        commutativeGroup.addDataDependency(currentTask, dp);
        commutativeGroup.addCommutativeTask(currentTask);
        currentTask.setCommutativeGroup(commutativeGroup, dp.getDataAccessId());

        // A commutative dependency is always written in the task graph
        return true;
    }

    private void updateLastWritters(AbstractTask task, Parameter p) {
        DataType type = p.getType();
        int currentTaskId = task.getId();
        if (type == DataType.COLLECTION_T) {
            CollectionParameter cp = (CollectionParameter) p;
            for (Parameter sp : cp.getParameters()) {
                updateLastWritters(task, sp);
            }
        }
        if (type == DataType.DICT_COLLECTION_T) {
            DictCollectionParameter dcp = (DictCollectionParameter) p;
            for (Map.Entry<Parameter, Parameter> entry : dcp.getParameters().entrySet()) {
                updateLastWritters(task, entry.getKey());
                updateLastWritters(task, entry.getValue());
            }
        }
        if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T || type == DataType.COLLECTION_T
            || type == DataType.DICT_COLLECTION_T) {
            DependencyParameter dp = (DependencyParameter) p;
            int dataId = dp.getDataAccessId().getDataId();
            if (DEBUG) {
                LOGGER.debug("Removing writters info for datum " + dataId + " and task " + currentTaskId);
            }
            DataAccessesInfo dai = this.accessesInfo.get(dataId);
            if (dai != null) {
                switch (dp.getDirection()) {
                    case OUT:
                    case INOUT:
                        dai.completedProducer(task);
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void updateParameterAccess(Task t, Parameter p) {
        DataType type = p.getType();

        if (type == DataType.COLLECTION_T) {
            for (Parameter subParam : ((CollectionParameter) p).getParameters()) {
                updateParameterAccess(t, subParam);
            }
        }

        if (type == DataType.DICT_COLLECTION_T) {
            for (Map.Entry<Parameter, Parameter> entry : ((DictCollectionParameter) p).getParameters().entrySet()) {
                updateParameterAccess(t, entry.getKey());
                updateParameterAccess(t, entry.getValue());
            }
        }

        if (type == DataType.FILE_T || type == DataType.DIRECTORY_T || type == DataType.OBJECT_T
            || type == DataType.PSCO_T || type == DataType.STREAM_T || type == DataType.EXTERNAL_STREAM_T
            || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T || type == DataType.COLLECTION_T
            || type == DataType.DICT_COLLECTION_T) {

            DependencyParameter dPar = (DependencyParameter) p;
            DataAccessId dAccId = dPar.getDataAccessId();
            if (DEBUG) {
                LOGGER.debug("Treating that data " + dAccId + " has been accessed at " + dPar.getDataTarget());
            }

            ReadersInfo readerData = new ReadersInfo(p, t);
            if (t.getOnFailure() == OnFailure.CANCEL_SUCCESSORS
                && (t.getStatus() == TaskState.FAILED || t.getStatus() == TaskState.CANCELED)
                || t.isCancelledByException()) {
                this.dip.dataAccessHasBeenCanceled(dAccId, readerData);
            } else {
                this.dip.dataHasBeenAccessed(dAccId);
            }
        }
    }

    private void checkResultFileTransfer(Task t) {
        LinkedList<DataInstanceId> fileIds = new LinkedList<>();
        for (Parameter p : t.getTaskDescription().getParameters()) {
            DataType type = p.getType();
            if (type == DataType.DIRECTORY_T || type == DataType.FILE_T) {
                DependencyParameter fp = (DependencyParameter) p;
                DataInstanceId dId = null;
                switch (fp.getDirection()) {
                    case COMMUTATIVE:
                    case INOUT:
                        dId = ((RWAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                        break;
                    case OUT:
                        dId = ((WAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                        break;
                    default:
                        break;
                }
                if (dId != null) {
                    DataAccessesInfo dai = this.accessesInfo.get(dId.getDataId());
                    if (dai != null) {
                        if (dai.isFinalProducer(t)) {
                            fileIds.add(dId);
                        }
                    }
                }
            }
        }

        // Order the transfer of the result files
        final int numFT = fileIds.size();
        if (numFT > 0) {
            if (DEBUG) {
                LOGGER.debug("Ordering transfers for result files of task " + t.getId());
            }
            for (DataInstanceId fileId : fileIds) {
                int id = fileId.getDataId();
                if (DEBUG) {
                    LOGGER.debug("- Requesting result file " + id + " because of task " + t.getId());
                }
                this.dip.blockDataAndGetResultFile(id, new ResultListener(new Semaphore(0)));
                this.dip.unblockDataId(id);
            }
        }
    }

    /*
     **************************************************************************************************************
     * GRAPH WRAPPERS
     **************************************************************************************************************/
    @Override
    public void drawEdges(Task currentTask, DependencyParameter dp, AbstractTask lastWriter) {
        // Retrieve common information
        int dataId = dp.getDataAccessId().getDataId();
        Direction d = dp.getDataAccessId().getDirection();
        int dataVersion;
        switch (d) {
            case C:
            case R:
                dataVersion = ((RAccessId) dp.getDataAccessId()).getRVersionId();
                break;
            case W:
                dataVersion = ((WAccessId) dp.getDataAccessId()).getWVersionId();
                break;
            default:
                dataVersion = ((RWAccessId) dp.getDataAccessId()).getRVersionId();
                break;
        }

        // Add edges on graph depending on the dependency type
        switch (dp.getType()) {
            case STREAM_T:
            case EXTERNAL_STREAM_T:
                drawStreamEdge(currentTask, dp, !d.equals(Direction.R));
                break;
            default:
                if (lastWriter != null && lastWriter != currentTask) {
                    if (lastWriter instanceof Task) {
                        if (lastWriter.getSuccessors().contains(currentTask.getCommutativeGroup(dataId))) {
                            addEdgeFromCommutativeToTask(currentTask, dataId, dataVersion,
                                ((CommutativeGroupTask) lastWriter), false);
                        } else {
                            addDataEdgeFromTaskToTask((Task) lastWriter, currentTask, dataId, dataVersion);
                        }
                    } else {
                        if (!(lastWriter instanceof Task && !currentTask.hasCommutativeParams())) {
                            addEdgeFromCommutativeToTask(currentTask, dataId, dataVersion,
                                ((CommutativeGroupTask) lastWriter), true);
                        }
                    }
                } else {
                    addDataEdgeFromMainToTask(currentTask, dataId, dataVersion);
                }
                break;
        }
    }

    @Override
    public void drawStreamEdge(AbstractTask currentTask, DependencyParameter dp, boolean isWrite) {
        String stream = "Stream" + dp.getDataAccessId().getDataId();

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
        if (task.hasCommutativeParams()) {
            // In case task has commutative params, it will be added to graph with the group
        } else {
            // Add node to graph
            addTaskToGraph(task);
        }
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

    @Override
    public void checkIfPreviousGroupInGraph(int dataId) {
        DataAccessesInfo dai = this.accessesInfo.get(dataId);
        if (dai != null) {
            List<AbstractTask> lastWriters = dai.getDataWriters();
            AbstractTask lastWriter = null;
            if (!lastWriters.isEmpty()) {
                lastWriter = lastWriters.get(0);
            }

            if (lastWriter instanceof CommutativeGroupTask && !((CommutativeGroupTask) lastWriter).getGraphDrawn()) {
                CommutativeIdentifier comId = ((CommutativeGroupTask) lastWriter).getCommutativeIdentifier();
                // Adds the group to the graph and removes task from pendingToDraw
                addCommutativeGroupTaskToGraph(comId.toString());
                ((CommutativeGroupTask) lastWriter).setGraphDrawn();
                this.pendingToDrawCommutative
                    .remove(((CommutativeGroupTask) lastWriter).getCommutativeIdentifier().toString());
            }
        }
    }

    /**
     * Puts a new commutative group to the graph.
     *
     * @param identifier Commutative group Id.
     */
    private void addCommutativeGroupTaskToGraph(String identifier) {
        LOGGER.debug("Adding commutative group to graph");
        this.gm.addCommutativeGroupToGraph(identifier);
        for (Task t : this.pendingToDrawCommutative.get(identifier)) {
            addTaskToGraph(t);
        }
        this.gm.closeGroupInGraph();
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
    public void addEdgeFromTaskToMain(AbstractTask task, EdgeType edgeType, int dataId, int dataVersion) {
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
     * @param tg Name of the group.
     */
    private void addNewGroupBarrier(TaskGroup tg) {
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

        String src = String.valueOf(tg.getLastTaskId());
        tg.setBarrierDrawn();
        if (!tg.hasPendingTasks() && tg.isClosed() && tg.hasBarrier()) {
            Application app = tg.getApp();
            app.removeGroup(tg.getName());
        }
        this.gm.addEdgeToGraphFromGroup(src, newSyncStr, "", tg.getName(), "clusterTasks", EdgeType.USER_DEPENDENCY);
    }

}
