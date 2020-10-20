/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.CommutativeGroupTask;
import es.bsc.compss.types.CommutativeIdentifier;
import es.bsc.compss.types.ReadersInfo;
import es.bsc.compss.types.ReduceTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.TaskGroup;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.Direction;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
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
import es.bsc.compss.types.request.ap.WaitForConcurrentRequest;
import es.bsc.compss.types.request.ap.WaitForTaskRequest;
import es.bsc.compss.util.ErrorManager;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashSet;
import java.util.Hashtable;
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
public class TaskAnalyser {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String TASK_FAILED = "Task failed: ";
    private static final String TASK_CANCELED = "Task canceled: ";

    // Components
    private DataInfoProvider dip;
    private GraphGenerator gm;

    // Map: data Id -> WritersInfo
    private Map<Integer, WritersInfo> writers;
    // Tasks being waited on: taskId -> list of semaphores where to notify end of task
    private Hashtable<AbstractTask, List<Semaphore>> waitedTasks;
    // Concurrent tasks being waited on: taskId -> semaphore where to notify end of task
    private Map<Integer, List<Task>> concurrentAccessMap;
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
        this.writers = new TreeMap<>();

        this.waitedTasks = new Hashtable<>();
        this.concurrentAccessMap = new TreeMap<>();
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
        TaskDescription params = currentTask.getTaskDescription();
        LOGGER.info("New " + (params.getType() == TaskType.METHOD ? "method" : "service") + " task(" + params.getName()
            + "), ID = " + currentTask.getId());

        if (IS_DRAW_GRAPH) {
            addNewTask(currentTask);
        }

        Application app = currentTask.getApplication();
        app.newTask(currentTask);

        // Check scheduling enforcing data
        int constrainingParam = -1;
        if (params.getType() == TaskType.SERVICE && params.hasTargetObject()) {
            constrainingParam = params.getParameters().size() - 1 - params.getNumReturns();
        }

        // Add task to the groups
        for (TaskGroup group : app.getCurrentGroups()) {
            currentTask.addTaskGroup(group);
            group.addTask(currentTask);
        }

        // Process parameters
        List<Parameter> parameters = params.getParameters();
        boolean taskHasEdge = false;
        for (int paramIdx = 0; paramIdx < parameters.size(); paramIdx++) {
            boolean isConstraining = paramIdx == constrainingParam;
            boolean paramHasEdge =
                registerParameterAccessAndAddDependencies(app, currentTask, parameters.get(paramIdx), isConstraining);
            taskHasEdge = taskHasEdge || paramHasEdge;
        }

        if (currentTask instanceof ReduceTask) {
            // If it a reduce task. It is dynamically decomposed in a set of partial reduce tasks.
            // These tasks are using IN and OUT temporal collection parameters.
            // In this part of code we register this parameters in order to be managed when tasks are finishing.
            this.reduceTasksNames.add(((ReduceTask) currentTask).getTaskDescription().getName());
            // Register partial output parameters
            List<Parameter> reducePartialsOut = ((ReduceTask) currentTask).getIntermediateOutParameters();
            for (int paramIdx = 0; paramIdx < reducePartialsOut.size(); paramIdx++) {
                boolean isConstraining = paramIdx == constrainingParam;
                registerParameterAccessAndAddDependencies(app, currentTask, reducePartialsOut.get(paramIdx),
                    isConstraining);
            }
            // Register partial tasks input parameters
            List<Parameter> reducePartialsIn = ((ReduceTask) currentTask).getIntermediateInParameters();
            for (int paramIdx = 0; paramIdx < reducePartialsIn.size(); paramIdx++) {
                boolean isConstraining = paramIdx == constrainingParam;
                registerParameterAccessAndAddDependencies(app, currentTask, reducePartialsIn.get(paramIdx),
                    isConstraining);
            }
            // Register partial task collections
            List<CollectionParameter> reduceCollections = ((ReduceTask) currentTask).getIntermediateCollections();
            for (int paramIdx = 0; paramIdx < reduceCollections.size(); paramIdx++) {
                boolean isConstraining = paramIdx == constrainingParam;
                registerParameterAccessAndAddDependencies(app, currentTask, reduceCollections.get(paramIdx),
                    isConstraining);
            }
            // Final reduce task collection
            CollectionParameter finalCollection = ((ReduceTask) currentTask).getFinalCollection();
            boolean isConstraining = false;
            registerParameterAccessAndAddDependencies(app, currentTask, finalCollection, isConstraining);
        }

        if (IS_DRAW_GRAPH) {
            if (!taskHasEdge) {
                // If the graph must be written and the task has no edge due to its parameters,
                // add a direct dependency from last sync to task.
                addEdgeFromMainToTask(currentTask);
            }
        }
    }

    /**
     * Returns the tasks dependent to the requested task.
     *
     * @param request Requested task.
     */
    public void findWaitedTask(WaitForTaskRequest request) {
        int dataId = request.getDataId();
        AccessMode am = request.getAccessMode();
        Semaphore sem = request.getSemaphore();

        // Retrieve writers information
        WritersInfo wi = this.writers.get(dataId);
        if (wi != null) {
            switch (wi.getDataType()) {
                case STREAM_T:
                case EXTERNAL_STREAM_T:
                    // Mark the data accesses
                    List<AbstractTask> lastStreamWriters = wi.getStreamWriters();
                    for (AbstractTask lastWriter : lastStreamWriters) {
                        treatDataAccess(lastWriter, am, dataId);
                    }
                    // We do not wait for stream task to complete
                    sem.release();
                    break;
                default:
                    // Retrieve last writer task
                    AbstractTask lastWriter = wi.getDataWriter();
                    // Mark the data access
                    if (lastWriter != null) {
                        treatDataAccess(lastWriter, am, dataId);
                    }
                    // Release task if possible. Otherwise add to waiting
                    if (lastWriter == null || lastWriter.getStatus() == TaskState.FINISHED
                        || lastWriter.getStatus() == TaskState.CANCELED || lastWriter.getStatus() == TaskState.FAILED) {
                        sem.release();
                    } else {
                        List<Semaphore> list = this.waitedTasks.get(lastWriter);
                        if (list == null) {
                            list = new LinkedList<>();
                        }
                        list.add(sem);
                        this.waitedTasks.put(lastWriter, list);
                    }
                    break;
            }
        } else {
            // No writer registered, release
            sem.release();
        }
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
            List<Semaphore> sems = this.waitedTasks.remove(task);
            if (sems != null) {
                for (Semaphore sem : sems) {
                    sem.release();
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

            if (task instanceof ReduceTask) {
                // When a reduce task end we have to "deregister/remove" not used parameters
                List<Parameter> paramList = ((ReduceTask) task).getUnusedParameters();
                for (Parameter param : paramList) {
                    updateParameterAccess(task, param);
                    updateLastWritters(task, param);
                }
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
            releaseTaskGroups(task);

            // Releases commutative groups dependent and releases all the waiting tasks
            releaseCommutativeGroups(task);
        }
        // Release data dependent tasks
        if (DEBUG) {
            LOGGER.debug("Releasing data dependant tasks for task " + taskId);
        }

        // Release data dependent tasks. If it a partial reduce, it does not have to release data dependent tasks.
        boolean successorIsReduce = false;
        for (AbstractTask t : aTask.getSuccessors()) {
            if (this.reduceTasksNames.contains(((Task) t).getTaskDescription().getName())) {
                successorIsReduce = true;
            }
        }
        if (!successorIsReduce) {
            aTask.releaseDataDependents();
        }
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
        if (IS_DRAW_GRAPH) {
            // Addition of missing commutative groups to graph
            addMissingCommutativeTasksToGraph();
            addNewBarrier();

            // We can draw the graph on a barrier while we wait for tasks
            this.gm.commitGraph();
        }

        Application app = request.getApp();
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
        WritersInfo wi = this.writers.remove(dataId);
        if (wi != null) {
            switch (wi.getDataType()) {
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
     * Removes a given group from an application.
     *
     * @param app Application to which the group to be cancelled belongs
     * @param groupName name of the group to be cancelled
     * @return the group to be cancelled
     */
    public TaskGroup removeTaskGroup(Application app, String groupName) {
        TaskGroup tg = app.removeGroup(groupName);
        return tg;
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
     * ********************************************************************************************************
     * CONCURRENT PUBLIC METHODS
     **********************************************************************************************************/

    /**
     * Check whether a dataId is of type concurrent or not.
     *
     * @param daId {@code true} if the dataId is concurrent, {@code false} otherwise.
     */
    public boolean dataWasAccessedConcurrent(int daId) {
        List<Task> concurrentAccess = this.concurrentAccessMap.get(daId);
        if (concurrentAccess != null) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Returns the concurrent tasks dependent to the requested task.
     *
     * @param request Requested task.
     */
    public void findWaitedConcurrent(WaitForConcurrentRequest request) {
        int dataId = request.getDataId();
        AccessMode am = request.getAccessMode();
        List<Task> concurrentAccess = this.concurrentAccessMap.get(dataId);
        if (concurrentAccess != null) {
            // Add to writers if needed
            this.concurrentAccessMap.put(dataId, null);
        }

        Semaphore semTasks = request.getTaskSemaphore();
        int n = 0;
        for (Task task : concurrentAccess) {
            treatDataAccess(task, am, dataId);
            if (task.getStatus() != TaskState.FINISHED) {
                n++;
                List<Semaphore> list = this.waitedTasks.get(task);
                if (list == null) {
                    list = new LinkedList<>();
                }
                list.add(semTasks);
                this.waitedTasks.put(task, list);
            }
        }
        request.setNumWaitedTasks(n);
        request.getSemaphore().release();
    }

    /**
     * Removes the tasks that have accessed the data in a concurrent way.
     *
     * @param dataId Data Id.
     */
    public void removeFromConcurrentAccess(int dataId) {
        List<Task> returnedValue = this.concurrentAccessMap.remove(dataId);
        if (returnedValue == null) {
            LOGGER.debug("The concurrent list could not be removed");
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
        tg.setClosed();
        if (IS_DRAW_GRAPH) {
            this.gm.closeGroupInGraph();
        }
    }

    private void releaseTaskGroups(Task task) {
        for (TaskGroup group : task.getTaskGroupList()) {
            group.removeTask(task);
            LOGGER.debug("Group " + group.getName() + " released task " + task.getId());
            if (!group.hasPendingTasks() && group.hasBarrier()) {
                group.releaseBarrier();
                if (group.getBarrierDrawn()) {
                    task.getApplication().removeGroup(group.getName());
                    LOGGER.debug("All tasks of group " + group.getName() + " have finished execution");
                }
                LOGGER.debug("All tasks of group " + group.getName() + " have finished execution");
            }
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
                    List<Semaphore> sems = this.waitedTasks.remove(group);
                    if (sems != null) {
                        for (Semaphore sem : sems) {
                            sem.release();
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
        switch (am) {
            case R:
                if (!dataWasAccessedConcurrent(daId.getDataId())) {
                    hasParamEdge = checkDependencyForRead(currentTask, dp);
                } else {
                    hasParamEdge = checkDependencyForConcurrent(currentTask, dp);
                }
                if (isConstraining) {
                    RAccessId raId = (RAccessId) dp.getDataAccessId();
                    DataInstanceId dependingDataId = raId.getReadDataInstance();
                    if (dependingDataId != null) {
                        if (dependingDataId.getVersionId() > 1) {
                            WritersInfo wi = this.writers.get(dependingDataId.getDataId());
                            if (wi != null) {
                                switch (wi.getDataType()) {
                                    case STREAM_T:
                                    case EXTERNAL_STREAM_T:
                                        // Retrieve all the stream writers and enforce the execution to be near any
                                        List<AbstractTask> lastWriters = wi.getStreamWriters();
                                        if (!lastWriters.isEmpty()) {
                                            currentTask.setEnforcingTask((Task) (lastWriters.get(0)));
                                        }
                                        break;
                                    default:
                                        // Retrieve the writer and enforce the execution to be near the writer task
                                        AbstractTask lastWriter = wi.getDataWriter();
                                        if (lastWriter != null) {
                                            currentTask.setEnforcingTask((Task) lastWriter);
                                        }
                                        break;
                                }
                            }
                        }
                    }
                }
                break;
            case RW:
                if (!dataWasAccessedConcurrent(daId.getDataId())) {
                    hasParamEdge = checkDependencyForRead(currentTask, dp);
                } else {
                    hasParamEdge = checkDependencyForConcurrent(currentTask, dp);
                    removeFromConcurrentAccess(dp.getDataAccessId().getDataId());
                }
                if (isConstraining) {
                    RWAccessId raId = (RWAccessId) dp.getDataAccessId();
                    DataInstanceId dependingDataId = raId.getReadDataInstance();
                    if (dependingDataId != null) {
                        if (dependingDataId.getVersionId() > 1) {
                            WritersInfo wi = this.writers.get(dependingDataId.getDataId());
                            if (wi != null) {
                                switch (wi.getDataType()) {
                                    case STREAM_T:
                                        // Retrieve all the stream writers and enforce the execution to be near any
                                        List<AbstractTask> lastWriters = wi.getStreamWriters();
                                        if (!lastWriters.isEmpty()) {
                                            currentTask.setEnforcingTask((Task) lastWriters.get(0));
                                        }
                                        break;
                                    default:
                                        // Retrieve the writer and enforce the execution to be near the writer task
                                        AbstractTask lastWriter = wi.getDataWriter();
                                        if (lastWriter != null) {
                                            currentTask.setEnforcingTask((Task) lastWriter);
                                        }
                                        break;
                                }
                            }
                        }
                    }
                }
                registerOutputValues(currentTask, dp);
                break;
            case W:
                // Check concurrent
                if (dataWasAccessedConcurrent(daId.getDataId())) {
                    removeFromConcurrentAccess(dp.getDataAccessId().getDataId());
                }
                // Register output values
                registerOutputValues(currentTask, dp);
                break;
            case C:
                hasParamEdge = checkDependencyForRead(currentTask, dp);
                List<Task> tasks = this.concurrentAccessMap.get(daId.getDataId());
                if (tasks == null) {
                    tasks = new LinkedList<Task>();
                    this.concurrentAccessMap.put(daId.getDataId(), tasks);
                }
                tasks.add(currentTask);
                break;
            case CV:
                // Commutative accesses are processed in addCommutativeDependencies
                break;
        }

        return hasParamEdge;
    }

    private boolean checkDependencyForRead(Task currentTask, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();

        if (DEBUG) {
            LOGGER.debug("Checking READ dependency for datum " + dataId + " and task " + currentTask.getId());
        }

        WritersInfo wi = this.writers.get(dataId);
        if (wi != null) {
            switch (wi.getDataType()) {
                case STREAM_T:
                case EXTERNAL_STREAM_T:
                    addStreamDependency(currentTask, dp, wi);
                    break;
                default:
                    addRegularDependency(currentTask, dp, wi);
                    break;
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

        // A read dependency is always written in the task graph
        return true;
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

    /**
     * Checks the concurrent dependencies of a task {@code currentTask} considering the parameter {@code dp}.
     *
     * @param currentTask Task.
     * @param dp Dependency Parameter.
     */
    private boolean checkDependencyForConcurrent(Task currentTask, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();
        List<Task> tasks = this.concurrentAccessMap.get(dataId);
        if (!tasks.contains(currentTask)) {
            if (DEBUG) {
                LOGGER.debug("There was a concurrent access for datum " + dataId);
                LOGGER.debug("Adding dependency between concurrent list and task " + currentTask.getId());
            }
            for (Task t : tasks) {
                // Add dependency
                currentTask.addDataDependency(t, dp);
                if (IS_DRAW_GRAPH) {
                    drawEdges(currentTask, dp, t);
                }
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }

            currentTask.registerFreeParam(dp);

            // Add dependency to last sync point
            if (IS_DRAW_GRAPH) {
                drawEdges(currentTask, dp, null);
            }
        }

        // A concurrent dependency is always written in the task graph
        return true;
    }

    private void addRegularDependency(Task currentTask, DependencyParameter dp, WritersInfo wi) {
        int dataId = dp.getDataAccessId().getDataId();
        AbstractTask lastWriter = wi.getDataWriter();
        if (lastWriter != null && lastWriter != currentTask) {
            if (DEBUG) {
                LOGGER.debug("Last writer for datum " + dataId + " is task " + lastWriter.getId());
                LOGGER
                    .debug("Adding dependency between task " + lastWriter.getId() + " and task " + currentTask.getId());
            }

            if (lastWriter instanceof Task
                && ((Task) lastWriter).getCommutativeGroup(dp.getDataAccessId().getDataId()) != null) {
                currentTask.addDataDependency(((Task) lastWriter).getCommutativeGroup(dp.getDataAccessId().getDataId()),
                    dp);
            }

            // Add dependency
            currentTask.addDataDependency(lastWriter, dp);
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }
            currentTask.registerFreeParam(dp);
        }

        // Add edge to graph
        if (IS_DRAW_GRAPH) {
            drawEdges(currentTask, dp, lastWriter);
            checkIfPreviousGroupInGraph(dataId, currentTask);
        }
    }

    private void addStreamDependency(Task currentTask, DependencyParameter dp, WritersInfo wi) {
        int dataId = dp.getDataAccessId().getDataId();
        List<AbstractTask> lastStreamWriters = wi.getStreamWriters();
        if (!lastStreamWriters.isEmpty()) {
            if (DEBUG) {
                StringBuilder sb = new StringBuilder();
                if (lastStreamWriters.size() > 1) {
                    sb.append("Last writers for stream datum ");
                    sb.append(dataId);
                    sb.append(" are tasks ");
                } else {
                    sb.append("Last writer for stream datum ");
                    sb.append(dataId);
                    sb.append(" is task ");
                }
                for (AbstractTask lastWriter : lastStreamWriters) {
                    sb.append(lastWriter.getId());
                    sb.append(" ");
                }
                LOGGER.debug(sb.toString());
            }

            // Add dependencies
            for (AbstractTask lastWriter : lastStreamWriters) {
                // Debug message
                if (DEBUG) {
                    LOGGER.debug("Adding stream dependency between task " + lastWriter.getId() + " and task "
                        + currentTask.getId());
                }

                // Add dependency
                currentTask.addStreamDataDependency(lastWriter);
            }
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last stream writer for datum " + dataId);
            }
        }

        // Add edge to graph
        if (IS_DRAW_GRAPH) {
            drawStreamEdge(currentTask, dp, false);
            checkIfPreviousGroupInGraph(dataId, currentTask);
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
        if (com == null) {
            LOGGER.info("Creating a new commutative group " + comId);
            com = new CommutativeGroupTask(currentTask.getApplication(), comId);

            if (IS_DRAW_GRAPH) {
                LOGGER.debug("Checking if previous group in graph");
                checkIfPreviousGroupInGraph(daId.getDataId(), currentTask);
            }
            WritersInfo wi = this.writers.get(daId.getDataId());
            if (wi != null) {
                AbstractTask predecessor = wi.getDataWriter();
                com.setParentDataDependency(predecessor);
                LOGGER.debug("Setting parent data dependency");
            }
            this.commutativeGroup.put(comId.toString(), com);
            com.setRegisteredVersion(firstRegistered);
            registerOutputValues(com, dp);

        }

        com.setFinalVersion(((RWAccessId) daId).getWVersionId());
        boolean hasParamEdge = checkDependencyForCommutative(currentTask, dp, com);
        registerOutputValues(com, dp);

        return hasParamEdge;
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
            WritersInfo wi = this.writers.get(dataId);
            if (wi != null) {
                switch (dp.getDirection()) {
                    case OUT:
                    case INOUT:
                        // Substitute the current entry by the new access
                        if (wi.getDataWriter() != null && wi.getDataWriter().getId() == currentTaskId) {
                            wi.setDataWriter(null);
                        }
                        break;
                    default:
                        break;
                }
            }
        }
    }

    private void treatDataAccess(AbstractTask lastWriter, AccessMode am, int dataId) {
        // Add to writers if needed
        if (am == AccessMode.RW) {
            WritersInfo wi = this.writers.get(dataId);
            if (wi != null) {
                switch (wi.getDataType()) {
                    case STREAM_T:
                    case EXTERNAL_STREAM_T:
                        // Nothing to do, we do not reset the writers because of the main access
                        break;
                    default:
                        // Reset the writers entry
                        wi.setDataWriter(null);
                        break;
                }
            } else {
                // Add a new reset entry
                LOGGER.warn("Adding null writer info for data " + dataId);
                this.writers.put(dataId, null);
            }
        }

        // Add graph description
        if (IS_DRAW_GRAPH) {
            TreeSet<Integer> toPass = new TreeSet<>();
            toPass.add(dataId);
            DataInstanceId dii = this.dip.getLastVersions(toPass).get(0);
            int dataVersion = dii.getVersionId();
            addEdgeFromTaskToMain(lastWriter, EdgeType.DATA_DEPENDENCY, dataId, dataVersion);
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

            boolean canceledByException = false;
            if (t.hasTaskGroups()) {
                for (TaskGroup tg : t.getTaskGroupList()) {
                    if (tg.hasException() && t.getStatus() == TaskState.CANCELED) {
                        canceledByException = true;
                    }
                }
            }
            ReadersInfo readerData = new ReadersInfo(p, t);
            if (t.getOnFailure() == OnFailure.CANCEL_SUCCESSORS
                && (t.getStatus() == TaskState.FAILED || t.getStatus() == TaskState.CANCELED) || canceledByException) {
                this.dip.dataAccessHasBeenCanceled(dAccId, readerData);
            } else {
                this.dip.dataHasBeenAccessed(dAccId);
            }
        }
    }

    /**
     * Registers the output values of the task {@code currentTask}.
     *
     * @param currentTask Task.
     * @param dp Dependency Parameter.
     */
    private void registerOutputValues(AbstractTask currentTask, DependencyParameter dp) {
        int currentTaskId = currentTask.getId();
        int dataId = dp.getDataAccessId().getDataId();
        Application app = currentTask.getApplication();

        if (DEBUG) {
            LOGGER.debug("Checking WRITE dependency for datum " + dataId + " and task " + currentTaskId);
        }

        // Update global last writers
        switch (dp.getType()) {
            case STREAM_T:
            case EXTERNAL_STREAM_T:
                WritersInfo wi = this.writers.get(dataId);
                if (wi != null) {
                    wi.addStreamWriter(currentTask);
                } else {
                    wi = new WritersInfo(dp.getType(), Arrays.asList(currentTask));
                }
                this.writers.put(dataId, wi);

                if (IS_DRAW_GRAPH) {
                    drawStreamEdge(currentTask, dp, true);
                }
                break;
            default:
                // Substitute the current entry by the new access
                WritersInfo newWi = new WritersInfo(dp.getType(), currentTask);
                LOGGER.info("Setting writer for data " + dataId);
                this.writers.put(dataId, newWi);
                break;
        }

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

    private void checkResultFileTransfer(Task t) {
        LinkedList<DataInstanceId> fileIds = new LinkedList<>();
        for (Parameter p : t.getTaskDescription().getParameters()) {
            switch (p.getType()) {
                case DIRECTORY_T:
                case FILE_T:
                    DependencyParameter fp = (DependencyParameter) p;
                    switch (fp.getDirection()) {
                        case IN:
                        case IN_DELETE:
                        case CONCURRENT:
                            break;
                        case COMMUTATIVE:
                        case INOUT:
                            DataInstanceId dId = ((RWAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                            WritersInfo wi = this.writers.get(dId.getDataId());
                            if (wi != null) {
                                switch (wi.getDataType()) {
                                    case STREAM_T:
                                    case EXTERNAL_STREAM_T:
                                        // Streams have no result files regarding their direction
                                        break;
                                    default:
                                        AbstractTask lastWriter = wi.getDataWriter();
                                        if (lastWriter != null && lastWriter == t) {
                                            fileIds.add(dId);
                                        }
                                        break;
                                }
                            }
                            break;
                        case OUT:
                            dId = ((WAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                            wi = this.writers.get(dId.getDataId());
                            if (wi != null) {
                                switch (wi.getDataType()) {
                                    case STREAM_T:
                                        // Streams have no result files regarding their direction
                                        break;
                                    default:
                                        AbstractTask lastWriter = wi.getDataWriter();
                                        if (lastWriter != null && lastWriter == t) {
                                            fileIds.add(dId);
                                        }
                                        break;
                                }
                            }
                            break;
                    }
                    break;
                default:
                    break;
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

    /**
     * Adds edges to graph.
     *
     * @param currentTask New task.
     * @param dp Dependency parameter causing the dependency.
     * @param lastWriter Last writer task.
     */
    private void drawEdges(Task currentTask, DependencyParameter dp, AbstractTask lastWriter) {
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
                    } else if (!(lastWriter instanceof Task && !currentTask.hasCommutativeParams())) {
                        addEdgeFromCommutativeToTask(currentTask, dataId, dataVersion,
                            ((CommutativeGroupTask) lastWriter), true);
                    }
                } else {
                    addDataEdgeFromMainToTask(currentTask, dataId, dataVersion);
                }
                break;
        }
    }

    /**
     * Adds the stream node and edge to the graph.
     * 
     * @param currentTask Writer or reader task.
     * @param dp Stream parameter.
     * @param isWrite Whether the task is reading or writing the stream parameter.
     */
    private void drawStreamEdge(AbstractTask currentTask, DependencyParameter dp, boolean isWrite) {
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

    /**
     * Checks if the previous group was printed on the graph.
     *
     * @param dataId Data Id.
     * @param currentTask Task to check.
     */
    private void checkIfPreviousGroupInGraph(int dataId, Task currentTask) {
        WritersInfo wi = this.writers.get(dataId);
        if (wi != null) {
            AbstractTask lastWriter = wi.getDataWriter();

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

    /**
     * We have accessed to data produced by a task from the main code STEPS: Adds a new synchronization point if any
     * task has been created Adds a dependency from task to synchronization.
     *
     * @param task Task that generated the value.
     * @param edgeType Type of edge for the DOT representation.
     * @param dataId Data causing the dependency.
     */
    private void addEdgeFromTaskToMain(AbstractTask task, EdgeType edgeType, int dataId, int dataVersion) {
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
        for (WritersInfo wi : this.writers.values()) {
            if (wi != null) {
                // Add data writers
                AbstractTask dataWriter = wi.getDataWriter();
                if (dataWriter != null) {
                    uniqueWriters.add(dataWriter);
                }
                // Add stream writers
                uniqueWriters.addAll(wi.getStreamWriters());
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


    private static class WritersInfo {

        private final DataType dataType;
        private AbstractTask dataWriter;
        private final List<AbstractTask> streamWriters;


        public WritersInfo(DataType dataType, AbstractTask dataWriter) {
            this.dataType = dataType;
            this.dataWriter = dataWriter;
            this.streamWriters = new ArrayList<>();
        }

        public void setDataWriter(AbstractTask dataWriter) {
            this.dataWriter = dataWriter;
        }

        public WritersInfo(DataType dataType, List<AbstractTask> streamWriters) {
            this.dataType = dataType;
            this.dataWriter = null;
            this.streamWriters = new ArrayList<>();
            if (streamWriters != null) {
                this.streamWriters.addAll(streamWriters);
            }
        }

        public DataType getDataType() {
            return this.dataType;
        }

        public AbstractTask getDataWriter() {
            return this.dataWriter;
        }

        public List<AbstractTask> getStreamWriters() {
            return this.streamWriters;
        }

        public void addStreamWriter(AbstractTask writerTask) {
            if (writerTask != null) {
                this.streamWriters.add(writerTask);
            }
        }

        @Override
        public String toString() {
            StringBuilder sb = new StringBuilder();
            sb.append("WI [ ");
            sb.append("dataType = ").append(this.dataType).append(", ");
            sb.append("dataWriter = ").append(this.dataWriter != null ? this.dataWriter.getId() : "null").append(", ");
            sb.append("streamWriters = [");
            for (AbstractTask t : this.streamWriters) {
                sb.append(t.getId()).append(" ");
            }
            sb.append("]");
            sb.append("]");

            return sb.toString();
        }

    }

}
