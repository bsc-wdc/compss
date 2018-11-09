/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.HashMap;
import java.util.HashSet;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.Map.Entry;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StubItf;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.Task.TaskState;
import es.bsc.compss.types.data.DataAccessId.RWAccessId;
import es.bsc.compss.types.data.DataInfo;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.AccessParams.*;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataAccessId.*;
import es.bsc.compss.types.parameter.BindingObjectParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.ExternalPSCOParameter;
import es.bsc.compss.types.data.operation.ResultListener;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.parameter.FileParameter;
import es.bsc.compss.types.parameter.ObjectParameter;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.WaitForTaskRequest;
import es.bsc.compss.util.ErrorManager;


/**
 * Class to analyze the data dependencies between tasks
 *
 */
public class TaskAnalyser {

    // Components
    private DataInfoProvider DIP;
    private GraphGenerator GM;

    // <File id, Last writer task> table
    private TreeMap<Integer, Task> writers;
    // Method information
    private HashMap<Integer, Integer> currentTaskCount;
    // Map: app id -> task count
    private HashMap<Long, Integer> appIdToTotalTaskCount;
    // Map: app id -> task count
    private HashMap<Long, Integer> appIdToTaskCount;
    // Map: app id -> semaphore to notify end of app
    private HashMap<Long, Semaphore> appIdToSemaphore;
    // List of appIds stopped on a barrier synchronization point
    private HashSet<Long> appIdBarrierFlags;
    // Map: app id -> set of written data ids (for result files)
    private HashMap<Long, TreeSet<Integer>> appIdToWrittenFiles;
    // Map: app id -> set of written data ids (for result SCOs)
    private HashMap<Long, TreeSet<Integer>> appIdToSCOWrittenIds;
    // Tasks being waited on: taskId -> list of semaphores where to notify end of task
    private Hashtable<Task, List<Semaphore>> waitedTasks;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();
    private static final String TASK_FAILED = "Task failed: ";

    // Graph drawing
    private static final boolean IS_DRAW_GRAPH = GraphGenerator.isEnabled();
    private int synchronizationId;
    private boolean taskDetectedAfterSync;

    /**
     * Creates a new Task Analyser instance
     *
     */
    public TaskAnalyser() {
        this.currentTaskCount = new HashMap<>();
        this.writers = new TreeMap<>();
        this.appIdToTaskCount = new HashMap<>();
        this.appIdToTotalTaskCount = new HashMap<>();
        this.appIdToSemaphore = new HashMap<>();
        this.appIdBarrierFlags = new HashSet<>();
        this.appIdToWrittenFiles = new HashMap<>();
        this.appIdToSCOWrittenIds = new HashMap<>();
        this.waitedTasks = new Hashtable<>();

        synchronizationId = 0;
        taskDetectedAfterSync = false;

        LOGGER.info("Initialization finished");
    }

    /**
     * Sets the TaskAnalyser co-workers
     *
     * @param DIP
     */
    public void setCoWorkers(DataInfoProvider DIP) {
        this.DIP = DIP;
    }

    /**
     * Sets the graph generator co-worker
     *
     * @param GM
     */
    public void setGM(GraphGenerator GM) {
        this.GM = GM;
    }

    /**
     * Process the dependencies of a new task @currentTask
     *
     * @param currentTask
     */
    public void processTask(Task currentTask) {
        TaskDescription params = currentTask.getTaskDescription();
        LOGGER.info("New " + (params.getType() == TaskType.METHOD ? "method" : "service") + " task(" + params.getName() + "), ID = "
                + currentTask.getId());

        if (IS_DRAW_GRAPH) {
            addNewTask(currentTask);
        }

        // Update task count
        Integer methodId = params.getId();
        Integer actualCount = this.currentTaskCount.get(methodId);
        if (actualCount == null) {
            actualCount = 0;
        }
        this.currentTaskCount.put(methodId, actualCount + 1);

        // Update app id task count
        Long appId = currentTask.getAppId();
        Integer taskCount = this.appIdToTaskCount.get(appId);
        if (taskCount == null) {
            taskCount = 0;
        }
        taskCount++;
        this.appIdToTaskCount.put(appId, taskCount);
        Integer totalTaskCount = this.appIdToTotalTaskCount.get(appId);
        if (totalTaskCount == null) {
            totalTaskCount = 0;
        }
        totalTaskCount++;
        this.appIdToTotalTaskCount.put(appId, totalTaskCount);

        // Check scheduling enforcing data
        int constrainingParam = -1;
        if (params.getType() == TaskType.SERVICE && params.hasTargetObject()) {
            constrainingParam = params.getParameters().length - 1 - params.getNumReturns();
        }

        Parameter[] parameters = params.getParameters();
        for (int paramIdx = 0; paramIdx < parameters.length; paramIdx++) {
            Parameter p = parameters[paramIdx];
            if (DEBUG) {
                LOGGER.debug("* Parameter : " + p);
            }

            // Conversion: direction -> access mode
            AccessMode am = AccessMode.R;
            switch (p.getDirection()) {
                case IN:
                    am = AccessMode.R;
                    break;
                case OUT:
                    am = AccessMode.W;
                    break;
                case INOUT:
                    am = AccessMode.RW;
                    break;
            }

            // Inform the Data Manager about the new accesses
            DataAccessId daId;
            switch (p.getType()) {
                case FILE_T:
                    FileParameter fp = (FileParameter) p;
                    daId = this.DIP.registerFileAccess(am, fp.getLocation());
                    break;
                case PSCO_T:
                    ObjectParameter pscop = (ObjectParameter) p;
                    // Check if its PSCO class and persisted to infer its type
                    pscop.setType(DataType.PSCO_T);
                    daId = this.DIP.registerObjectAccess(am, pscop.getValue(), pscop.getCode());
                    break;
                case EXTERNAL_PSCO_T:
                    ExternalPSCOParameter externalPSCOparam = (ExternalPSCOParameter) p;
                    // Check if its PSCO class and persisted to infer its type
                    externalPSCOparam.setType(DataType.EXTERNAL_PSCO_T);
                    daId = DIP.registerExternalPSCOAccess(am, externalPSCOparam.getId(), externalPSCOparam.getCode());
                    break;
                case BINDING_OBJECT_T:
                    BindingObjectParameter bindingObjectparam = (BindingObjectParameter) p;
                    // Check if its Binding OBJ and register its access
                    bindingObjectparam.setType(DataType.BINDING_OBJECT_T);
                    daId = DIP.registerBindingObjectAccess(am, bindingObjectparam.getBindingObject(), bindingObjectparam.getCode());
                    break;
                case OBJECT_T:
                    ObjectParameter op = (ObjectParameter) p;
                    // Check if its PSCO class and persisted to infer its type
                    if (op.getValue() instanceof StubItf && ((StubItf) op.getValue()).getID() != null) {
                        op.setType(DataType.PSCO_T);
                    }
                    daId = this.DIP.registerObjectAccess(am, op.getValue(), op.getCode());
                    break;
                default:
                    /*
                     * Basic types (including String). The only possible access mode is R (already checked by the API)
                     */
                    continue;
            }

            // Add dependencies to the graph and register output values for future dependencies
            DependencyParameter dp = (DependencyParameter) p;
            dp.setDataAccessId(daId);
            switch (am) {
                case R:
                    checkDependencyForRead(currentTask, dp);
                    if (paramIdx == constrainingParam) {
                        DataAccessId.RAccessId raId = (DataAccessId.RAccessId) dp.getDataAccessId();
                        DataInstanceId dependingDataId = raId.getReadDataInstance();
                        if (dependingDataId != null) {
                            if (dependingDataId.getVersionId() > 1) {
                                currentTask.setEnforcingTask(this.writers.get(dependingDataId.getDataId()));
                            }
                        }
                    }
                    break;
                case RW:
                    checkDependencyForRead(currentTask, dp);
                    if (paramIdx == constrainingParam) {
                        DataAccessId.RWAccessId raId = (DataAccessId.RWAccessId) dp.getDataAccessId();
                        DataInstanceId dependingDataId = raId.getReadDataInstance();
                        if (dependingDataId != null) {
                            if (dependingDataId.getVersionId() > 1) {
                                currentTask.setEnforcingTask(this.writers.get(dependingDataId.getDataId()));
                            }
                        }
                    }
                    registerOutputValues(currentTask, dp);
                    break;
                case W:
                    registerOutputValues(currentTask, dp);
                    break;
            }
        }
    }

    /**
     * Registers the end of execution of task @task
     *
     * @param task
     */
    public void endTask(Task task) {
        int taskId = task.getId();
        boolean isFree = task.isFree();
        TaskState taskState = task.getStatus();

        LOGGER.info("Notification received for task " + taskId + " with end status " + taskState);

        // Check status
        if (!isFree) {
            LOGGER.debug("Task " + taskId + " is not registered as free. Waiting for other executions to end");
            return;
        }
        if (taskState == TaskState.FAILED) {
            ErrorManager.error(TASK_FAILED + task);
            return;
        }

        /*
         * Treat end of task
         */
        LOGGER.debug("Ending task " + taskId);

        // Free dependencies
        Long appId = task.getAppId();
        Integer taskCount = this.appIdToTaskCount.get(appId) - 1;
        this.appIdToTaskCount.put(appId, taskCount);
        if (taskCount == 0) {
            // Remove the appId from the barrier flags (if existent, otherwise do nothing)
            this.appIdBarrierFlags.remove(appId);

            Semaphore sem = this.appIdToSemaphore.remove(appId);
            if (sem != null) {
                // Application was synchronized on a barrier flag or a no more tasks
                // Release the application semaphore
                this.appIdToTaskCount.remove(appId);
                sem.release();
            }
        }

        // Check if task is being waited
        List<Semaphore> sems = this.waitedTasks.remove(task);
        if (sems != null) {
            for (Semaphore sem : sems) {
                sem.release();
            }
        }

        for (Parameter param : task.getTaskDescription().getParameters()) {
            DataType type = param.getType();
            if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T || type == DataType.EXTERNAL_PSCO_T
                    || type == DataType.BINDING_OBJECT_T) {
                DependencyParameter dPar = (DependencyParameter) param;
                DataAccessId dAccId = dPar.getDataAccessId();
                LOGGER.debug("Treating that data " + dAccId + " has been accessed at " + dPar.getDataTarget());
                this.DIP.dataHasBeenAccessed(dAccId);
            }
        }

        // Check if the finished task was the last writer of a file, but only if task generation has finished
        // Task generation is finished if we are on noMoreTasks but we are not on a barrier
        if (this.appIdToSemaphore.get(appId) != null && !this.appIdBarrierFlags.contains(appId)) {
            checkResultFileTransfer(task);
        }

        // Release data dependent tasks
        task.releaseDataDependents();
    }

    /**
     * Checks if a finished task is the last writer of its file parameters and, eventually, order the necessary
     * transfers
     *
     * @param t
     */
    private void checkResultFileTransfer(Task t) {
        LinkedList<DataInstanceId> fileIds = new LinkedList<>();
        for (Parameter p : t.getTaskDescription().getParameters()) {
            switch (p.getType()) {
                case FILE_T:
                    FileParameter fp = (FileParameter) p;
                    switch (fp.getDirection()) {
                        case IN:
                            break;
                        case INOUT:
                            DataInstanceId dId = ((RWAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                            if (this.writers.get(dId.getDataId()) == t) {
                                fileIds.add(dId);
                            }
                            break;
                        case OUT:
                            dId = ((WAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                            if (this.writers.get(dId.getDataId()) == t) {
                                fileIds.add(dId);
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
            // List<ResultFile> resFiles = new ArrayList<ResultFile>(numFT);
            for (DataInstanceId fileId : fileIds) {
                try {
                    int id = fileId.getDataId();
                    this.DIP.blockDataAndGetResultFile(id, new ResultListener(new Semaphore(0)));
                    this.DIP.unblockDataId(id);
                } catch (Exception e) {
                    LOGGER.error("Exception ordering transfer when task ends", e);
                }
            }

        }
    }

    /**
     * Returns the tasks dependent to the requested task
     *
     * @param request
     */
    public void findWaitedTask(WaitForTaskRequest request) {
        int dataId = request.getDataId();
        AccessMode am = request.getAccessMode();
        Semaphore sem = request.getSemaphore();
        Task lastWriter = this.writers.get(dataId);

        if (lastWriter != null) {
            // Add to writers if needed
            if (am == AccessMode.RW) {
                this.writers.put(dataId, null);
            }

            // Add graph description
            if (IS_DRAW_GRAPH) {
                TreeSet<Integer> toPass = new TreeSet<>();
                toPass.add(dataId);
                DataInstanceId dii = DIP.getLastVersions(toPass).get(0);
                int dataVersion = dii.getVersionId();
                addEdgeFromTaskToMain(lastWriter, dataId, dataVersion);
            }
        }

        // Release task if possible. Otherwise add to waiting
        if (lastWriter == null || lastWriter.getStatus() == TaskState.FINISHED) {
            sem.release();
        } else {
            List<Semaphore> list = this.waitedTasks.get(lastWriter);
            if (list == null) {
                list = new LinkedList<>();
                this.waitedTasks.put(lastWriter, list);
            }
            list.add(sem);
        }
    }

    /**
     * Barrier
     *
     * @param request
     */
    public void barrier(BarrierRequest request) {
        Long appId = request.getAppId();
        Integer count = this.appIdToTaskCount.get(appId);
        if (IS_DRAW_GRAPH) {
            addNewBarrier();

            // We can draw the graph on a barrier while we wait for tasks
            this.GM.commitGraph();
        }

        // Release the semaphore only if all application tasks have finished
        if (count == null || count == 0) {
            request.getSemaphore().release();
        } else {
            this.appIdBarrierFlags.add(appId);
            this.appIdToSemaphore.put(appId, request.getSemaphore());
        }
    }

    /**
     * End of execution barrier
     *
     * @param request
     */
    public void noMoreTasks(EndOfAppRequest request) {
        Long appId = request.getAppId();
        Integer count = this.appIdToTaskCount.get(appId);

        if (IS_DRAW_GRAPH) {
            this.GM.commitGraph();
        }

        if (count == null || count == 0) {
            this.appIdToTaskCount.remove(appId);
            request.getSemaphore().release();
        } else {
            this.appIdToSemaphore.put(appId, request.getSemaphore());
        }
    }

    /**
     * Returns the written files and deletes them
     *
     * @param appId
     * @return
     */
    public TreeSet<Integer> getAndRemoveWrittenFiles(Long appId) {
        return this.appIdToWrittenFiles.remove(appId);
    }

    /**
     * Shutdown
     *
     */
    public void shutdown() {
        if (IS_DRAW_GRAPH) {
            GraphGenerator.removeTemporaryGraph();
        }
    }

    /**
     * Returns the task state
     *
     * @return
     */
    public String getTaskStateRequest() {
        StringBuilder sb = new StringBuilder("\t").append("<TasksInfo>").append("\n");
        for (Entry<Long, Integer> e : this.appIdToTotalTaskCount.entrySet()) {
            Long appId = e.getKey();
            Integer totalTaskCount = e.getValue();
            Integer taskCount = this.appIdToTaskCount.get(appId);
            if (taskCount == null) {
                taskCount = 0;
            }
            int completed = totalTaskCount - taskCount;
            sb.append("\t\t").append("<Application id=\"").append(appId).append("\">").append("\n");
            sb.append("\t\t\t").append("<TotalCount>").append(totalTaskCount).append("</TotalCount>").append("\n");
            sb.append("\t\t\t").append("<InProgress>").append(taskCount).append("</InProgress>").append("\n");
            sb.append("\t\t\t").append("<Completed>").append(completed).append("</Completed>").append("\n");
            sb.append("\t\t").append("</Application>").append("\n");
        }
        sb.append("\t").append("</TasksInfo>").append("\n");
        return sb.toString();
    }

    /**
     * Deletes the specified data and its renamings
     *
     * @param dataInfo
     */
    public void deleteData(DataInfo dataInfo) {
        int dataId = dataInfo.getDataId();

        LOGGER.debug("Deleting data with id " + dataId);

        Task task = writers.remove(dataId);
        if (task != null) {
            return;
        }

        LOGGER.debug("Removing " + dataInfo.getDataId() + " from written files");
        for (TreeSet<Integer> files : appIdToWrittenFiles.values()) {
            files.remove(dataInfo.getDataId());
        }
    }

    /*
     **************************************************************************************************************
     * DATA DEPENDENCY MANAGEMENT PRIVATE METHODS
     **************************************************************************************************************/
    /**
     * Checks the dependencies of a task @currentTask considering the parameter @dp
     *
     * @param currentTask
     * @param dp
     */
    private void checkDependencyForRead(Task currentTask, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();
        Task lastWriter = this.writers.get(dataId);

        if (lastWriter != null && lastWriter != currentTask) {
            if (DEBUG) {
                LOGGER.debug("Last writer for datum " + dp.getDataAccessId().getDataId() + " is task " + lastWriter.getId());
                LOGGER.debug("Adding dependency between task " + lastWriter.getId() + " and task " + currentTask.getId());
            }
            // Add dependency
            currentTask.addDataDependency(lastWriter);
        } else{
            if (DEBUG) {
            LOGGER.debug("There is no last writer for datum " + dp.getDataAccessId().getDataId());
            }
        }
        // Handle when -g enabled
        if (IS_DRAW_GRAPH) {
            int dataVersion = -1;
            Direction d = dp.getDataAccessId().getDirection();
            switch (d) {
                case R:
                    dataVersion = ((DataAccessId.RAccessId) dp.getDataAccessId()).getRVersionId();
                    break;
                case W:
                    dataVersion = ((DataAccessId.WAccessId) dp.getDataAccessId()).getWVersionId();
                    break;
                default:
                    dataVersion = ((DataAccessId.RWAccessId) dp.getDataAccessId()).getRVersionId();
                    break;
            }
            if (lastWriter != null && lastWriter != currentTask) {
                addEdgeFromTaskToTask(lastWriter, currentTask, dataId, dataVersion);
            } else {
                addEdgeFromMainToTask(currentTask, dataId, dataVersion);
            }
        }
    }

    /**
     * Registers the output values of the task @currentTask
     *
     * @param currentTask
     * @param dp
     */
    private void registerOutputValues(Task currentTask, DependencyParameter dp) {
        int currentTaskId = currentTask.getId();
        int dataId = dp.getDataAccessId().getDataId();
        Long appId = currentTask.getAppId();

        // Update global last writer
        this.writers.put(dataId, currentTask);

        // Update file and PSCO lists
        switch (dp.getType()) {
            case FILE_T:
                TreeSet<Integer> fileIdsWritten = this.appIdToWrittenFiles.get(appId);
                if (fileIdsWritten == null) {
                    fileIdsWritten = new TreeSet<>();
                    this.appIdToWrittenFiles.put(appId, fileIdsWritten);
                }
                fileIdsWritten.add(dataId);
                break;
            case PSCO_T:
                TreeSet<Integer> pscoIdsWritten = this.appIdToSCOWrittenIds.get(appId);
                if (pscoIdsWritten == null) {
                    pscoIdsWritten = new TreeSet<>();
                    this.appIdToSCOWrittenIds.put(appId, pscoIdsWritten);
                }
                pscoIdsWritten.add(dataId);
                break;
            default:
                // Nothing to do with basic types
                // Objects are not checked, their version will be only get if the main accesses them
                break;
        }

        if (DEBUG) {
            LOGGER.debug("New writer for datum " + dp.getDataAccessId().getDataId() + " is task " + currentTaskId);
        }
    }

    /*
     **************************************************************************************************************
     * GRAPH WRAPPERS
     **************************************************************************************************************/
    /**
     * We have detected a new task, register it into the graph STEPS: Only adds the node
     *
     * @param task
     */
    private void addNewTask(Task task) {
        // Add task to graph
        this.GM.addTaskToGraph(task);
        // Set the syncId of the task
        task.setSynchronizationId(this.synchronizationId);
        // Update current sync status
        taskDetectedAfterSync = true;
    }

    /**
     * We will execute a task whose data is produced by another task. STEPS: Add an edge from the previous task or the
     * last synchronization point to the new task
     *
     * @param source
     * @param dest
     * @param dataId
     */
    private void addEdgeFromTaskToTask(Task source, Task dest, int dataId, int dataVersion) {
        if (source.getSynchronizationId() == dest.getSynchronizationId()) {
            String src = String.valueOf(source.getId());
            String dst = String.valueOf(dest.getId());
            String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
            this.GM.addEdgeToGraph(src, dst, dep);
        } else {
            String src = "Synchro" + dest.getSynchronizationId();
            String dst = String.valueOf(dest.getId());
            String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
            this.GM.addEdgeToGraph(src, dst, dep);
        }
    }

    /**
     * We will execute a task with no predecessors, data must be retrieved from the last synchronization point. STEPS:
     * Add edge from sync to task
     *
     * @param dest
     * @param dataId
     */
    private void addEdgeFromMainToTask(Task dest, int dataId, int dataVersion) {
        String src = "Synchro" + dest.getSynchronizationId();
        String dst = String.valueOf(dest.getId());
        String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
        this.GM.addEdgeToGraph(src, dst, dep);
    }

    /**
     * We have accessed to data produced by a task from the main code STEPS: Adds a new synchronization point if any
     * task has been created Adds a dependency from task to synchronization
     *
     * @param task
     * @param dataId
     */
    private void addEdgeFromTaskToMain(Task task, int dataId, int dataVersion) {
        // Add Sync if any task has been created
        if (this.taskDetectedAfterSync) {
            this.taskDetectedAfterSync = false;

            int oldSyncId = this.synchronizationId;
            this.synchronizationId++;
            this.GM.addSynchroToGraph(this.synchronizationId);
            if (this.synchronizationId > 1) {
                String oldSync = "Synchro" + oldSyncId;
                String currentSync = "Synchro" + this.synchronizationId;
                this.GM.addEdgeToGraph(oldSync, currentSync, "");
            }
        }

        // Add edge from task to sync
        String src = String.valueOf(task.getId());
        String dest = "Synchro" + this.synchronizationId;
        this.GM.addEdgeToGraph(src, dest, String.valueOf(dataId) + "v" + String.valueOf(dataVersion));
    }

    /**
     * We have explicitly called the barrier API. STEPS: Add a new synchronization node. Add an edge from last
     * synchronization point to barrier. Add edges from writer tasks to barrier
     */
    private void addNewBarrier() {
        // Add barrier node
        int oldSync = this.synchronizationId;
        this.synchronizationId++;
        this.taskDetectedAfterSync = false;
        this.GM.addBarrierToGraph(this.synchronizationId);

        // Add edge from last sync
        String newSync_str = "Synchro" + this.synchronizationId;
        String oldSync_str = "Synchro" + oldSync;
        if (this.synchronizationId > 1) {
            this.GM.addEdgeToGraph(oldSync_str, newSync_str, "");
        }

        // Add edges from writers to barrier
        HashSet<Task> uniqueWriters = new HashSet<>(this.writers.values());
        for (Task writer : uniqueWriters) {
            if (writer != null && writer.getSynchronizationId() == oldSync) {
                String taskId = String.valueOf(writer.getId());
                this.GM.addEdgeToGraph(taskId, newSync_str, "");
            }
        }
    }

}
