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
import es.bsc.compss.types.Task;
import es.bsc.compss.types.Task.TaskState;
import es.bsc.compss.types.TaskDescription;
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
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.parameter.BindingObjectParameter;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.ExternalPSCOParameter;
import es.bsc.compss.types.parameter.FileParameter;
import es.bsc.compss.types.parameter.ObjectParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.parameter.StreamParameter;
import es.bsc.compss.types.request.ap.BarrierRequest;
import es.bsc.compss.types.request.ap.EndOfAppRequest;
import es.bsc.compss.types.request.ap.WaitForConcurrentRequest;
import es.bsc.compss.types.request.ap.WaitForTaskRequest;
import es.bsc.compss.util.ErrorManager;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Hashtable;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
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
    private TreeMap<Integer, WritersInfo> writers;
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
    // Concurrent tasks being waited on: taskId -> semaphore where to notify end of task
    private TreeMap<Integer, List<Task>> concurrentAccessMap;

    // Graph drawing
    private static final boolean IS_DRAW_GRAPH = GraphGenerator.isEnabled();
    private int synchronizationId;
    private boolean taskDetectedAfterSync;


    /**
     * Creates a new Task Analyzer instance.
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
        this.concurrentAccessMap = new TreeMap<>();

        this.synchronizationId = 0;
        this.taskDetectedAfterSync = false;

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
    }

    private DataAccessId registerParameterAccessAndAddDependencies(Task currentTask, boolean isConstraining,
            Parameter p) {

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
            case CONCURRENT:
                am = AccessMode.C;
                break;
        }
        // Inform the Data Manager about the new accesses
        DataAccessId daId = null;
        switch (p.getType()) {
            case FILE_T:
                FileParameter fp = (FileParameter) p;
                daId = this.dip.registerFileAccess(am, fp.getLocation());
                break;
            case PSCO_T:
                ObjectParameter pscop = (ObjectParameter) p;
                // Check if its PSCO class and persisted to infer its type
                pscop.setType(DataType.PSCO_T);
                daId = this.dip.registerObjectAccess(am, pscop.getValue(), pscop.getCode());
                break;
            case EXTERNAL_PSCO_T:
                ExternalPSCOParameter externalPSCOparam = (ExternalPSCOParameter) p;
                // Check if its PSCO class and persisted to infer its type
                externalPSCOparam.setType(DataType.EXTERNAL_PSCO_T);
                daId = dip.registerExternalPSCOAccess(am, externalPSCOparam.getId(), externalPSCOparam.getCode());
                break;
            case BINDING_OBJECT_T:
                BindingObjectParameter bindingObjectparam = (BindingObjectParameter) p;
                // Check if its Binding OBJ and register its access
                bindingObjectparam.setType(DataType.BINDING_OBJECT_T);
                daId = dip.registerBindingObjectAccess(am, bindingObjectparam.getBindingObject(),
                        bindingObjectparam.getCode());
                break;
            case OBJECT_T:
                ObjectParameter op = (ObjectParameter) p;
                // Check if its PSCO class and persisted to infer its type
                if (op.getValue() instanceof StubItf && ((StubItf) op.getValue()).getID() != null) {
                    op.setType(DataType.PSCO_T);
                }
                daId = this.dip.registerObjectAccess(am, op.getValue(), op.getCode());
                break;
            case STREAM_T:
                StreamParameter sp = (StreamParameter) p;
                daId = this.dip.registerStreamAccess(am, sp.getValue(), sp.getCode());
                break;
            case COLLECTION_T:
                CollectionParameter cp = (CollectionParameter) p;
                for (Parameter content : cp.getParameters()) {
                    registerParameterAccessAndAddDependencies(currentTask, isConstraining, content);
                }
                daId = dip.registerCollectionAccess(am, cp);
                break;
            default:
                // This is a basic type, there are no accesses to register
                return null;
        }
        DependencyParameter dp = (DependencyParameter) p;
        dp.setDataAccessId(daId);
        addDependencies(am, currentTask, isConstraining, dp);
        return daId;
    }

    private void addDependencies(AccessMode am, Task currentTask, boolean isConstraining, DependencyParameter dp) {
        // Add dependencies to the graph and register output values for future dependencies
        DataAccessId daId = dp.getDataAccessId();
        switch (am) {
            case R:
                if (!dataWasAccessedConcurrent(daId.getDataId())) {
                    checkDependencyForRead(currentTask, dp);
                } else {
                    checkDependencyForConcurrent(currentTask, dp);
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
                                        // Retrieve all the stream writers and enforce the execution to be near any
                                        List<Task> lastWriters = wi.getStreamWriters();
                                        if (!lastWriters.isEmpty()) {
                                            currentTask.setEnforcingTask(lastWriters.get(0));
                                        }
                                        break;
                                    default:
                                        // Retrieve the writer and enforce the execution to be near the writer task
                                        Task lastWriter = wi.getDataWriter();
                                        if (lastWriter != null) {
                                            currentTask.setEnforcingTask(lastWriter);
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
                    checkDependencyForRead(currentTask, dp);
                } else {
                    checkDependencyForConcurrent(currentTask, dp);
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
                                        List<Task> lastWriters = wi.getStreamWriters();
                                        if (!lastWriters.isEmpty()) {
                                            currentTask.setEnforcingTask(lastWriters.get(0));
                                        }
                                        break;
                                    default:
                                        // Retrieve the writer and enforce the execution to be near the writer task
                                        Task lastWriter = wi.getDataWriter();
                                        if (lastWriter != null) {
                                            currentTask.setEnforcingTask(lastWriter);
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
                if (dataWasAccessedConcurrent(daId.getDataId())) {
                    removeFromConcurrentAccess(dp.getDataAccessId().getDataId());
                }
                registerOutputValues(currentTask, dp);
                break;
            case C:
                checkDependencyForRead(currentTask, dp);
                List<Task> tasks = this.concurrentAccessMap.get(daId.getDataId());
                if (tasks == null) {
                    tasks = new LinkedList<Task>();
                    this.concurrentAccessMap.put(daId.getDataId(), tasks);
                }
                tasks.add(currentTask);
                break;
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
            constrainingParam = params.getParameters().size() - 1 - params.getNumReturns();
        }
        List<Parameter> parameters = params.getParameters();
        for (int paramIdx = 0; paramIdx < parameters.size(); paramIdx++) {
            registerParameterAccessAndAddDependencies(currentTask, paramIdx == constrainingParam,
                    parameters.get(paramIdx));
        }
    }

    /**
     * Registers the end of execution of task @{code task}.
     *
     * @param task Ended task.
     */
    public void endTask(Task task) {
        int taskId = task.getId();
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
            if (type == DataType.FILE_T || type == DataType.OBJECT_T || type == DataType.PSCO_T
                    || type == DataType.STREAM_T || type == DataType.EXTERNAL_PSCO_T
                    || type == DataType.BINDING_OBJECT_T) {

                DependencyParameter dPar = (DependencyParameter) param;
                DataAccessId dAccId = dPar.getDataAccessId();
                LOGGER.debug("Treating that data " + dAccId + " has been accessed at " + dPar.getDataTarget());
                if (task.getOnFailure() == OnFailure.CANCEL_SUCCESSORS
                        && (task.getStatus() == TaskState.FAILED || task.getStatus() == TaskState.CANCELED)) {
                    this.dip.dataAccessHasBeenCanceled(dAccId);
                } else {
                    this.dip.dataHasBeenAccessed(dAccId);
                }
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
     * transfers.
     *
     * @param t Task.
     */
    private void checkResultFileTransfer(Task t) {
        LinkedList<DataInstanceId> fileIds = new LinkedList<>();
        for (Parameter p : t.getTaskDescription().getParameters()) {
            switch (p.getType()) {
                case FILE_T:
                    FileParameter fp = (FileParameter) p;
                    switch (fp.getDirection()) {
                        case IN:
                        case CONCURRENT:
                            break;
                        case INOUT:
                            DataInstanceId dId = ((RWAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                            WritersInfo wi = this.writers.get(dId.getDataId());
                            if (wi != null) {
                                switch (wi.getDataType()) {
                                    case STREAM_T:
                                        // Streams have no result files regarding their direction
                                        break;
                                    default:
                                        Task lastWriter = wi.getDataWriter();
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
                                        Task lastWriter = wi.getDataWriter();
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
            // List<ResultFile> resFiles = new ArrayList<ResultFile>(numFT);
            for (DataInstanceId fileId : fileIds) {
                try {
                    int id = fileId.getDataId();
                    this.dip.blockDataAndGetResultFile(id, new ResultListener(new Semaphore(0)));
                    this.dip.unblockDataId(id);
                } catch (Exception e) {
                    LOGGER.error("Exception ordering transfer when task ends", e);
                }
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
                    // Mark the data accesses
                    List<Task> lastStreamWriters = wi.getStreamWriters();
                    for (Task lastWriter : lastStreamWriters) {
                        treatDataAccess(lastWriter, am, dataId);
                    }
                    // We do not wait for stream task to complete
                    sem.release();
                    break;
                default:
                    // Retrieve last writer task
                    Task lastWriter = wi.getDataWriter();
                    // Mark the data access
                    if (lastWriter != null) {
                        treatDataAccess(lastWriter, am, dataId);
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
                    break;
            }
        }
    }

    /**
     * Checks how the data was accessed.
     *
     * @param lastWriter Writer task.
     * @param am Access mode.
     * @param dataId Data Id.
     */
    private void treatDataAccess(Task lastWriter, AccessMode am, int dataId) {
        // Add to writers if needed
        if (am == AccessMode.RW) {
            WritersInfo wi = this.writers.get(dataId);
            if (wi != null) {
                switch (wi.getDataType()) {
                    case STREAM_T:
                        // Nothing to do, we do not reset the writers because of the main access
                        break;
                    default:
                        // Reset the writers entry
                        this.writers.put(dataId, null);
                        break;
                }
            } else {
                // Add a new reset entry
                this.writers.put(dataId, null);
            }
        }

        // Add graph description
        if (IS_DRAW_GRAPH) {
            TreeSet<Integer> toPass = new TreeSet<>();
            toPass.add(dataId);
            DataInstanceId dii = dip.getLastVersions(toPass).get(0);
            int dataVersion = dii.getVersionId();
            addEdgeFromTaskToMain(lastWriter, EdgeType.DATA_DEPENDENCY, dataId, dataVersion);
        }
    }

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
                List<Semaphore> list = waitedTasks.get(task);
                if (list == null) {
                    list = new LinkedList<>();
                    this.waitedTasks.put(task, list);
                }
                list.add(semTasks);
            }
        }
        request.setNumWaitedTasks(n);
        request.getSemaphore().release();
    }

    /**
     * Barrier.
     *
     * @param request Barrier request.
     */
    public void barrier(BarrierRequest request) {
        Long appId = request.getAppId();
        Integer count = this.appIdToTaskCount.get(appId);
        if (IS_DRAW_GRAPH) {
            addNewBarrier();

            // We can draw the graph on a barrier while we wait for tasks
            this.gm.commitGraph();
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
     * End of execution barrier.
     *
     * @param request End of execution request.
     */
    public void noMoreTasks(EndOfAppRequest request) {
        Long appId = request.getAppId();
        Integer count = this.appIdToTaskCount.get(appId);

        if (IS_DRAW_GRAPH) {
            this.gm.commitGraph();
        }

        if (count == null || count == 0) {
            this.appIdToTaskCount.remove(appId);
            request.getSemaphore().release();
        } else {
            this.appIdToSemaphore.put(appId, request.getSemaphore());
        }
    }

    /**
     * Returns the written files and deletes them.
     *
     * @param appId Application id.
     * @return List of written files of the application.
     */
    public TreeSet<Integer> getAndRemoveWrittenFiles(Long appId) {
        return this.appIdToWrittenFiles.remove(appId);
    }

    /**
     * Shutdown the component.
     */
    public void shutdown() {
        if (IS_DRAW_GRAPH) {
            GraphGenerator.removeTemporaryGraph();
        }
    }

    /**
     * Returns the tasks state.
     *
     * @return A string representation of the tasks state.
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
     * Deletes the specified data and its renamings.
     *
     * @param dataInfo DataInfo.
     */
    public void deleteData(DataInfo dataInfo) {
        int dataId = dataInfo.getDataId();

        LOGGER.debug("Deleting data with id " + dataId);

        WritersInfo wi = this.writers.remove(dataId);
        if (wi != null) {
            switch (wi.getDataType()) {
                case STREAM_T:
                    // No data to delete
                    break;
                default:
                    Task task = wi.getDataWriter();
                    if (task != null) {
                        // Cannot delete data because task is still running
                        return;
                    } else {
                        // Remove data
                        LOGGER.debug("Removing " + dataInfo.getDataId() + " from written files");
                        for (TreeSet<Integer> files : this.appIdToWrittenFiles.values()) {
                            files.remove(dataInfo.getDataId());
                        }
                    }
                    break;
            }
        }
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
     **************************************************************************************************************
     * DATA DEPENDENCY MANAGEMENT PRIVATE METHODS
     **************************************************************************************************************/
    /**
     * Checks the dependencies of a task {@code currentTask} considering the parameter {@code dp}.
     *
     * @param currentTask Task to analyze.
     * @param dp Dependency Parameter to analyze.
     */
    private void checkDependencyForRead(Task currentTask, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();

        if (DEBUG) {
            LOGGER.debug("Checking READ dependency for datum " + dataId + " and task " + currentTask.getId());
        }

        WritersInfo wi = this.writers.get(dataId);
        if (wi != null) {
            switch (wi.getDataType()) {
                case STREAM_T:
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
        }
    }

    private void addStreamDependency(Task currentTask, DependencyParameter dp, WritersInfo wi) {
        int dataId = dp.getDataAccessId().getDataId();
        List<Task> lastStreamWriters = wi.getStreamWriters();
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
                for (Task lastWriter : lastStreamWriters) {
                    sb.append(lastWriter.getId());
                    sb.append(" ");
                }
                LOGGER.debug(sb.toString());
            }

            // Add dependencies
            for (Task lastWriter : lastStreamWriters) {
                // Debug message
                if (DEBUG) {
                    LOGGER.debug("Adding stream dependency between task " + lastWriter.getId() + " and task "
                            + currentTask.getId());
                }

                // Add dependency
                currentTask.addStreamDataDependency(lastWriter);

                // Add edge to graph
                if (IS_DRAW_GRAPH) {
                    drawEdges(currentTask, dp, lastWriter);
                }
            }
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last stream writer for datum " + dataId);
            }
        }
    }

    private void addRegularDependency(Task currentTask, DependencyParameter dp, WritersInfo wi) {
        int dataId = dp.getDataAccessId().getDataId();
        Task lastWriter = wi.getDataWriter();
        if (lastWriter != null && lastWriter != currentTask) {
            if (DEBUG) {
                LOGGER.debug("Last writer for datum " + dataId + " is task " + lastWriter.getId());
                LOGGER.debug(
                        "Adding dependency between task " + lastWriter.getId() + " and task " + currentTask.getId());
            }

            // Add dependency
            currentTask.addDataDependency(lastWriter);

            // Add edge to graph
            if (IS_DRAW_GRAPH) {
                drawEdges(currentTask, dp, lastWriter);
            }
        } else {
            // Task is free
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }
        }
    }

    /**
     * Adds edges to graph.
     *
     * @param currentTask New task.
     * @param dp Dependency parameter causing the dependency.
     * @param lastWriter Last writer task.
     */
    private void drawEdges(Task currentTask, DependencyParameter dp, Task lastWriter) {
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
                addStreamEdgeFromTaskToTask(lastWriter, currentTask, dataId, dataVersion);
                break;
            default:
                if (lastWriter != null && lastWriter != currentTask) {
                    addDataEdgeFromTaskToTask(lastWriter, currentTask, dataId, dataVersion);
                } else {
                    addDataEdgeFromMainToTask(currentTask, dataId, dataVersion);
                }
                break;
        }
    }

    /**
     * Checks the concurrent dependencies of a task {@code currentTask} considering the parameter {@code dp}.
     *
     * @param currentTask Task.
     * @param dp Dependency Parameter.
     */
    private void checkDependencyForConcurrent(Task currentTask, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();
        List<Task> tasks = this.concurrentAccessMap.get(dataId);

        if (this.concurrentAccessMap != null && tasks.contains(currentTask) == false) {
            if (DEBUG) {
                LOGGER.debug("There was a concurrent access for datum " + dataId);
                LOGGER.debug("Adding dependency between list and task " + currentTask.getId());
            }
            for (Task t : tasks) {
                // Add dependency
                currentTask.addDataDependency(t);
                if (IS_DRAW_GRAPH) {
                    drawEdges(currentTask, dp, t);
                }
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("There is no last writer for datum " + dataId);
            }
        }
    }

    /**
     * Registers the output values of the task {@code currentTask}.
     *
     * @param currentTask Task.
     * @param dp Dependency Parameter.
     */
    private void registerOutputValues(Task currentTask, DependencyParameter dp) {
        int currentTaskId = currentTask.getId();
        int dataId = dp.getDataAccessId().getDataId();
        Long appId = currentTask.getAppId();

        if (DEBUG) {
            LOGGER.debug("Checking WRITE dependency for datum " + dataId + " and task " + currentTaskId);
        }

        // Update global last writers
        switch (dp.getType()) {
            case STREAM_T:
                WritersInfo wi = this.writers.get(dataId);
                if (wi != null) {
                    wi.addStreamWriter(currentTask);
                } else {
                    wi = new WritersInfo(dp.getType(), Arrays.asList(currentTask));
                }
                this.writers.put(dataId, wi);
                break;
            default:
                // Substitute the current entry by the new access
                WritersInfo newWi = new WritersInfo(dp.getType(), currentTask);
                this.writers.put(dataId, newWi);
                break;
        }

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
            LOGGER.debug("New writer for datum " + dataId + " is task " + currentTaskId);
        }
    }

    /*
     **************************************************************************************************************
     * GRAPH WRAPPERS
     **************************************************************************************************************/
    /**
     * We have detected a new task, register it into the graph. STEPS: Only adds the node.
     *
     * @param task New task.
     */
    private void addNewTask(Task task) {
        // Add task to graph
        this.gm.addTaskToGraph(task);
        // Set the syncId of the task
        task.setSynchronizationId(this.synchronizationId);
        // Update current sync status
        this.taskDetectedAfterSync = true;
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
     * We will execute a task whose data is produced by another task. STEPS: Add an edge from the previous task or the
     * last synchronization point to the new task.
     *
     * @param source Source task.
     * @param dest Destination task.
     * @param dataId Data causing the dependency.
     * @param dataVersion Data version.
     */
    private void addStreamEdgeFromTaskToTask(Task source, Task dest, int dataId, int dataVersion) {
        // Streams do not consider main synchro points, add dependency between tasks
        String src = String.valueOf(source.getId());
        String dst = String.valueOf(dest.getId());
        String dep = String.valueOf(dataId) + "v" + String.valueOf(dataVersion);
        this.gm.addEdgeToGraph(src, dst, EdgeType.STREAM_DEPENDENCY, dep);
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
     * We have accessed to data produced by a task from the main code STEPS: Adds a new synchronization point if any
     * task has been created Adds a dependency from task to synchronization.
     *
     * @param task Task that generated the value.
     * @param edgeType Type of edge for the DOT representation.
     * @param dataId Data causing the dependency.
     */
    private void addEdgeFromTaskToMain(Task task, EdgeType edgeType, int dataId, int dataVersion) {
        // Add Sync if any task has been created
        if (this.taskDetectedAfterSync) {
            this.taskDetectedAfterSync = false;

            int oldSyncId = this.synchronizationId;
            this.synchronizationId++;
            this.gm.addSynchroToGraph(this.synchronizationId);
            if (this.synchronizationId > 1) {
                String oldSync = "Synchro" + oldSyncId;
                String currentSync = "Synchro" + this.synchronizationId;
                this.gm.addEdgeToGraph(oldSync, currentSync, edgeType, "");
            }
        }

        // Add edge from task to sync
        String src = String.valueOf(task.getId());
        String dest = "Synchro" + this.synchronizationId;
        this.gm.addEdgeToGraph(src, dest, edgeType, String.valueOf(dataId) + "v" + String.valueOf(dataVersion));
    }

    /**
     * We have explicitly called the barrier API. STEPS: Add a new synchronization node. Add an edge from last
     * synchronization point to barrier. Add edges from writer tasks to barrier.
     */
    private void addNewBarrier() {
        // Add barrier node
        int oldSync = this.synchronizationId;
        String oldSyncStr = "Synchro" + oldSync;

        this.synchronizationId++;
        this.taskDetectedAfterSync = false;
        this.gm.addBarrierToGraph(this.synchronizationId);

        // Add edge from last sync
        String newSyncStr = "Synchro" + this.synchronizationId;
        if (this.synchronizationId > 1) {
            this.gm.addEdgeToGraph(oldSyncStr, newSyncStr, EdgeType.USER_DEPENDENCY, "");
        }

        // Add edges from writers to barrier
        HashSet<Task> uniqueWriters = new HashSet<>();
        for (WritersInfo wi : this.writers.values()) {
            if (wi != null) {
                // Add data writers
                Task dataWriter = wi.getDataWriter();
                if (dataWriter != null) {
                    uniqueWriters.add(dataWriter);
                }
                // Add stream writers
                uniqueWriters.addAll(wi.getStreamWriters());
            }
        }
        for (Task writer : uniqueWriters) {
            if (writer != null && writer.getSynchronizationId() == oldSync) {
                String taskId = String.valueOf(writer.getId());
                this.gm.addEdgeToGraph(taskId, newSyncStr, EdgeType.USER_DEPENDENCY, "");
            }
        }
    }


    private static class WritersInfo {

        private final DataType dataType;
        private final Task dataWriter;
        private final List<Task> streamWriters;


        public WritersInfo(DataType dataType, Task dataWriter) {
            this.dataType = dataType;
            this.dataWriter = dataWriter;
            this.streamWriters = new ArrayList<>();
        }

        public WritersInfo(DataType dataType, List<Task> streamWriters) {
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

        public Task getDataWriter() {
            return this.dataWriter;
        }

        public List<Task> getStreamWriters() {
            return this.streamWriters;
        }

        public void addStreamWriter(Task writerTask) {
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
            for (Task t : this.streamWriters) {
                sb.append(t.getId()).append(" ");
            }
            sb.append("]");
            sb.append("]");

            return sb.toString();
        }

    }

}
