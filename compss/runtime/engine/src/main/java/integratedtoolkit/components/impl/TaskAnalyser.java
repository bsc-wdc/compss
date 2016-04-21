package integratedtoolkit.components.impl;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.ITExecution.ParamType;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.Task.TaskState;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.TaskParams.Type;
import integratedtoolkit.types.data.AccessParams.AccessMode;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataAccessId.RWAccessId;
import integratedtoolkit.types.data.DataAccessId.WAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.data.FileInfo;
import integratedtoolkit.types.data.operation.ResultListener;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.FileParameter;
import integratedtoolkit.types.parameter.ObjectParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.parameter.SCOParameter;
import integratedtoolkit.types.request.ap.EndOfAppRequest;
import integratedtoolkit.types.request.ap.WaitForTaskRequest;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ElementNotFoundException;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Graph;

import java.util.HashMap;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map.Entry;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.concurrent.Semaphore;

import org.apache.log4j.Logger;

public class TaskAnalyser {

    // Constants definition
    private static final String TASK_FAILED = "Task failed: ";
    // Components
    private DataInfoProvider DIP;
    private TaskDispatcher TD;
    // Dependency graph
    private Graph<Integer, Task> depGraph;
    // <File id, Last writer task> table
    private TreeMap<Integer, Integer> writers;
    // Method information
    private HashMap<Integer, Integer> currentTaskCount;
    // Map: app id -> task count
    private HashMap<Long, Integer> appIdToTotalTaskCount;
    // Map: app id -> task count
    private HashMap<Long, Integer> appIdToTaskCount;
    // Map: app id -> semaphore to notify end of app
    private HashMap<Long, Semaphore> appIdToSemaphore;
    // Map: app id -> set of written data ids (for result files)
    private HashMap<Long, TreeSet<Integer>> appIdToWrittenFiles;
    // Map: app id -> set of written data ids (for result SCOs)
    private HashMap<Long, TreeSet<Integer>> appIdToSCOWrittenIds;    
    // Tasks being waited on: taskId -> list of semaphores where to notify end of task
    private Hashtable<Integer, List<Semaphore>> waitedTasks;

    // Logger
    private static final Logger logger = Logger.getLogger(Loggers.TA_COMP);
    private static final boolean debug = logger.isDebugEnabled();
    // Graph drawing
    private static final boolean drawGraph = System.getProperty(ITConstants.IT_GRAPH) != null
            && System.getProperty(ITConstants.IT_GRAPH).equals("true") ? true : false;

    private static int synchronizationId;

    public TaskAnalyser() {
        depGraph = new Graph<Integer, Task>();

        currentTaskCount = new HashMap<Integer, Integer>();
        writers = new TreeMap<Integer, Integer>();
        appIdToTaskCount = new HashMap<Long, Integer>();
        appIdToTotalTaskCount = new HashMap<Long, Integer>();
        appIdToSemaphore = new HashMap<Long, Semaphore>();
        appIdToWrittenFiles = new HashMap<Long, TreeSet<Integer>>();
        waitedTasks = new Hashtable<Integer, List<Semaphore>>();
        synchronizationId = 0;
        logger.info("Initialization finished");
    }

    public void setCoWorkers(DataInfoProvider DIP, TaskDispatcher TD) {
        this.DIP = DIP;
        this.TD = TD;
    }

    public Task getTask(int taskId) {
        return depGraph.get(taskId);
    }

    public void processTask(Task currentTask) {
        TaskParams params = currentTask.getTaskParams();
        logger.info("New " + (params.getType() == Type.METHOD ? "method" : "service") + " task(" + params.getName() + "), ID = " + currentTask.getId());
        if (drawGraph) {
            RuntimeMonitor.addTaskToGraph(currentTask);
            if (synchronizationId > 0) {
                RuntimeMonitor.addEdgeToGraph("Synchro" + synchronizationId, String.valueOf(currentTask.getId()), "");
            }
        }

        int currentTaskId = currentTask.getId();
        Parameter[] parameters = params.getParameters();
        depGraph.addNode(currentTaskId, currentTask);

        // Update task count
        Integer methodId = params.getId();
        Integer actualCount = currentTaskCount.get(methodId);
        if (actualCount == null) {
            actualCount = 0;
        }
        currentTaskCount.put(methodId, actualCount + 1);

        // Update app id task count
        Long appId = currentTask.getAppId();
        Integer taskCount = appIdToTaskCount.get(appId);
        if (taskCount == null) {
            taskCount = 0;
        }
        taskCount++;
        appIdToTaskCount.put(appId, taskCount);
        Integer totalTaskCount = appIdToTotalTaskCount.get(appId);
        if (totalTaskCount == null) {
            totalTaskCount = 0;
        }
        totalTaskCount++;
        appIdToTotalTaskCount.put(appId, totalTaskCount);

        boolean isWaiting = true;
        for (Parameter p : parameters) {
            if (debug) {
                logger.debug("* Parameter : " + p);
            }

            // Conversion: direction -> access mode
            AccessMode am = null;
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
                    daId = DIP.registerFileAccess(am, fp.getLocation(), methodId);
                    break;
                    
                case SCO_T:
                case PSCO_T:
            		SCOParameter sco = (SCOParameter) p;
                    daId = DIP.registerObjectAccess(am, sco.getValue(), sco.getCode(), methodId);
            		break;                                        

                case OBJECT_T:
                    ObjectParameter op = (ObjectParameter) p;
                    daId = DIP.registerObjectAccess(am, op.getValue(), op.getCode(), methodId);
                    break;

                default:
                    /* Basic types (including String).
                     * The only possible access mode is R (already checked by the API)
                     */
                    continue;
            }

            //Add dependencies to the graph and register output values for future dependencies
            DependencyParameter dp = (DependencyParameter) p;
            dp.setDataAccessId(daId);
            switch (am) {
                case R:
                    isWaiting = checkDependencyForRead(currentTask, dp) && isWaiting;
                    break;

                case RW:
                    isWaiting = checkDependencyForRead(currentTask, dp) && isWaiting;
                    registerOutputValues(currentTask, dp);
                    break;

                case W:
                    registerOutputValues(currentTask, dp);
                    break;
            }

        }

        //Check scheduling enforcing data
        if (params.getType() == TaskParams.Type.SERVICE && params.hasTargetObject()) {
            checkSchedulingConstraints(currentTask);
        }

        try {
            if (!depGraph.hasPredecessors(currentTaskId)) {
                // No dependencies for this task, schedule
                if (debug) {
                    logger.debug("Task " + currentTaskId + " has NO dependencies, send for schedule");
                }
                List<Task> s = new LinkedList<Task>();
                s.add(currentTask);
                currentTaskCount.put(params.getId(), currentTaskCount.get(params.getId()) - 1);
                TD.scheduleTasks(s, false, new int[CoreManager.getCoreCount()]);
            } else if (isWaiting) {
                TD.newWaitingTask(methodId);
            }
        } catch (ElementNotFoundException e) {
            ErrorManager.fatal("Error checking dependencies for task " + currentTaskId, e);
            return;
        }
    }

    private boolean checkDependencyForRead(Task currentTask, DependencyParameter dp) {
        int dataId = dp.getDataAccessId().getDataId();
        Integer lastWriterId = writers.get(dataId);
        if (lastWriterId != null
                && lastWriterId > 0
                && depGraph.get(lastWriterId) != null
                && lastWriterId != currentTask.getId()) { // avoid self-dependencies

            if (debug) {
                logger.debug("Last writer for datum " + dp.getDataAccessId().getDataId() + " is task " + lastWriterId);
                logger.debug("Adding dependency between task " + lastWriterId + " and task " + currentTask.getId());
            }

            if (drawGraph) {
                try {
                    boolean b = true;
                    for (Task t : depGraph.getSuccessors(lastWriterId)) {
                        if (t.getId() == currentTask.getId()) {
                            b = false;
                        }
                    }
                    if (b) {
                        RuntimeMonitor.addEdgeToGraph(String.valueOf(lastWriterId), String.valueOf(currentTask.getId()), String.valueOf(dp.getDataAccessId().getDataId()));
                    }
                } catch (Exception e) {
                    logger.error("Error drawing dependency in graph", e);
                }
            }

            try {
                depGraph.addEdge(lastWriterId, currentTask.getId());
                return !depGraph.hasPredecessors(lastWriterId); // check if it's a second-level task for this predecessor
            } catch (ElementNotFoundException e) {
                ErrorManager.fatal("Error when adding a dependency between tasks "
                        + lastWriterId + " and " + currentTask.getId(), e);
            }
        } else if (drawGraph && lastWriterId != null && lastWriterId < 0) {
            RuntimeMonitor.addEdgeToGraph("Synchro" + (-lastWriterId), String.valueOf(currentTask.getId()), "");
        }
        return true;
    }

    public void registerOutputValues(Task currentTask, DependencyParameter dp) {
        int currentTaskId = currentTask.getId();
        int dataId = dp.getDataAccessId().getDataId();
        Long appId = currentTask.getAppId();
        writers.put(dataId, currentTaskId); // update global last writer
        if (dp.getType() == ParamType.FILE_T) { // Objects are not checked, their version will be only get if the main access them
            TreeSet<Integer> idsWritten = appIdToWrittenFiles.get(appId);
            if (idsWritten == null) {
                idsWritten = new TreeSet<Integer>();
                appIdToWrittenFiles.put(appId, idsWritten);
            }
            idsWritten.add(dataId);
        }
        if (dp.getType() == ParamType.PSCO_T) { 
            TreeSet<Integer> idsWritten = appIdToSCOWrittenIds.get(appId);
            if (idsWritten == null) {
                idsWritten = new TreeSet<Integer>();
                appIdToSCOWrittenIds.put(appId, idsWritten);
            }
            idsWritten.add(dataId);
        }           
        if (debug) {
            logger.debug("New writer for datum " + dp.getDataAccessId().getDataId() + " is task " + currentTaskId);
        }
    }

    public void checkSchedulingConstraints(Task task) {
        DependencyParameter dependentParameter;
        DataInstanceId dependingDataId = null;
        TaskParams params = task.getTaskParams();
        if (params.hasReturnValue()) {
            dependentParameter = (DependencyParameter) params.getParameters()[params.getParameters().length - 2];
        } else {
            dependentParameter = (DependencyParameter) params.getParameters()[params.getParameters().length - 1];
        }
        switch (dependentParameter.getDirection()) {
            case IN:
                DataAccessId.RAccessId raId = (DataAccessId.RAccessId) dependentParameter.getDataAccessId();
                dependingDataId = raId.getReadDataInstance();
                break;
            case INOUT:
                DataAccessId.RWAccessId rwaId = (DataAccessId.RWAccessId) dependentParameter.getDataAccessId();
                dependingDataId = rwaId.getReadDataInstance();
                break;
            case OUT:
                break;
        }

        if (dependingDataId != null) {
            if (params.getType() == TaskParams.Type.SERVICE && dependingDataId.getVersionId() > 1) {
                task.setEnforcingData(dependingDataId);
                task.forceStrongScheduling();
            }
        }
    }

    public void updateGraph(Task task, int implId, Worker<?> resource) {
        int taskId = task.getId();
        if (task.getStatus() == TaskState.FAILED) {
            ErrorManager.warn(TASK_FAILED + task);
        }

        // Update app id task count
        Long appId = task.getAppId();
        Integer taskCount = appIdToTaskCount.get(appId) - 1;
        appIdToTaskCount.put(appId, taskCount);
        if (taskCount == 0) {
            Semaphore sem = appIdToSemaphore.remove(appId);
            if (sem != null) { // App has notified that no more tasks are coming
                appIdToTaskCount.remove(appId);
                sem.release();
            }
        }

        // Check if task is being waited
        List<Semaphore> sems = waitedTasks.remove(taskId);
        if (sems != null) {
            for (Semaphore sem : sems) {
                sem.release();
            }
        }

        LinkedList<DataAccessId> readedData = new LinkedList<DataAccessId>();
        for (Parameter param : task.getTaskParams().getParameters()) {
            ParamType type = param.getType();

            if (type == ParamType.FILE_T || type == ParamType.OBJECT_T || type == ParamType.SCO_T || type == ParamType.PSCO_T) {
                DependencyParameter dPar = (DependencyParameter) param;
                DataAccessId dAccId = dPar.getDataAccessId();
                readedData.add(dAccId);
            }
        }

        DIP.dataHasBeenRead(readedData, task.getTaskParams().getId());

        // Dependency-free tasks
        List<Task> toSchedule = new LinkedList<Task>();

        try {
            Iterator<Task> i = depGraph.getIteratorOverSuccessors(taskId);
            while (i.hasNext()) {
                Task succ = i.next();
                int succId = succ.getId();

                // Remove the dependency
                depGraph.removeEdge(taskId, succId);

                // Schedule if task has no more dependencies
                if (!depGraph.hasPredecessors(succId)) {
                    toSchedule.add(succ);
                }
            }
        } catch (ElementNotFoundException e) {
            ErrorManager.fatal("Error removing the dependencies of task " + taskId, e);
        }

        if (!toSchedule.isEmpty()) {

            if (debug) {
                StringBuilder sb = new StringBuilder("All dependencies solved for tasks: ");
                for (Task t : toSchedule) {
                    sb.append(t.getId()).append("(").append(t.getTaskParams().getName()).append(") ");
                }
                logger.debug(sb);
            }

            int[] successors = new int[CoreManager.getCoreCount()];
            for (Task t : toSchedule) {
                try {
                    Iterator<Task> i = depGraph.getIteratorOverSuccessors(t.getId());
                    while (i.hasNext()) {
                        Task succ = i.next();
                        int succId = succ.getId();

                        boolean hasPreds = false;
                        Iterator<Task> j = depGraph.getIteratorOverPredecessors(succId);
                        while (j.hasNext()) {
                            Task pred = j.next();
                            int predId = pred.getId();
                            hasPreds = hasPreds || depGraph.hasPredecessors(predId);
                        }
                        if (!hasPreds) {

                            successors[t.getTaskParams().getId()]++;
                        }
                    }
                    currentTaskCount.put(t.getTaskParams().getId(), currentTaskCount.get(t.getTaskParams().getId()) - 1);
                } catch (Exception e) {
                    ErrorManager.fatal("Error updating the waiting tasks of " + taskId, e);
                }
            }

            TD.scheduleTasks(toSchedule, true, successors);
        }

        TD.notifyJobEnd(task, implId, resource);

        // Add the task to the set of finished tasks
        //finishedTasks.add(task);
        // Check if the finished task was the last writer of a file, but only if task generation has finished
        if (appIdToSemaphore.get(appId) != null) {
            checkResultFileTransfer(task);
        }

        depGraph.removeNode(taskId);
    }

    // Private method to check if a finished task is the last writer of its file parameters and eventually order the necessary transfers
    private void checkResultFileTransfer(Task t) {
        LinkedList<DataInstanceId> fileIds = new LinkedList<DataInstanceId>();
        for (Parameter p : t.getTaskParams().getParameters()) {
            switch (p.getType()) {
                case FILE_T:
                    FileParameter fp = (FileParameter) p;
                    switch (fp.getDirection()) {
                        case IN:
                            break;
                        case INOUT:
                            DataInstanceId dId = ((RWAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                            if (writers.get(dId.getDataId()) == t.getId()) {
                                fileIds.add(dId);
                            }
                            break;
                        case OUT:
                            dId = ((WAccessId) fp.getDataAccessId()).getWrittenDataInstance();
                            if (writers.get(dId.getDataId()) == t.getId()) {
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
            //List<ResultFile> resFiles = new ArrayList<ResultFile>(numFT);
            for (DataInstanceId fileId : fileIds) {
                try {
                    int id = fileId.getDataId();
                    DIP.blockDataAndGetResultFile(id, new ResultListener(new Semaphore(0)));
                    DIP.unblockDataId(id);
                } catch (Exception e) {
                }
            }

        }
    }

    public void findWaitedTask(WaitForTaskRequest request) {
        int dataId = request.getDataId();
        AccessMode am = request.getAccessMode();
        Semaphore sem = request.getSemaphore();
        Integer lastWriterId = writers.get(dataId);
        if (drawGraph && lastWriterId != null) {
            if (am == AccessMode.RW) {
                writers.put(dataId, -synchronizationId);
            }
            synchronizationId++;
            try {
                RuntimeMonitor.addSynchroToGraph(synchronizationId);
                RuntimeMonitor.addEdgeToGraph(String.valueOf(lastWriterId), "Synchro" + synchronizationId, String.valueOf(dataId));
                if (synchronizationId > 0) {
                    RuntimeMonitor.addEdgeToGraph("Synchro" + (synchronizationId - 1), "Synchro" + synchronizationId, String.valueOf(dataId));
                }
            } catch (Exception e) {
                logger.error("Error adding task to graph file", e);
            }
        }
        if (lastWriterId == null || depGraph.get(lastWriterId) == null) {
            sem.release();
        } else {
            List<Semaphore> list = waitedTasks.get(lastWriterId);
            if (list == null) {
                list = new LinkedList<Semaphore>();
                waitedTasks.put(lastWriterId, list);
            }
            list.add(sem);
        }
    }

    public void noMoreTasks(EndOfAppRequest request) {
        Long appId = request.getAppId();
        Integer count = appIdToTaskCount.get(appId);
        if (drawGraph) {
            RuntimeMonitor.commitGraph();
        }
        if (count == null || count == 0) {
            appIdToTaskCount.remove(appId);
            request.getSemaphore().release();
        } else {
            appIdToSemaphore.put(appId, request.getSemaphore());
        }
    }

    public TreeSet<Integer> getAndRemoveWrittenFiles(Long appId) {
        TreeSet<Integer> data = appIdToWrittenFiles.remove(appId);
        return data;
    }

    public void shutdown() {
        if (drawGraph) {
            RuntimeMonitor.removeTemporaryGraph();
        }
    }

    public String getGraphDOTFormat() {
        return depGraph.getGraphDotFormat();
    }

    public String getTaskStateRequest() {
        StringBuilder sb = new StringBuilder("\t<TasksInfo>\n");
        for (Entry<Long, Integer> e : appIdToTotalTaskCount.entrySet()) {
            Long appId = e.getKey();
            Integer totalTaskCount = e.getValue();
            Integer taskCount = appIdToTaskCount.get(appId);
            if (taskCount == null) {
                taskCount = 0;
            }
            int completed = totalTaskCount - taskCount;
            sb.append("\t\t<Application id=\"").append(appId).append("\">\n");
            sb.append("\t\t\t<TotalCount>").append(totalTaskCount).append("</TotalCount>\n");
            sb.append("\t\t\t<InProgress>").append(taskCount).append("</InProgress>\n");
            sb.append("\t\t\t<Completed>").append(completed).append("</Completed>\n");
            sb.append("\t\t</Application>\n");
        }
        sb.append("\t</TasksInfo>\n");
        return sb.toString();
    }

    public void deleteFile(FileInfo fileInfo) {
        int dataId = fileInfo.getDataId();
        Integer taskId = writers.get(dataId);
        if (taskId != null) {
            Task task = depGraph.get(taskId);
            if (task != null) {
                return;
            }
        }
        for (TreeSet<Integer> files : appIdToWrittenFiles.values()) {
            files.remove(fileInfo.getDataId());
        }
    }
}
