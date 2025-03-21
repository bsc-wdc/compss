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
package es.bsc.compss.types;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.api.impl.DoNothingApplicationMonitor;
import es.bsc.compss.checkpoint.CheckpointManager;
import es.bsc.compss.components.monitor.impl.GraphHandler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.info.CollectionInfo;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.info.FileInfo;
import es.bsc.compss.types.data.params.DataOwner;
import es.bsc.compss.types.request.ap.BarrierGroupRequest;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;

import java.security.SecureRandom;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;
import java.util.Random;
import java.util.Set;
import java.util.Stack;
import java.util.TimerTask;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Application implements ApplicationTaskMonitor, DataOwner {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);

    private static final Random APP_ID_GENERATOR = new SecureRandom();

    private static final TreeMap<Long, Application> APPLICATIONS = new TreeMap<>();
    private static final ApplicationRunner DEFAULT_RUNNER = new DoNothingApplicationMonitor();
    private static final Application NO_APPLICATION = new Application(null, null, DEFAULT_RUNNER);

    private static GraphHandler GH;
    private static CheckpointManager CP;

    private static final int DEFAULT_THROTTLE_WAIT_TASK_COUNT = Integer.MAX_VALUE;
    private static final Semaphore THROTTLE;

    static {
        String maxTasks = System.getenv(COMPSsConstants.COMPSS_THROTTLE_MAX_TASKS);
        int throttleThreshold;
        if (maxTasks != null && !maxTasks.isEmpty()) {
            throttleThreshold = Integer.parseInt(maxTasks);
        } else {
            throttleThreshold = DEFAULT_THROTTLE_WAIT_TASK_COUNT;
        }
        THROTTLE = new Semaphore(throttleThreshold);
    }

    /*
     * Application definition
     */
    // Id of the application
    private final Long id;
    // Parallelism source
    private final String parallelismSource;

    private TimerTask wallClockKiller;

    /*
     * Element running the main code of the application
     */
    private final ApplicationRunner runner;

    /*
     * Application state variables
     */
    // Task count
    private int totalTaskCount;

    /*
     * Application's task groups
     */
    // Task groups. Map: group name -> commutative group tasks
    private TreeMap<String, TaskGroup> taskGroups;
    // Registered task groups
    private Stack<TaskGroup> currentTaskGroups;

    /*
     * Application's Data
     */
    // Map: filename:host:path -> file identifier
    private final TreeMap<String, FileInfo> nameToData;
    // Map: hash code -> object identifier
    private final TreeMap<Integer, DataInfo> codeToData;
    // Map: collectionName -> collection identifier
    private final TreeMap<String, CollectionInfo> collectionToData;


    public static void setCP(CheckpointManager cp) {
        Application.CP = cp;
    }

    public static void setGH(GraphHandler gh) {
        Application.GH = gh;
    }

    /**
     * Returns the tasks state.
     *
     * @return A string representation of the tasks state.
     */
    public static String getTaskStateRequest() {
        StringBuilder sb = new StringBuilder("\t").append("<TasksInfo>").append("\n");
        for (Application app : APPLICATIONS.values()) {
            Long appId = app.getId();
            Integer totalTaskCount = app.totalTaskCount;
            TaskGroup appBaseGroup = app.currentTaskGroups.firstElement();
            Integer taskCount = appBaseGroup.getTasks().size();
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
     * Registers an application with Id @code{appId}. If the application has already been registered, it returns the
     * previous instance. Otherwise, it creates a new application instance.
     *
     * @param appId Id of the application to be registered
     * @return Application instance registered for that appId.
     */
    public static Application registerApplication(Long appId) {
        return registerApplication(appId, null, null);
    }

    /**
     * Registers a new application with a non-currently-used appId.
     *
     * @param parallelismSource element identifying the inner tasks
     * @param runner element running the main code of the application
     * @return Application instance registered.
     */
    public static Application registerApplication(String parallelismSource, ApplicationRunner runner) {
        Long appId = APP_ID_GENERATOR.nextLong();
        while (APPLICATIONS.containsKey(appId)) {
            appId = APP_ID_GENERATOR.nextLong();
        }
        return registerApplication(appId, parallelismSource, runner);
    }

    /**
     * Registers an application with Id @code{appId}. If the application has already been registered, it returns the
     * previous instance. Otherwise, it creates a new application instance.
     *
     * @param appId Id of the application to be registered
     * @param parallelismSource element identifying the inner tasks
     * @param runner element running the main code of the application
     * @return Application instance registered for that appId.
     */
    private static Application registerApplication(Long appId, String parallelismSource, ApplicationRunner runner) {
        Application app;
        if (appId == null) {
            LOGGER.error("No application id", new Exception("Application id is null"));
            app = NO_APPLICATION;
        } else {
            synchronized (APPLICATIONS) {
                app = APPLICATIONS.get(appId);
                if (app == null) {
                    if (runner == null) {
                        runner = DEFAULT_RUNNER;
                    }
                    app = new Application(appId, parallelismSource, runner);
                    APPLICATIONS.put(appId, app);
                }
            }
        }
        return app;
    }

    /**
     * Deregisters the application with Id @code{appId}.
     *
     * @param appId Id of the application to be remove
     * @return Application instance registered for that appId. Returns @literal{null}, if there was no application
     *         registered with that id.
     */
    public static Application deregisterApplication(Long appId) {
        Application app;
        synchronized (APPLICATIONS) {
            app = APPLICATIONS.remove(appId);
        }
        return app;
    }

    /**
     * Get all the registered applications.
     *
     * @return array with registered applications.
     */
    public static Application[] getApplications() {
        synchronized (APPLICATIONS) {
            return APPLICATIONS.values().toArray(new Application[APPLICATIONS.size()]);
        }
    }

    private Application(Long appId, String parallelismSource, ApplicationRunner runner) {
        this.id = appId;
        this.parallelismSource = parallelismSource;
        this.runner = runner;
        this.totalTaskCount = 0;
        this.currentTaskGroups = new Stack<>();
        this.taskGroups = new TreeMap<>();
        this.stackTaskGroup("App" + appId);
        this.nameToData = new TreeMap<>();
        this.codeToData = new TreeMap<>();
        this.collectionToData = new TreeMap<>();
    }

    public Long getId() {
        return this.id;
    }

    public String getParallelismSource() {
        return parallelismSource;
    }

    public GraphHandler getGH() {
        return GH;
    }

    public CheckpointManager getCP() {
        return CP;
    }

    /*
     * ----------------------------------- GROUP MANAGEMENT -----------------------------------
     */

    /**
     * Registers a new group of tasks to the application.
     *
     * @param groupName name of the group to register
     */
    public final void openTaskGroup(String groupName) {
        stackTaskGroup(groupName);
        this.GH.openTaskGroup(groupName);
    }

    private void stackTaskGroup(String groupName) {
        LOGGER.debug("Adding group " + groupName + " to the current groups stack.");
        TaskGroup tg = new TaskGroup(groupName, this);
        this.currentTaskGroups.push(tg);
        this.taskGroups.put(groupName, tg);
    }

    /**
     * Removes the peek of the TaskGroups stack.
     */
    public final void closeCurrentTaskGroup() {
        popGroup();
        this.GH.closeTaskGroup();
    }

    private void popGroup() {
        TaskGroup tg = this.currentTaskGroups.pop();
        tg.setClosed();
    }

    public Iterable<TaskGroup> getCurrentGroups() {
        return this.currentTaskGroups;
    }

    /**
     * Returns the TaskGroup with all the tasks of the application.
     *
     * @return TaskGroup with all the tasks of the application.
     */
    public TaskGroup getBaseTaskGroup() {
        return this.currentTaskGroups.firstElement();
    }

    /**
     * Returns the specified group if it belongs to the application.
     *
     * @param groupName name of the group
     * @return the specified group if it belongs to the application. If the group does not exist it returns null.
     */
    public TaskGroup getGroup(String groupName) {
        return this.taskGroups.get(groupName);
    }

    /**
     * Removes the specified task group from the application.
     *
     * @param name group name
     * @return the removed group
     */
    public TaskGroup removeGroup(String name) {
        return this.taskGroups.remove(name);
    }

    /*
     * ----------------------------------- EXECUTION MANAGEMENT -----------------------------------
     */

    @Override
    public void onTaskCreation(Task t) {
        // Check if throttle is exceeded and wait until throttle is correct.
        THROTTLE.acquireUninterruptibly();
        this.totalTaskCount++;
        getTaskMonitor().onCreation();
    }

    @Override
    public void onTaskAnalysisStart(Task task) {
        // Add task to the groups
        for (TaskGroup group : this.getCurrentGroups()) {
            task.addTaskGroup(group);
            group.addTask(task);
        }
        this.GH.startTaskAnalysis(task);
    }

    @Override
    public void onTaskAnalysisEnd(Task task, boolean taskHasEdge) {
        this.GH.endTaskAnalysis(task, taskHasEdge);

        // Prepare checkpointer for task
        this.CP.newTask(task);
    }

    @Override
    public void onCommutativeGroupCreation(CommutativeGroupTask g) {
        this.GH.createCommutativeGroup(g);
    }

    @Override
    public void onTaskBelongsToCommutativeGroup(Task t, CommutativeGroupTask g) {
        this.GH.taskBelongsToCommutativeGroup(t, g);

    }

    @Override
    public void onCommutativeGroupClosure(CommutativeGroupTask g) {
        this.GH.closeCommutativeGroup(g);
    }

    /**
     * Registers the end of a task execution belonging to the application and removes it from all the groups it belongs
     * to.
     *
     * @param task finished task to be removed
     */
    public void endTask(Task task) {
        THROTTLE.release();
        for (TaskGroup group : task.getTaskGroupList()) {
            group.removeTask(task);
            LOGGER.debug("Group " + group.getName() + " released task " + task.getId());
            if (!group.hasPendingTasks()) {
                LOGGER.debug("All tasks of group " + group.getName() + " have finished execution");
                if (group.hasBarrier()) {
                    group.releaseBarrier();
                    if (group.isClosed()) {
                        removeGroup(group.getName());
                    }
                }
            }
        }
    }

    /**
     * The application's main code cannot make no progress until further notice.
     */
    public void stalled() {
        this.runner.stalledApplication();
    }

    /**
     * The application's main code can resume the execution.
     *
     * @param sem notify when the runner is ready to continue
     */
    public void readyToContinue(Semaphore sem) {
        this.runner.readyToContinue(sem);
    }

    private void reachesGroupBarrier(TaskGroup tg, Barrier request) {
        if (tg != null) {
            tg.registerBarrier(request);
        } else {
            request.release();
        }
    }

    /**
     * Registers that the application has reached a group barrier and the execution thread is waiting for all the tasks
     * to complete.
     *
     * @param groupName name of group holding the barrier
     * @param request request that waits for the barrier
     */
    public final void reachesGroupBarrier(String groupName, BarrierGroupRequest request) {
        TaskGroup tg = this.getGroup(groupName);
        reachesGroupBarrier(tg, request);
        this.GH.groupBarrier(request);
    }

    /**
     * Registers that the application has reached a barrier and the execution thread is waiting for all the tasks to
     * complete.
     *
     * @param barrier barrier object to indicate that all task have finished.
     */
    public final void reachesBarrier(Barrier barrier) {
        doBarrier(barrier);
        this.GH.barrier(this.nameToData, this.codeToData, this.collectionToData);
    }

    /**
     * Registers that the application has reached its end, no more tasks will be generated.
     *
     * @param barrier barrier object to indicate that all task have finished.
     */
    public final void endReached(Barrier barrier) {
        doBarrier(barrier);
        this.GH.endApp();
    }

    private void doBarrier(Barrier barrier) {
        TaskGroup baseGroup = this.currentTaskGroups.firstElement();
        this.reachesGroupBarrier(baseGroup, barrier);
    }

    /*
     * ----------------------------------- DATA MANAGEMENT -----------------------------------
     */

    @Override
    public void registerFileData(String locationKey, FileInfo di) {
        this.nameToData.put(locationKey, di);
    }

    @Override
    public FileInfo getFileData(String locationKey) {
        return this.nameToData.get(locationKey);
    }

    @Override
    public FileInfo removeFileData(String locationKey) throws ValueUnawareRuntimeException {
        FileInfo di = this.nameToData.remove(locationKey);
        removeData(di);
        return di;
    }

    @Override
    public void registerObjectData(int code, DataInfo di) {
        this.codeToData.put(code, di);
    }

    @Override
    public DataInfo getObjectData(int code) {
        return this.codeToData.get(code);
    }

    @Override
    public DataInfo removeObjectData(int code) throws ValueUnawareRuntimeException {
        DataInfo di = this.codeToData.remove(code);
        removeData(di);
        return di;
    }

    @Override
    public void registerCollectionData(String collectionId, CollectionInfo di) {
        this.collectionToData.put(collectionId, di);
    }

    @Override
    public CollectionInfo getCollectionData(String collectionId) {
        return this.collectionToData.get(collectionId);
    }

    @Override
    public CollectionInfo removeCollectionData(String collectionId) throws ValueUnawareRuntimeException {
        CollectionInfo di = this.collectionToData.remove(collectionId);
        removeData(di);
        return di;
    }

    private void removeData(DataInfo dataInfo) throws ValueUnawareRuntimeException {
        if (dataInfo == null) {
            throw new ValueUnawareRuntimeException();
        }
        // We delete the data associated with all the versions of the same object
        dataInfo.delete();
    }

    /**
     * Removes all the Data generated by the application.
     *
     * @return list of all the removed data.
     */
    public List<DataInfo> popAllData() {

        List<DataInfo> localData = new LinkedList<>();
        localData.addAll(this.nameToData.values());
        this.nameToData.clear();
        localData.addAll(this.codeToData.values());
        this.codeToData.clear();
        localData.addAll(this.collectionToData.values());
        this.collectionToData.clear();
        return localData;
    }

    /**
     * Returns a set with all the FileIds written by the application.
     *
     * @return set with all the DataIds corresponding to files written by the application.
     */
    public Set<FileInfo> getWrittenFiles() {
        Set<FileInfo> wfiles = new HashSet<>();
        for (DataInfo di : this.nameToData.values()) {
            if (di.getCurrentDataVersion().getDataInstanceId().getVersionId() > 1) {
                wfiles.add((FileInfo) di);
            }
        }
        return wfiles;
    }

    public void setTimerTask(WallClockTimerTask wcTimerTask) {
        this.wallClockKiller = wcTimerTask;
    }

    /**
     * Cancel the wall clock time timer task.
     */
    public void cancelTimerTask() {
        if (this.wallClockKiller != null) {
            wallClockKiller.cancel();
            wallClockKiller = null;
        }
    }

    public TaskMonitor getTaskMonitor() {
        return this.runner.getTaskMonitor();
    }

}
