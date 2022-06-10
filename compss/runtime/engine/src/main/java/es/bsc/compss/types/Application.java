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
package es.bsc.compss.types;

import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.DataInfo;

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


public class Application {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);

    private static final Random APP_ID_GENERATOR = new SecureRandom();

    private static final TreeMap<Long, Application> APPLICATIONS = new TreeMap<>();
    private static final Application NO_APPLICATION = new Application(null, null, null);

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
    // Application has already reached its end
    private boolean ending = false;

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
    // Data registered by the application
    private LinkedList<DataInfo> data;
    // Map: filename:host:path -> file identifier
    private final TreeMap<String, Integer> nameToId;

    // Set of written data ids (for result files)
    private Set<Integer> writtenFileDataIds;
    // Set of written data ids (for result SCOs)
    private Set<Integer> writtenPSCODataIds;


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
     * Registers a new application with a non-currently-used appId.
     *
     * @return Application instance registered.
     */
    public static Application registerApplication() {
        return registerApplication(null, null);
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
    public static Application registerApplication(Long appId, String parallelismSource, ApplicationRunner runner) {
        Application app;
        if (appId == null) {
            LOGGER.error("No application id", new Exception("Application id is null"));
            app = NO_APPLICATION;
        } else {
            synchronized (APPLICATIONS) {
                app = APPLICATIONS.get(appId);
                if (app == null) {
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
     * Removes a data as an output File of any task.
     *
     * @param dataId Id of the data to be removed
     */
    public static void removeWrittenFileIdFromAllApps(int dataId) {
        synchronized (APPLICATIONS) {
            for (Application app : APPLICATIONS.values()) {
                app.removeWrittenFileId(dataId);
            }
        }
    }

    /**
     * Removes a data as an output PSCO of any task.
     *
     * @param dataId Id of the data to be removed
     */
    public static void removeWrittenPSCOIdFromAllApps(int dataId) {
        synchronized (APPLICATIONS) {
            for (Application app : APPLICATIONS.values()) {
                app.removeWrittenPSCOId(dataId);
            }
        }
    }

    private Application(Long appId, String parallelismSource, ApplicationRunner runner) {
        this.id = appId;
        this.parallelismSource = parallelismSource;
        this.runner = runner;
        this.totalTaskCount = 0;
        this.ending = false;
        this.currentTaskGroups = new Stack<>();
        this.taskGroups = new TreeMap<>();
        this.stackTaskGroup("App" + appId);
        this.data = new LinkedList<>();
        this.nameToId = new TreeMap<>();
        this.writtenFileDataIds = new HashSet<>();
        this.writtenPSCODataIds = new HashSet<>();
    }

    public Long getId() {
        return this.id;
    }

    public String getParallelismSource() {
        return parallelismSource;
    }

    /*
     * ----------------------------------- GROUP MANAGEMENT -----------------------------------
     */
    /**
     * Registers a new group of tasks to the application.
     *
     * @param groupName name of the group to register
     */
    public final void stackTaskGroup(String groupName) {
        LOGGER.debug("Adding group " + groupName + " to the current groups stack.");
        TaskGroup tg = new TaskGroup(groupName, this);
        this.currentTaskGroups.push(tg);
        this.taskGroups.put(groupName, tg);
    }

    /**
     * Removes and returns the peek of the TaskGroups stack.
     */
    public final void popGroup() {
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
    /**
     * Registers the existence of a new task for the application and registers it into all the currently open groups.
     *
     * @param task task to be added to the application's task groups
     */
    public void newTask(Task task) {
        this.totalTaskCount++;
        // Add task to the groups
        for (TaskGroup group : this.getCurrentGroups()) {
            task.addTaskGroup(group);
            group.addTask(task);
        }
    }

    /**
     * Registers the end of a task execution belonging to the application and removes it from all the groups it belongs
     * to.
     *
     * @param task finished task to be removed
     */
    public void endTask(Task task) {
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
        if (runner != null) {
            this.runner.stalledApplication();
        }
    }

    /**
     * The application's main code can resume the execution.
     * 
     * @param sem notify when the runner is ready to continue
     */
    public void readyToContinue(Semaphore sem) {
        if (this.runner != null) {
            this.runner.readyToContinue(sem);
        } else {
            sem.release();
        }
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
    public final void reachesGroupBarrier(String groupName, Barrier request) {
        TaskGroup tg = this.getGroup(groupName);
        reachesGroupBarrier(tg, request);
    }

    /**
     * Registers that the application has reached a barrier and the execution thread is waiting for all the tasks to
     * complete.
     *
     * @param barrier barrier object to indicate that all task have finished.
     */
    public final void reachesBarrier(Barrier barrier) {
        TaskGroup baseGroup = this.currentTaskGroups.firstElement();
        this.reachesGroupBarrier(baseGroup, barrier);
    }

    /**
     * Registers that the application has reached its end, no more tasks will be generated.
     *
     * @param barrier barrier object to indicate that all task have finished.
     */
    public final void endReached(Barrier barrier) {
        this.ending = true;
        reachesBarrier(barrier);
    }

    /**
     * Returns @literal{true} if the application has reached its end and won't generate more tasks.
     *
     * @return @literal{true} if the application has reached its end and won't generate more tasks
     */
    public final boolean isEnding() {
        return this.ending;
    }

    /*
     * ----------------------------------- DATA MANAGEMENT -----------------------------------
     */
    /**
     * Adds a data as registered by the application.
     *
     * @param di Data registered by the application.
     */
    public void addData(DataInfo di) {
        this.data.add(di);
    }

    /**
     * Stores the relation between a file and the corresponding dataInfo.
     *
     * @param locationKey file location
     * @param di data registered by the application
     */
    public void registerFileData(String locationKey, DataInfo di) {
        this.nameToId.put(locationKey, di.getDataId());
    }

    /**
     * Returns the Data Id related to a file.
     *
     * @param locationKey file location
     * @return data Id related to the file
     */
    public Integer getFileDataId(String locationKey) {
        return nameToId.get(locationKey);
    }

    /**
     * Removes any data association related to file location.
     *
     * @param locationKey file location
     */
    public void removeFileData(String locationKey) {
        this.nameToId.remove(locationKey);
    }

    /**
     * Removes a data value as generated by the application.
     *
     * @param di Data registered by the application that will be removed
     */
    public void removeData(DataInfo di) {
        this.data.remove(di);
    }

    /**
     * Removes all the Data generated by the application.
     *
     * @return list of all the removed data.
     */
    public List<DataInfo> popAllData() {
        List<DataInfo> localData = this.data;
        this.data = new LinkedList<>();
        return localData;
    }

    /**
     * Adds a data as an output file of the task.
     *
     * @param dataId data to be registered as a file output.
     */
    public void addWrittenFileId(int dataId) {
        this.writtenFileDataIds.add(dataId);
    }

    /**
     * REmoves a data as an output file of the task.
     *
     * @param dataId data to be unregistered as a file output.
     */
    public void removeWrittenFileId(int dataId) {
        if (this.writtenFileDataIds.remove(dataId)) {
            LOGGER.info(" Removed data " + dataId + " from written files");
        }
    }

    /**
     * Returns a set with all the FileIds written by the application.
     *
     * @return set with all the DataIds corresponding to files written by the application.
     */
    public Set<Integer> getWrittenFileIds() {
        return this.writtenFileDataIds;
    }

    /**
     * Adds a data as an output PSCO of the task.
     *
     * @param dataId data to be registered as a PSCO output.
     */
    public void addWrittenPSCOId(int dataId) {
        this.writtenPSCODataIds.add(dataId);
    }

    /**
     * Removes a data as an output PSCO of the task.
     *
     * @param dataId data to be deregistered as a PSCO output.
     */
    public void removeWrittenPSCOId(int dataId) {
        if (this.writtenPSCODataIds.remove(dataId)) {
            LOGGER.info(" Removed data " + dataId + " from written pscos");
        }
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

}
