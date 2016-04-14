package integratedtoolkit.util;

import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import java.util.LinkedList;
import integratedtoolkit.types.Task;


/**
 * The QueueManager class is an utility to manage the schedule of all the
 * dependency-free tasks. It controls if they are running, if they have been
 * scheduled in a resource slot queue, if they failed on its previous execution
 * and must be rescheduled or if they have no resource where to run.
 *
 * There are many queues: - tasks without resource where to run - tasks to be
 * rescheduled - one queue for each slot of all the resources
 */
public class TaskSets {

    // Pending tasks
    /**
     * Tasks with no resource where they can be run
     */
    private LinkedList<Task>[] noResourceTasks;

    /**
     * Task to be rescheduled
     */
    private LinkedList<Task>[] tasksToReschedule;

    /**
     * Tasks with priority
     */
    private LinkedList<Task>[] priorityTasks;

    /**
     * Regular tasks
     */
    private LinkedList<Task>[] regularTasks;

    /**
     * Amount of tasks per core that can't be run
     */
    private int noResourceCount;

    /**
     * Amount of tasks per core to be rescheduled
     */
    private int toRescheduleCount;

    /**
     * Amount of priority tasks per core
     */
    private int priorityCount;

    /**
     * Amount of regular tasks
     */
    private int regularCount;

    /**
     * Constructs a new QueueManager
     *
     */
    public TaskSets() {
        int coreCount = CoreManager.getCoreCount();
        if (noResourceTasks == null) {
            noResourceCount = 0;
            noResourceTasks = new LinkedList[coreCount];
            for (int i = 0; i < coreCount; i++) {
                noResourceTasks[i] = new LinkedList<Task>();
            }
        } else {
            for (int i = 0; i < coreCount; i++) {
                noResourceTasks[i].clear();
            }
        }

        if (tasksToReschedule == null) {
            toRescheduleCount = 0;
            tasksToReschedule = new LinkedList[coreCount];
            for (int i = 0; i < coreCount; i++) {
                tasksToReschedule[i] = new LinkedList<Task>();
            }
        } else {
            for (int i = 0; i < coreCount; i++) {
                tasksToReschedule[i].clear();
            }
        }

        if (priorityTasks == null) {
            priorityCount = 0;
            priorityTasks = new LinkedList[coreCount];
            for (int i = 0; i < coreCount; i++) {
                priorityTasks[i] = new LinkedList<Task>();
            }
        } else {
            for (int i = 0; i < coreCount; i++) {
                priorityTasks[i].clear();
            }
        }

        if (regularTasks == null) {
            regularCount = 0;
            regularTasks = new LinkedList[coreCount];
            for (int i = 0; i < coreCount; i++) {
                regularTasks[i] = new LinkedList<Task>();
            }
        } else {
            for (int i = 0; i < coreCount; i++) {
                regularTasks[i].clear();
            }
        }
    }

    /**
     * ** NO RESOURCE TASKS MANAGEMENT *****
     */
    /**
     * Adds a task to the queue of tasks with no resource
     *
     * @param t Task to be added
     */
    public void waitWithoutNode(Task t) {
        noResourceCount++;
        noResourceTasks[t.getTaskParams().getId()].add(t);
    }

    /**
     * Removes from the queue of tasks with no resources all the tasks of a
     * specific core
     *
     * @param coreId core identifier that can be executed
     */
    public void resourceFound(int coreId) {
        noResourceCount -= noResourceTasks[coreId].size();
        for (Task t : noResourceTasks[coreId]) {
            if (t.getTaskParams().hasPriority()) {
                priorityTasks[coreId].add(t);
                priorityCount++;
            } else {
                regularTasks[coreId].add(t);
                regularCount++;
            }
        }
        noResourceTasks[coreId].clear();
    }

    /**
     * Gets the amount of task without resource that execute a specific core
     *
     * @param coreId identifier of the core
     * @return amount of task without resource that execute a specific core
     */
    public int getNoResourceCount(int coreId) {
        return noResourceTasks[coreId].size();
    }

    /**
     * Gets the amount of task without resource
     *
     * @return amount of task without resource
     */
    public int getNoResourceCount() {
        return noResourceCount;
    }

    /**
     * Returns the whole list of tasks without resource
     *
     * @return The whole list of tasks without resource
     */
    public LinkedList<Task>[] getPendingTasksWithoutNode() {
        return noResourceTasks;
    }

    /**
     * ** TO RESCHEDULE TASKs MANAGEMENT *****
     */
    /**
     * Adds a task to the queue of tasks to be rescheduled
     *
     * @param t Task to be added
     */
    public void newTaskToReschedule(Task t) {
        toRescheduleCount++;
        tasksToReschedule[t.getTaskParams().getId()].add(t);
    }

    /**
     * Removes a task from the queue of tasks to reschedule
     *
     * @param t tasks to be removed
     */
    public void rescheduledTask(Task t) {
        toRescheduleCount--;
        tasksToReschedule[t.getTaskParams().getId()].remove(t);
    }

    /**
     * Checks if there is any tasks to reschedule
     *
     * @return true if there is some tasks on the queue of tasks to reschedule
     */
    public boolean areTasksToReschedule() {
        return toRescheduleCount != 0;
    }

    /**
     * Gets the amount of task to be rescheduled that execute a specific core
     *
     * @param coreId identifier of the core
     * @return amount of task to be rescheduled that execute a specific core
     */
    public int getToRescheduleCount(int coreId) {
        return tasksToReschedule[coreId].size();
    }

    /**
     * Returns the whole list of tasks to reschedule
     *
     * @return The whole list of tasks to reschedule
     */
    public LinkedList<Task>[] getTasksToReschedule() {
        return tasksToReschedule;
    }

    /**
     * ** Priority Tasks Management
     */
    /**
     * Adds a task to the queue of priority tasks
     *
     * @param t Task to be added
     */
    public void newPriorityTask(Task t) {
        priorityTasks[t.getTaskParams().getId()].add(t);
        priorityCount++;

    }

    /**
     * Removes a task from the queue of priority tasks
     *
     * @param t tasks to be removed
     */
    public void priorityTaskScheduled(Task t) {
        priorityCount--;
        priorityTasks[t.getTaskParams().getId()].remove(t);
    }

    /**
     * Checks if there is any priority task
     *
     * @return true if there is some tasks on the queue of priority tasks
     */
    public boolean arePriorityTasks() {
        return priorityCount != 0;
    }

    /**
     * Gets the amount of priority task that execute a specific core
     *
     * @param coreId identifier of the core
     * @return amount of priority task that execute a specific core
     */
    public int getPriorityCount(int coreId) {
        return priorityTasks[coreId].size();
    }

    /**
     * Returns the whole list of priority tasks
     *
     * @return The whole list of priority tasks
     */
    public LinkedList<Task>[] getPriorityTasks() {
        return priorityTasks;
    }

    /**
     * ** Regular Tasks Management
     */
    /**
     * Adds a task to the queue of regular tasks
     *
     * @param t Task to be added
     */
    public void newRegularTask(Task t) {
        regularTasks[t.getTaskParams().getId()].add(t);
        regularCount++;
    }

    /**
     * Removes a task from the queue of regular tasks
     *
     * @param t tasks to be removed
     */
    public void regularTaskScheduled(Task t) {
        regularCount--;
        regularTasks[t.getTaskParams().getId()].remove(t);
    }

    /**
     * Checks if there is any regular task
     *
     * @return true if there is some tasks on the queue of regular tasks
     */
    public boolean areRegularTasks() {
        return regularCount != 0;
    }

    /**
     * Gets the amount of regular task that execute a specific core
     *
     * @param coreId identifier of the core
     * @return amount of regular task that execute a specific core
     */
    public int getRegularCount(int coreId) {
        return regularTasks[coreId].size();
    }

    /**
     * Returns the whole list of regular tasks
     *
     * @return The whole list of regular tasks
     */
    public LinkedList<Task>[] getRegularTasks() {
        return regularTasks;
    }

    /**
     * Creates a description of the current schedule for all the resources. The
     * string pattern is described as follows: On execution: hostName1: taskId
     * taskId ... (all running tasks for hostName1) hostName2: taskId ... (all
     * running tasks for hostName2) ...
     *
     * Pending: taskId taskId taskId ... (all pending tasks in slots, to
     * reschedule or without resource)
     *
     * @return description of the current schedule state
     */
    public String describeCurrentState() {
        String pending = "";
        for (int i = 0; i < CoreManager.getCoreCount(); i++) {
            for (Task t : tasksToReschedule[i]) {
                pending += " " + t.getId() + "r";
            }
            for (Task t : priorityTasks[i]) {
                pending += " " + t.getId() + "p";
            }
            for (Task t : noResourceTasks[i]) {
                pending += " " + t.getId() + "b";
            }
            for (Task t : regularTasks[i]) {
                pending += " " + t.getId();
            }
        }
        return pending;
    }

    /**
     * Obtains the data that must be shown on the monitor
     *
     * @return String with core Execution information in an XML format
     */
    public String getMonitoringInfo() {
        StringBuilder sb = new StringBuilder("");
        return sb.toString();
    }

    public void resizeDataStructures() {
         int coreCount = CoreManager.getCoreCount();
        //TASK SET STATE
        LinkedList<Task>[] tmp = new LinkedList[coreCount];
        System.arraycopy(noResourceTasks, 0, tmp, 0, noResourceTasks.length);
        for (int i = noResourceTasks.length; i < coreCount; i++) {
            tmp[i] = new LinkedList<Task>();
        }
        noResourceTasks = tmp;

        tmp = new LinkedList[coreCount];
        System.arraycopy(tasksToReschedule, 0, tmp, 0, tasksToReschedule.length);
        for (int i = tasksToReschedule.length; i < coreCount; i++) {
            tmp[i] = new LinkedList<Task>();
        }
        tasksToReschedule = tmp;

        tmp = new LinkedList[coreCount];
        System.arraycopy(priorityTasks, 0, tmp, 0, priorityTasks.length);
        for (int i = priorityTasks.length; i < coreCount; i++) {
            tmp[i] = new LinkedList<Task>();
        }
        priorityTasks = tmp;

        tmp = new LinkedList[coreCount];
        System.arraycopy(regularTasks, 0, tmp, 0, regularTasks.length);
        for (int i = regularTasks.length; i < coreCount; i++) {
            tmp[i] = new LinkedList<Task>();
        }
        regularTasks = tmp;
    }

    public void getWorkloadState(WorkloadStatus ss) {
        for (int coreId = 0; coreId < ss.getCoreCount(); coreId++) {
            ss.registerReadyTaskCounts(coreId, noResourceTasks[coreId].size(), priorityTasks[coreId].size() + regularTasks[coreId].size(), tasksToReschedule[coreId].size());
        }
    }
}
