package integratedtoolkit.types;

import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.allocatableactions.ExecutionAction;
import integratedtoolkit.types.colors.ColorConfiguration;
import integratedtoolkit.types.colors.ColorNode;
import integratedtoolkit.types.implementations.Implementation.TaskType;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * Representation of a Task
 *
 */
public class Task implements Comparable<Task> {

    // Task ID management
    private static final int FIRST_TASK_ID = 1;
    private static AtomicInteger nextTaskId = new AtomicInteger(FIRST_TASK_ID);

    /**
     * Task states
     *
     */
    public enum TaskState {
        TO_ANALYSE, // Task is beeing analysed
        TO_EXECUTE, // Task can be executed
        FINISHED, // Task has finished successfully
        FAILED // Task has failed
    }

    // Task fields
    private final long appId;
    private final int taskId;
    private TaskState status;
    private final TaskDescription taskDescription;

    // Data Dependencies
    private final List<Task> predecessors;
    private final List<Task> successors;

    // Syncrhonization point to which the task belongs
    private int synchronizationId;

    // Scheduling info
    private Task enforcingTask;
    private final List<ExecutionAction> executions;

    // Execution count information
    private int executionCount;

    /**
     * Creates a new task with the given parameters
     *
     * @param appId
     * @param methodClass
     * @param methodName
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param parameters
     */
    public Task(Long appId, String signature, boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed,
            boolean hasTarget, boolean hasReturn, Parameter[] parameters) {

        this.appId = appId;
        this.taskId = nextTaskId.getAndIncrement();
        this.status = TaskState.TO_ANALYSE;
        this.taskDescription = new TaskDescription(signature, isPrioritary, numNodes, isReplicated, isDistributed, hasTarget, hasReturn,
                parameters);
        this.predecessors = new LinkedList<>();
        this.successors = new LinkedList<>();
        this.executions = new LinkedList<>();
    }

    /**
     * Creates a new task with the given parameters
     *
     * @param appId
     * @param namespace
     * @param service
     * @param port
     * @param operation
     * @param isPrioritary
     * @param hasTarget
     * @param parameters
     */
    public Task(Long appId, String namespace, String service, String port, String operation, boolean isPrioritary, boolean hasTarget,
            Parameter[] parameters) {

        this.appId = appId;
        this.taskId = nextTaskId.getAndIncrement();
        this.status = TaskState.TO_ANALYSE;
        this.taskDescription = new TaskDescription(namespace, service, port, operation, isPrioritary, hasTarget, parameters);
        this.predecessors = new LinkedList<>();
        this.successors = new LinkedList<>();
        this.executions = new LinkedList<>();
    }

    /**
     * Returns the current number of generated tasks
     *
     * @return
     */
    public static int getCurrentTaskCount() {
        return nextTaskId.get();
    }

    /**
     * Adds a data dependency from the @producer to this task
     *
     * @param producer
     */
    public void addDataDependency(Task producer) {
        producer.successors.add(this);
        this.predecessors.add(producer);
    }

    /**
     * Release all the tasks that are data dependent to this task
     *
     */
    public void releaseDataDependents() {
        for (Task t : this.successors) {
            t.predecessors.remove(this);
        }
        this.successors.clear();
    }

    /**
     * Returns all the successor tasks
     *
     * @return
     */
    public List<Task> getSuccessors() {
        return successors;
    }

    /**
     * Returns all the predecessor tasks
     *
     * @return
     */
    public List<Task> getPredecessors() {
        return predecessors;
    }

    /**
     * Sets the synchronization id of the task to @syncId
     *
     */
    public void setSynchronizationId(int syncId) {
        this.synchronizationId = syncId;
    }

    /**
     * Returns the syncrhonization Id of the task
     *
     * @return
     */
    public int getSynchronizationId() {
        return this.synchronizationId;
    }

    /**
     * Returns the app id
     *
     * @return
     */
    public long getAppId() {
        return appId;
    }

    /**
     * Returns the task id
     *
     * @return
     */
    public int getId() {
        return taskId;
    }

    /**
     * Returns the task status
     *
     * @return
     */
    public TaskState getStatus() {
        return status;
    }

    /**
     * Sets a new task status
     *
     * @param status
     */
    public void setStatus(TaskState status) {
        this.status = status;
    }

    /**
     * Sets the task as enforcing
     *
     * @param task
     */
    public void setEnforcingTask(Task task) {
        this.enforcingTask = task;
    }

    /**
     * Returns whether the task is free or not
     *
     * @return
     */
    public boolean isFree() {
        return (this.executionCount == 0);
    }

    /**
     * Sets a new execution count for the task
     *
     * @param executionCount
     */
    public void setExecutionCount(int executionCount) {
        this.executionCount = executionCount;
    }

    /**
     * Decreases the execution count of the task
     *
     */
    public void decreaseExecutionCount() {
        --this.executionCount;
    }

    /**
     * Returns the task description
     *
     * @return
     */
    public TaskDescription getTaskDescription() {
        return taskDescription;
    }

    /**
     * Returns whether the task is scheduling forced or not
     *
     * @return
     */
    public boolean isSchedulingForced() {
        return this.enforcingTask != null;
    }

    /**
     * Returns the associated enforcing task
     *
     * @return
     */
    public Task getEnforcingTask() {
        return this.enforcingTask;
    }

    /**
     * Returns the DOT description of the task (only for monitoring)
     *
     * @return
     */
    public String getDotDescription() {
        int monitorTaskId = taskDescription.getId() + 1; // Coherent with Trace.java
        ColorNode color = ColorConfiguration.COLORS[monitorTaskId % ColorConfiguration.NUM_COLORS];

        String shape;
        if (taskDescription.getType() == TaskType.METHOD) {
            if (this.taskDescription.isReplicated()) {
                shape = "doublecircle";
            } else if (this.taskDescription.isDistributed()) {
                // Its only a scheduler hint, no need to show them differently
                shape = "circle";
            } else {
                shape = "circle";
            }
        } else { // Service
            shape = "diamond";
        }
        // TODO: Future Shapes "triangle" "square" "pentagon"

        return getId() + "[shape=" + shape + ", " + "style=filled fillcolor=\"" + color.getFillColor() + "\" fontcolor=\""
                + color.getFontColor() + "\"];";
    }

    /**
     * Returns the task legend description (only for monitoring)
     *
     * @return
     */
    public String getLegendDescription() {
        StringBuilder information = new StringBuilder();
        information.append("<tr>").append("\n");
        information.append("<td align=\"right\">").append(this.getMethodName()).append("</td>").append("\n");
        information.append("<td bgcolor=\"").append(this.getColor()).append("\">&nbsp;</td>").append("\n");
        information.append("</tr>").append("\n");

        return information.toString();
    }

    /**
     * Returns the method name associated to this task
     *
     * @return
     */
    public String getMethodName() {
        String methodName = taskDescription.getName();
        return methodName;
    }

    /**
     * Returns the task color (only for monitoring)
     *
     * @return
     */
    public String getColor() {
        int monitorTaskId = taskDescription.getId() + 1; // Coherent with Trace.java
        ColorNode color = ColorConfiguration.COLORS[monitorTaskId % ColorConfiguration.NUM_COLORS];
        return color.getFillColor();
    }

    /**
     * Adds a new execution to the task
     *
     * @param execution
     */
    public void addExecution(ExecutionAction execution) {
        this.executions.add(execution);
    }

    /**
     * Returns the executions of the task
     *
     * @return
     */
    public List<ExecutionAction> getExecutions() {
        return executions;
    }

    @Override
    public int compareTo(Task task) {
        if (task == null) {
            throw new NullPointerException();
        }

        return this.getId() - task.getId();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof Task) && (this.taskId == ((Task) o).taskId);
    }

    @Override
    public int hashCode() {
        return super.hashCode();
    }

    @Override
    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[[Task id: ").append(getId()).append("]");
        buffer.append(", [Status: ").append(getStatus()).append("]");
        buffer.append(", ").append(getTaskDescription().toString()).append("]");

        return buffer.toString();
    }

}
