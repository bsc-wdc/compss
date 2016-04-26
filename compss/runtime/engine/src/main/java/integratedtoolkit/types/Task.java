package integratedtoolkit.types;

import static integratedtoolkit.types.Colors.*;
import integratedtoolkit.types.allocatableactions.SingleExecution;
import integratedtoolkit.types.parameter.Parameter;
import java.util.LinkedList;
import java.util.concurrent.atomic.AtomicInteger;

public class Task implements Comparable<Task> {

    // Task ID management
    private static final int FIRST_TASK_ID = 1;
    private static AtomicInteger nextTaskId = new AtomicInteger(FIRST_TASK_ID);

    // Task states
    public enum TaskState {
        TO_ANALYSE,
        TO_EXECUTE,
        FINISHED,
        FAILED
    }

    private static final ColorConf[] COLORS = new ColorConf[]{
        new ColorConf(DARK_BLUE, WHITE),
        new ColorConf(YELLOW, BLACK),
        new ColorConf(LIGHT_GREEN, BLACK),
        new ColorConf(LIGHT_BLUE, BLACK),
        new ColorConf(PINK, WHITE),
        new ColorConf(GREY, BLACK),
        new ColorConf(VIOLET, WHITE),
        new ColorConf(PURPLE, WHITE),
        new ColorConf(DARK_RED, WHITE),
        new ColorConf(DARK_GREEN, WHITE),
        new ColorConf(BROWN, WHITE),
        new ColorConf(RED, WHITE)
    };

    private static class ColorConf {

        String fillColor;
        String fontColor;

        ColorConf(String fillColor, String fontColor) {
            this.fillColor = fillColor;
            this.fontColor = fontColor;
        }
    }

    // Task fields
    private final long appId;
    private final int taskId;
    private TaskState status;
    private final TaskParams taskParams;

    //Data Dependencies
    private final LinkedList<Task> predecessors;
    private final LinkedList<Task> successors;

    // Scheduling info
    private Task enforcingTask;
    private SingleExecution execution;

    public Task(Long appId, String methodClass, String methodName, boolean priority, boolean hasTarget, Parameter[] parameters) {
        this.appId = appId;
        this.taskId = nextTaskId.getAndIncrement();
        this.status = TaskState.TO_ANALYSE;
        this.taskParams = new TaskParams(methodClass, methodName, priority, hasTarget, parameters);
        this.predecessors = new LinkedList<Task>();
        this.successors = new LinkedList<Task>();
    }

    public Task(Long appId, String namespace, String service, String port, String operation, boolean priority, boolean hasTarget, Parameter[] parameters) {
        this.appId = appId;
        this.taskId = nextTaskId.getAndIncrement();
        this.status = TaskState.TO_ANALYSE;
        this.taskParams = new TaskParams(namespace, service, port, operation, priority, hasTarget, parameters);
        this.predecessors = new LinkedList<Task>();
        this.successors = new LinkedList<Task>();
    }

    public static int getCurrentTaskCount() {
        return nextTaskId.get();
    }

    public void addDataDependency(Task producer) {
        producer.successors.add(this);
        this.predecessors.add(producer);
    }

    public void releaseDataDependents() {
        for (Task t : this.successors) {
            t.predecessors.remove(this);
        }
        this.successors.clear();
    }

    public LinkedList<Task> getSuccessors() {
        return successors;
    }

    public LinkedList<Task> getPredecessors() {
        return predecessors;
    }

    public long getAppId() {
        return appId;
    }

    public int getId() {
        return taskId;
    }

    public TaskState getStatus() {
        return status;
    }

    public void setStatus(TaskState status) {
        this.status = status;
    }

    public void setEnforcingTask(Task task) {
        this.enforcingTask = task;
    }

    public TaskParams getTaskParams() {
        return taskParams;
    }

    public boolean isSchedulingForced() {
        return this.enforcingTask != null;
    }

    public Task getEnforcingTask() {
        return this.enforcingTask;
    }

    public String getDotDescription() {
        String shape; // black
        ColorConf color = COLORS[taskParams.getId() % COLORS.length];

        if (taskParams.getType() == TaskParams.Type.METHOD) {
            shape = "circle";
        } else { //Service
            shape = "diamond";
        }
        // Future Shapes "triangle" "square" "pentagon"
        return getId()
                + "[shape=" + shape + ", "
                + "style=filled fillcolor=\"" + color.fillColor + "\" fontcolor=\"" + color.fontColor + "\"];";
    }

    public String toString() {
        StringBuilder buffer = new StringBuilder();

        buffer.append("[[Task id: ").append(getId()).append("]");
        buffer.append(", [Status: ").append(getStatus()).append("]");
        buffer.append(", ").append(getTaskParams().toString()).append("]");

        return buffer.toString();
    }

    // Comparable interface implementation
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

    public void setExecution(SingleExecution execution) {
        this.execution = execution;
    }

    public SingleExecution getExecution() {
        return execution;
    }
}
