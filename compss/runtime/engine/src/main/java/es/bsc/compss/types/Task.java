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
package es.bsc.compss.types;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.colors.ColorConfiguration;
import es.bsc.compss.types.colors.ColorNode;
import es.bsc.compss.types.implementations.Implementation.TaskType;
import es.bsc.compss.types.parameter.Parameter;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Representation of a Task.
 */
public class Task implements Comparable<Task> {

    // Task ID management
    private static final int FIRST_TASK_ID = 1;
    private static AtomicInteger nextTaskId = new AtomicInteger(FIRST_TASK_ID);


    /**
     * Task states.
     */
    public enum TaskState {
        TO_ANALYSE, // Task is being analysed
        TO_EXECUTE, // Task can be executed
        FINISHED, // Task has finished successfully
        CANCELED, // Task has been canceled
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

    // Task Monitor
    private final TaskMonitor taskMonitor;

    // On failure behavior
    private final OnFailure onFailure;


    /**
     * Creates a new METHOD task with the given parameters.
     *
     * @param appId Application Id.
     * @param lang Application language.
     * @param signature Task signature.
     * @param isPrioritary Whether the task has priority or not.
     * @param numNodes Number of nodes used by the task.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed round-robin or not.
     * @param numReturns Number of returns of the task.
     * @param hasTarget Whether the task has a target object or not.
     * @param parameters Task parameter values.
     * @param monitor Task monitor.
     * @param onFailure On failure mechanisms.
     */
    public Task(Long appId, Lang lang, String signature, boolean isPrioritary, int numNodes, boolean isReplicated,
            boolean isDistributed, boolean hasTarget, int numReturns, List<Parameter> parameters, TaskMonitor monitor,
            OnFailure onFailure) {

        this.appId = appId;
        this.taskId = nextTaskId.getAndIncrement();
        this.status = TaskState.TO_ANALYSE;
        this.taskDescription = new TaskDescription(lang, signature, isPrioritary, numNodes, isReplicated, isDistributed,
                hasTarget, numReturns, parameters);
        this.predecessors = new LinkedList<>();
        this.successors = new LinkedList<>();
        this.executions = new LinkedList<>();
        this.taskMonitor = monitor;
        this.onFailure = onFailure;
    }

    /**
     * Creates a new SERVICE task with the given parameters.
     *
     * @param appId Application Id.
     * @param namespace Service namespace.
     * @param service Service name.
     * @param port Service port.
     * @param operation Service operation.
     * @param isPrioritary Whether the task has priority or not.
     * @param hasTarget Whether the task has a target object or not.
     * @param numReturns Number of returns of the task.
     * @param parameters Task parameter values.
     * @param monitor Task monitor.
     * @param onFailure On failure mechanisms.
     */
    public Task(Long appId, String namespace, String service, String port, String operation, boolean isPrioritary,
            boolean hasTarget, int numReturns, List<Parameter> parameters, TaskMonitor monitor, OnFailure onFailure) {

        this.appId = appId;
        this.taskId = nextTaskId.getAndIncrement();
        this.status = TaskState.TO_ANALYSE;
        this.taskDescription = new TaskDescription(namespace, service, port, operation, isPrioritary, hasTarget,
                numReturns, parameters);
        this.predecessors = new LinkedList<>();
        this.successors = new LinkedList<>();
        this.executions = new LinkedList<>();
        this.taskMonitor = monitor;
        this.onFailure = onFailure;
    }

    /**
     * Returns the current number of generated tasks.
     *
     * @return The number of generated tasks.
     */
    public static int getCurrentTaskCount() {
        return nextTaskId.get();
    }

    /**
     * Adds a data dependency from the given task {@code producer} to this task.
     *
     * @param producer Producer task.
     */
    public void addDataDependency(Task producer) {
        producer.successors.add(this);
        this.predecessors.add(producer);
    }

    /**
     * Releases all the tasks that are data dependent to this task.
     */
    public void releaseDataDependents() {
        for (Task t : this.successors) {
            synchronized (t) {
                t.predecessors.remove(this);
            }
        }
        this.successors.clear();
    }

    /**
     * Returns all the successor tasks.
     *
     * @return A list containing the successor tasks of this task.
     */
    public List<Task> getSuccessors() {
        return this.successors;
    }

    /**
     * Returns all the predecessor tasks.
     *
     * @return A list containing the predecessor tasks of this task.
     */
    public List<Task> getPredecessors() {
        return this.predecessors;
    }

    /**
     * Sets the synchronization id of the task to the given synchronization Id {@code syncId}.
     *
     * @param syncId Task synchronization Id.
     */
    public void setSynchronizationId(int syncId) {
        this.synchronizationId = syncId;
    }

    /**
     * Returns the synchronization Id of the task.
     *
     * @return Task synchronization Id.
     */
    public int getSynchronizationId() {
        return this.synchronizationId;
    }

    /**
     * Returns the application Id.
     *
     * @return The application Id.
     */
    public long getAppId() {
        return this.appId;
    }

    /**
     * Returns the task Id.
     *
     * @return The task Id.
     */
    public int getId() {
        return this.taskId;
    }

    /**
     * Returns the task status.
     *
     * @return The task status.
     */
    public TaskState getStatus() {
        return this.status;
    }

    /**
     * Sets a new task status.
     *
     * @param status New task status.
     */
    public void setStatus(TaskState status) {
        this.status = status;
    }

    /**
     * Sets an associated enforcing task.
     *
     * @param task Associated enforcing task.
     */
    public void setEnforcingTask(Task task) {
        this.enforcingTask = task;
    }

    /**
     * Returns whether the task is free or not.
     *
     * @return {@code true} if the task is free, {@code false} otherwise.
     */
    public boolean isFree() {
        return (this.executionCount == 0);
    }

    /**
     * Sets a new execution count for the task.
     *
     * @param executionCount New execution count.
     */
    public void setExecutionCount(int executionCount) {
        this.executionCount = executionCount;
    }

    /**
     * Decreases the execution count of the task.
     */
    public void decreaseExecutionCount() {
        --this.executionCount;
    }

    /**
     * Returns the task description.
     *
     * @return The task description.
     */
    public TaskDescription getTaskDescription() {
        return this.taskDescription;
    }

    /**
     * Returns whether the task scheduling is forced or not.
     *
     * @return {@code true} if the scheduling of this task is forced, {@code false} otherwise.
     */
    public boolean isSchedulingForced() {
        return this.enforcingTask != null;
    }

    /**
     * Returns the associated enforcing task.
     *
     * @return The associated enforcing task.
     */
    public Task getEnforcingTask() {
        return this.enforcingTask;
    }

    /**
     * Returns the DOT description of the task (only for monitoring).
     *
     * @return A string representing the description of the task in DOT format.
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

        return getId() + "[shape=" + shape + ", " + "style=filled fillcolor=\"" + color.getFillColor()
                + "\" fontcolor=\"" + color.getFontColor() + "\"];";
    }

    /**
     * Returns the task legend description (only for monitoring).
     *
     * @return A String containing the task legend description.
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
     * Returns the method name associated to this task.
     *
     * @return The associated method name.
     */
    public String getMethodName() {
        String methodName = this.taskDescription.getName();
        return methodName;
    }

    /**
     * Returns the task color (only for monitoring).
     *
     * @return The task color.
     */
    public String getColor() {
        int monitorTaskId = this.taskDescription.getId() + 1; // Coherent with Trace.java
        ColorNode color = ColorConfiguration.COLORS[monitorTaskId % ColorConfiguration.NUM_COLORS];
        return color.getFillColor();
    }

    /**
     * Adds a new execution to the task.
     *
     * @param execution New task execution.
     */
    public void addExecution(ExecutionAction execution) {
        this.executions.add(execution);
    }

    /**
     * Returns the executions of the task.
     *
     * @return List of executions of the task.
     */
    public List<ExecutionAction> getExecutions() {
        return executions;
    }

    /**
     * Returns the monitor associated to the Task.
     *
     * @return The associated monitor to the task.
     */
    public TaskMonitor getTaskMonitor() {
        return this.taskMonitor;
    }

    /**
     * Returns the on-failure mechanisms.
     * 
     * @return The on-failure mechanisms.
     */
    public OnFailure getOnFailure() {
        return this.onFailure;
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
