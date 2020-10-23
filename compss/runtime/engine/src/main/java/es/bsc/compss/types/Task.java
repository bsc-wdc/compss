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
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.colors.ColorConfiguration;
import es.bsc.compss.types.colors.ColorNode;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.parameter.DependencyParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.util.CoreManager;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;


/**
 * Representation of a Task.
 */
public class Task extends AbstractTask {

    private final TaskDescription taskDescription;

    // Scheduling info
    private Task enforcingTask;

    // Execution count information
    private int executionCount;

    // Task Monitor
    private final TaskMonitor taskMonitor;

    // Commutative groups of the task
    private TreeMap<Integer, CommutativeGroupTask> commutativeGroup;

    // List of task groups
    private LinkedList<TaskGroup> taskGroups;


    /**
     * Creates a new METHOD task with the given parameters.
     *
     * @param app Application to which the task belongs.
     * @param lang Application language.
     * @param signature Task signature.
     * @param isPrioritary Whether the task has priority or not.
     * @param numNodes Number of nodes used by the task.
     * @param isReduction Whether the task must be replicated or not.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed round-robin or not.
     * @param numReturns Number of returns of the task.
     * @param hasTarget Whether the task has a target object or not.
     * @param parameters Task parameter values.
     * @param monitor Task monitor.
     * @param onFailure On failure mechanisms.
     * @param timeOut Time for a task time out.
     */
    public Task(Application app, Lang lang, String signature, boolean isPrioritary, int numNodes, boolean isReduction,
        boolean isReplicated, boolean isDistributed, boolean hasTarget, int numReturns, List<Parameter> parameters,
        TaskMonitor monitor, OnFailure onFailure, long timeOut) {

        super(app);
        CoreElement core = CoreManager.getCore(signature);
        String parallelismSource = app.getParallelismSource();
        this.taskDescription =
            new TaskDescription(TaskType.METHOD, lang, signature, core, parallelismSource, isPrioritary, numNodes,
                isReduction, isReplicated, isDistributed, hasTarget, numReturns, onFailure, timeOut, parameters);
        this.taskMonitor = monitor;
        this.commutativeGroup = new TreeMap<>();
        this.taskGroups = new LinkedList<>();
    }

    /**
     * Creates a new SERVICE task with the given parameters.
     *
     * @param app Application to which the tasks belongs.
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
     * @param timeOut Time for a task time out.
     */
    public Task(Application app, String namespace, String service, String port, String operation, boolean isPrioritary,
        boolean hasTarget, int numReturns, List<Parameter> parameters, TaskMonitor monitor, OnFailure onFailure,
        long timeOut) {

        super(app);
        String signature =
            ServiceImplementation.getSignature(namespace, service, port, operation, hasTarget, numReturns, parameters);
        CoreElement core = CoreManager.getCore(signature);

        int numNodes = Constants.SINGLE_NODE;
        boolean isReplicated = Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK);
        boolean isDistributed = Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK);
        boolean isReduction = false;
        String parallelismSource = app.getParallelismSource();
        this.taskDescription = new TaskDescription(TaskType.SERVICE, Lang.UNKNOWN, signature, core, parallelismSource,
            isPrioritary, numNodes, isReduction, isReplicated, isDistributed, hasTarget, numReturns, onFailure, timeOut,
            parameters);
        this.taskMonitor = monitor;
        this.commutativeGroup = new TreeMap<>();
        this.taskGroups = new LinkedList<>();
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
     * Registers a new commutative group for the dataId {@code daId}.
     *
     * @param daId DataId of the group.
     * @param com Commutative group task to be set.
     */
    public void setCommutativeGroup(CommutativeGroupTask com, DataAccessId daId) {
        this.commutativeGroup.put(daId.getDataId(), com);
    }

    /**
     * Returns the specific commutative group for the data {@code daId}.
     *
     * @param daId DataId of the group.
     * @return The commutative group for the particular dataId.
     */
    public CommutativeGroupTask getCommutativeGroup(Integer daId) {
        return this.commutativeGroup.get(daId);
    }

    /**
     * Returns the list of commutative groups associated to the task.
     *
     * @return A list of all the commutative groups the task is part of.
     */
    public List<CommutativeGroupTask> getCommutativeGroupList() {
        LinkedList<CommutativeGroupTask> commutativeGroupList = new LinkedList<>();
        for (Map.Entry<Integer, CommutativeGroupTask> entry : commutativeGroup.entrySet()) {
            CommutativeGroupTask comTask = entry.getValue();
            commutativeGroupList.add(comTask);
        }
        return commutativeGroupList;
    }

    /**
     * Adds a new TaskGroup to the task.
     * 
     * @param taskGroup Group of tasks.
     */

    public void addTaskGroup(TaskGroup taskGroup) {
        this.taskGroups.add(taskGroup);
    }

    /**
     * Returns a list of the task groups for the task.
     *
     * @return The list of tas groups.
     */
    public LinkedList<TaskGroup> getTaskGroupList() {
        return this.taskGroups;
    }

    /**
     * Returns the DOT description of the task (only for monitoring).
     *
     * @return A string representing the description of the task in DOT format.
     */
    public String getDotDescription() {
        int monitorTaskId = taskDescription.getCoreElement().getCoreId() + 1; // Coherent with Trace.java
        ColorNode color = ColorConfiguration.getColors()[monitorTaskId % ColorConfiguration.NUM_COLORS];

        String shape;
        if (taskDescription.getType() == TaskType.METHOD) {
            if (this.taskDescription.isReplicated()) {
                shape = "doublecircle";
            } else {
                if (this.taskDescription.isDistributed()) {
                    // Its only a scheduler hint, no need to show them differently
                    shape = "circle";
                } else {
                    shape = "circle";
                }
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
        int monitorTaskId = this.taskDescription.getCoreElement().getCoreId() + 1; // Coherent with Trace.java
        ColorNode color = ColorConfiguration.getColors()[monitorTaskId % ColorConfiguration.NUM_COLORS];
        return color.getFillColor();
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
     * Returns if the task can be executed depending on if the groups are executing other tasks.
     *
     * @return {@literal true} if the task can be executed, {@literal false} otherwise.
     */
    public boolean canBeExecuted() {
        if (!this.commutativeGroup.isEmpty()) {
            for (CommutativeGroupTask com : commutativeGroup.values()) {
                if (com.processingExecution(this.getId())) {
                    return false;
                }
            }
        }
        return true;
    }

    /**
     * Sets new version for the data {@code daId}.
     *
     * @param daId New data version.
     */
    public void setVersion(DataAccessId daId) {
        for (Parameter p : this.getTaskDescription().getParameters()) {
            if (p instanceof DependencyParameter
                && ((DependencyParameter) p).getDataAccessId().getDataId() == daId.getDataId()) {
                ((DependencyParameter) p).setDataAccessId(daId);
            }
        }
    }

    /**
     * Returns if the task is member of any task group.
     * 
     * @return A boolean stating if the task is member of any task group.
     */
    public boolean hasTaskGroups() {
        return !this.taskGroups.isEmpty();
    }

    /**
     * Returns if any of the parameters of the task has a commutative direction.
     *
     * @return {@literal true} if the task has commutative parameters, {@literal false} otherwise.
     */
    public boolean hasCommutativeParams() {
        for (Parameter p : this.getTaskDescription().getParameters()) {
            if (p instanceof DependencyParameter) {

                if (p.getDirection() == Direction.COMMUTATIVE) {
                    return true;
                }
            }
        }
        return false;
    }

    /**
     * Returns the on-failure mechanisms.
     *
     * @return The on-failure mechanisms.
     */
    public OnFailure getOnFailure() {
        return this.taskDescription.getOnFailure();
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

    public List<Parameter> getParameterDataToRemove() {
        return new LinkedList<>();
    }

    public List<Parameter> getIntermediateParameters() {
        return new LinkedList<>();
    }

    public List<Parameter> getUnusedIntermediateParameters() {
        return new LinkedList<>();
    }

    public List<Parameter> getParameters() {
        return this.taskDescription.getParameters();
    }

    @Override
    public boolean isReduction() {
        return this.taskDescription.isReduction();
    }

}
