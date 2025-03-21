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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.parameter.impl.DependencyParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.util.Tracer;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Representation of a Task.
 */
public abstract class AbstractTask implements Comparable<AbstractTask> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Task fields
    private final Application app;
    private final int taskId;
    private TaskState status;

    // Data Dependencies
    private final List<AbstractTask> predecessors;
    private final List<AbstractTask> successors;

    // Stream Dependencies
    private final List<AbstractTask> streamDataProducers; // Previous tasks that produce a stream
    private final List<AbstractTask> streamDataConsumers; // Next tasks that consumer a stream

    // Syncrhonization point to which the task belongs
    private int synchronizationId;

    // Add execution to task
    private final List<AllocatableAction> executions;

    // Map of predecessors and their dependent parameters
    private final Map<AbstractTask, DependencyParameter> dependentTasks;

    // List of parameters free of dependencies
    private final List<Parameter> freeParams;

    // Listeners to notify when the task ends
    private final List<TaskListener> listeners;


    /**
     * Creates a new Abstract Method Task with the given parameters.
     *
     * @param app Application to which the task belongs.
     * @param taskId Id of the task
     */
    public AbstractTask(Application app, int taskId) {
        this.app = app;
        this.taskId = taskId;
        this.status = TaskState.TO_ANALYSE;
        this.predecessors = new LinkedList<>();
        this.successors = new LinkedList<>();
        this.executions = new LinkedList<>();
        this.streamDataProducers = new LinkedList<>();
        this.streamDataConsumers = new LinkedList<>();
        this.dependentTasks = new HashMap<>();
        this.freeParams = new LinkedList<>();
        this.listeners = new LinkedList<>();
    }

    /**
     * Adds a data dependency from the {@code producer} to this task.
     *
     * @param producer Producer task.
     */
    public void addDataDependency(AbstractTask producer, DependencyParameter dp) {
        producer.successors.add(this);
        this.predecessors.add(producer);
        this.dependentTasks.put(producer, dp);

        // Adding predecessors for task dependency tracing
        if (Tracer.isActivated() && Tracer.isTracingTaskDependencies()) {
            Tracer.addPredecessors(this.taskId, producer.taskId);
        }
    }

    /**
     * Adds a stream dependency from the given producer task to this task.
     *
     * @param producer Stream producer task.
     */
    public void addStreamDataDependency(AbstractTask producer) {
        producer.streamDataConsumers.add(this);
        this.streamDataProducers.add(producer);
    }

    /**
     * Release all the tasks that are data dependent to this task.
     */
    public void releaseDataDependents() {
        for (AbstractTask t : this.successors) {
            // We do not have to remove predecessor for reduces
            // to avoid uncontrolled executions of the reduce action
            if (!t.isReduction()) {
                synchronized (t) {
                    t.removePredecessor(this);
                }
            }
        }
        this.successors.clear();
    }

    public abstract boolean isReduction();

    /**
     * Remove the task from the predecessor's list of successors.
     */
    public void removePredecessor(AbstractTask t) {
        this.predecessors.remove(t);
        this.dependentTasks.remove(t);
    }

    /**
     * Returns all the successor tasks.
     *
     * @return All the successor tasks.
     */
    public List<AbstractTask> getSuccessors() {
        return this.successors;
    }

    /**
     * Returns all the predecessor tasks.
     *
     * @return All the predecessor tasks.
     */
    public List<AbstractTask> getPredecessors() {
        return this.predecessors;
    }

    /**
     * Returns the dependency parameter of a concrete task.
     *
     * @return The task dependency parameter.
     */
    public DependencyParameter getDependencyParameters(AbstractTask t) {
        return this.dependentTasks.get(t);
    }

    /**
     * Returns if the parameter has dependencies.
     *
     * @return The list of the parameters free of dependencies.
     */
    public List<Parameter> getFreeParams() {
        return freeParams;
    }

    /**
     * Registers a parameter as free of dependencies.
     *
     * @param p Parameter to register.
     */
    public void registerFreeParam(Parameter p) {
        this.freeParams.add(p);
    }

    /**
     * Returns all the tasks producing stream elements used by the current task.
     *
     * @return All the tasks producing stream elements used by the current task.
     */
    public List<AbstractTask> getStreamProducers() {
        return this.streamDataProducers;
    }

    /**
     * Returns all the tasks consuming stream elements from the current task.
     *
     * @return All the tasks consuming stream elements from the current task.
     */
    public List<AbstractTask> getStreamConsumers() {
        return this.streamDataConsumers;
    }

    /**
     * Sets the synchronization id of the task to {@code syncId}.
     *
     * @param syncId Synchronization Id.
     */
    public void setSynchronizationId(int syncId) {
        this.synchronizationId = syncId;
    }

    /**
     * Returns the synchronization Id of the task.
     *
     * @return The synchronization Id of the task.
     */
    public int getSynchronizationId() {
        return this.synchronizationId;
    }

    /**
     * Returns the application to which the task belongs.
     *
     * @return The application to which the task belongs.
     */
    public Application getApplication() {
        return this.app;
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
     * Adds a listener to notify when the Abstract task ends.
     *
     * @param listener listener to notify on task end
     */
    public void addListener(TaskListener listener) {
        this.listeners.add(listener);
    }

    /**
     * Notifies all listeners that the abstract task has ended.
     */
    public void notifyListeners() {
        for (TaskListener listener : this.listeners) {
            listener.taskFinished();
        }
    }

    /**
     * Adds a new execution to the task.
     *
     * @param execution The new execution to add.
     */
    public void addExecution(AllocatableAction execution) {
        this.executions.add(execution);
    }

    /**
     * Returns the executions of the task.
     *
     * @return The executions of the task.
     */
    public List<AllocatableAction> getExecutions() {
        return this.executions;
    }

    /**
     * Returns the DOT description of the task (only for monitoring).
     *
     * @return A string containing the DOT description of the task.
     */
    public abstract String getDotDescription();

    /**
     * Returns the task legend description (only for monitoring).
     *
     * @return The task legend description (only for monitoring).
     */
    public abstract String getLegendDescription();

    /**
     * Returns the task color (only for monitoring).
     *
     * @return The task color (only for monitoring).
     */
    public abstract String getColor();

    @Override
    public int compareTo(AbstractTask task) {
        if (task == null) {
            throw new NullPointerException();
        }

        return this.getId() - task.getId();
    }

    @Override
    public boolean equals(Object o) {
        return (o instanceof AbstractTask) && (this.taskId == ((AbstractTask) o).taskId);
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

        return buffer.toString();
    }

    /**
     * Registers the end of execution of the task.
     *
     * @param checkpointing {@literal true} if task has been recovered by the checkpoint management
     */
    public void end(boolean checkpointing) {
        // Release data dependent tasks
        if (DEBUG) {
            LOGGER.debug("Releasing data dependant tasks for task " + taskId);
        }
        this.releaseDataDependents();
    }
}
