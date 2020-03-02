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

import es.bsc.compss.scheduler.types.AllocatableAction;

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


/**
 * Representation of a Task.
 */
public abstract class AbstractTask implements Comparable<AbstractTask> {

    // Task ID management
    private static final int FIRST_TASK_ID = 1;
    private static AtomicInteger nextTaskId = new AtomicInteger(FIRST_TASK_ID);

    // Task fields
    private final Long appId;
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


    /**
     * Creates a new Abstract Method Task with the given parameters.
     *
     * @param appId Application id.
     */
    public AbstractTask(Long appId) {
        this.appId = appId;
        this.taskId = nextTaskId.getAndIncrement();
        this.status = TaskState.TO_ANALYSE;
        this.predecessors = new LinkedList<>();
        this.successors = new LinkedList<>();
        this.executions = new LinkedList<>();
        this.streamDataProducers = new LinkedList<>();
        this.streamDataConsumers = new LinkedList<>();
    }

    /**
     * Returns the current number of generated tasks.
     *
     * @return The current number of generated tasks.
     */
    public static int getCurrentTaskCount() {
        return nextTaskId.get();
    }

    /**
     * Adds a data dependency from the {@code producer} to this task.
     *
     * @param producer Producer task.
     */
    public void addDataDependency(AbstractTask producer) {
        producer.successors.add(this);
        this.predecessors.add(producer);
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
            synchronized (t) {
                t.predecessors.remove(this);
            }
        }
        this.successors.clear();
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

}
