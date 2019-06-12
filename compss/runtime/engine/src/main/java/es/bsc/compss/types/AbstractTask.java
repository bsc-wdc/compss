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

import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.AllocatableAction;


/**
 * Representation of a Task
 */
public abstract class AbstractTask implements Comparable<AbstractTask> {

 // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TA_COMP);
    
    // Task ID management
    private static final int FIRST_TASK_ID = 1;
    private static AtomicInteger nextTaskId = new AtomicInteger(FIRST_TASK_ID);

    // Task fields
    private final long appId;
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
     * Creates a new METHOD task with the given parameters
     *
     * @param appId
     * @param lang
     * @param signature
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param numReturns
     * @param hasTarget
     * @param parameters
     * @param monitor
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
     * Release all the tasks that are data dependent to this task
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
     * Returns all the successor tasks
     *
     * @return
     */
    public List<AbstractTask> getSuccessors() {
        return successors;
    }

    /**
     * Returns all the predecessor tasks
     *
     * @return
     */
    public List<AbstractTask> getPredecessors() {
        return predecessors;
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
     * Sets the synchronization id of the task to @syncId
     *
     * @param syncId
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
     * Returns the DOT description of the task (only for monitoring)
     *
     * @return
     */
    public abstract String getDotDescription();

    /**
     * Returns the task legend description (only for monitoring)
     *
     * @return
     */
    public abstract String getLegendDescription(); 

    

    /**
     * Returns the task color (only for monitoring)
     *
     * @return
     */
    public abstract String getColor(); 

    /**
     * Adds a new execution to the task
     *
     * @param execution
     */
    public void addExecution(AllocatableAction execution) {
        this.executions.add(execution);
    }

    /**
     * Returns the executions of the task
     *
     * @return
     */
    public List<AllocatableAction> getExecutions() {
        return executions;
    }
    
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
