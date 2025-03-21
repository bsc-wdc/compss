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
package es.bsc.compss.agent.rest.types;

public class TaskProfile {

    private final long taskReception;
    private Long taskCreated;
    private Long taskAnalyzed;
    private Long taskScheduled;
    private Long taskSubmitted;
    private Long executionStart;
    private Long executionEnd;
    private Long endNotification;
    private Long taskEnd;


    /**
     * Initializes a new TaskProfile instance.
     */
    public TaskProfile() {
        this.taskReception = System.currentTimeMillis();
        this.taskCreated = null;
        this.taskAnalyzed = null;
        this.taskScheduled = null;
        this.executionStart = null;
        this.executionEnd = null;
        this.taskEnd = null;
    }

    /**
     * Returns the creation time.
     *
     * @return The creation time.
     */
    public Long getCreationTime() {
        Long length = null;
        if (this.taskCreated != null) {
            length = this.taskCreated - this.taskReception;
        }
        return length;
    }

    /**
     * Returns the analysis time.
     *
     * @return The analysis time.
     */
    public Long getAnalysisTime() {
        Long length = null;
        if (this.taskAnalyzed != null && this.taskCreated != null) {
            length = this.taskAnalyzed - this.taskCreated;
        }
        return length;
    }

    /**
     * Returns the scheduling time.
     *
     * @return The scheduling time.
     */
    public Long getSchedulingTime() {
        Long length = null;
        if (this.taskScheduled != null && this.taskAnalyzed != null) {
            length = this.taskScheduled - this.taskAnalyzed;
        }
        return length;
    }

    /**
     * Returns the pre-execution time.
     *
     * @return The pre-execution time.
     */
    public Long getPreExecutionTime() {
        Long length = null;
        if (this.executionStart != null && this.taskScheduled != null) {
            length = this.executionStart - this.taskScheduled;
        }
        return length;
    }

    /**
     * Returns the task execution time.
     *
     * @return The task execution time.
     */
    public Long getExecutionTime() {
        Long length = null;
        if (this.executionEnd != null && this.executionStart != null) {
            length = this.executionEnd - this.executionStart;
        }
        return length;
    }

    /**
     * Returns the task post-execution time.
     *
     * @return The task post-execution time.
     */
    public Long getPostExecutionTime() {
        Long length = null;
        if (this.taskEnd != null && this.executionEnd != null) {
            length = this.taskEnd - this.executionEnd;
        }
        return length;
    }

    /**
     * Returns the total task time.
     *
     * @return The total task time.
     */
    public Long getTotalTime() {
        Long length = null;
        if (this.taskEnd != null) {
            length = this.taskEnd - this.taskReception;
        }
        return length;
    }

    /**
     * Marks the task creation time.
     *
     * @param ts timestamp of the creation
     */
    public void setTaskCreated(Long ts) {
        this.taskCreated = ts;
    }

    /**
     * Returns the task creation time.
     *
     * @return task creation time
     */
    public Long getTaskCreated() {
        return taskCreated;
    }

    /**
     * Marks the task analysis setTaskEnd time.
     *
     * @param ts timestamp of the setTaskEnd of task accesses analysis
     */
    public void setTaskAnalyzed(Long ts) {
        this.taskAnalyzed = System.currentTimeMillis();
    }

    /**
     * Returns the task accesses setTaskEnd analysis time.
     * 
     * @return task accesses setTaskEnd analysis time
     */
    public Long getTaskAnalyzed() {
        return taskAnalyzed;
    }

    /**
     * Marks the task scheduling time.
     *
     * @param ts timestamp of the task scheduling
     */
    public void setTaskScheduled(Long ts) {
        this.taskScheduled = System.currentTimeMillis();
    }

    /**
     * Return the time when the task was scheduled.
     * 
     * @return time when the task was scheduled
     */
    public Long getTaskScheduled() {
        return taskScheduled;
    }

    /**
     * Marks the submission of the task to a worker.
     *
     * @param ts timestamp of the execution start
     */
    public void setTaskSubmitted(Long ts) {
        this.taskSubmitted = ts;
    }

    /**
     * Returns the task submission time.
     * 
     * @return task submission time
     */
    public Long getTaskSubmitted() {
        return taskSubmitted;
    }

    /**
     * Marks the start of the task's execution at a given time.
     *
     * @param ts timestamp of the execution start
     */
    public void setExecutionStart(Long ts) {
        this.executionStart = ts;
    }

    /**
     * Returns the task execution start time.
     *
     * @return task execution setTaskEnd time
     */
    public Long getExecutionStart() {
        return executionStart;
    }

    /**
     * Marks the start of the task's execution at a given time.
     *
     * @param ts timestamp of the execution start
     */
    public void setExecutionEnd(Long ts) {
        this.executionEnd = ts;
    }

    /**
     * Returns the task execution setTaskEnd time.
     *
     * @return task execution setTaskEnd time
     */
    public Long getExecutionEnd() {
        return executionEnd;
    }

    /**
     * Marks the task setTaskEnd notification submission time.
     * 
     * @param ts setTaskEnd notification timestamp
     */
    public void setEndNotification(Long ts) {
        this.endNotification = ts;
    }

    /**
     * Returns the time when the setTaskEnd notification was submitted.
     * 
     * @return time when the setTaskEnd notification was submitted.
     */
    public Long getEndNotified() {
        return endNotification;
    }

    /**
     * Marks the task processing end time.
     *
     * @param ts timestamp when the task processing was completed
     */
    public void setTaskEnd(Long ts) {
        this.taskEnd = ts;
    }

    /**
     * Returns the time when the task was completely processed on this agent.
     * 
     * @return time when the task was completely processed
     */
    public Long getTaskEnd() {
        return taskEnd;
    }
}
