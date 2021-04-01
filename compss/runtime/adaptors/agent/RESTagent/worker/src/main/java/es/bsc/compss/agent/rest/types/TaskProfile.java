/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
    private Long taskCreation;
    private Long taskAnalyzed;
    private Long taskScheduled;
    private Long executionStart;
    private Long executionEnd;
    private Long taskEnd;


    /**
     * Initializes a new TaskProfile instance.
     */
    public TaskProfile() {
        this.taskReception = System.currentTimeMillis();
        this.taskCreation = null;
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
        if (this.taskCreation != null) {
            length = this.taskCreation - this.taskReception;
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
        if (this.taskAnalyzed != null && this.taskCreation != null) {
            length = this.taskAnalyzed - this.taskCreation;
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
     * Marks the task as created.
     */
    public void created() {
        this.taskCreation = System.currentTimeMillis();
    }

    /**
     * Marks the task as analyzed.
     */
    public void processedAccesses() {
        this.taskAnalyzed = System.currentTimeMillis();
    }

    /**
     * Marks the task as scheduled.
     */
    public void scheduled() {
        this.taskScheduled = System.currentTimeMillis();
    }

    /**
     * Marks the start of the task's execution.
     */
    public void submitted() {
        this.executionStart = System.currentTimeMillis();
    }

    /**
     * Marks the task as executed.
     */
    public void finished() {
        this.executionEnd = System.currentTimeMillis();
    }

    /**
     * Marks the task as ended.
     */
    public void end() {
        this.taskEnd = System.currentTimeMillis();
    }

}
