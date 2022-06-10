/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.api;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.worker.COMPSsException;


public interface TaskMonitor {

    /**
     * Actions to be performed by monitor on task creation.
     */
    public void onCreation();

    /**
     * Actions to be performed by monitor on data access.
     */
    public void onAccessesProcessed();

    /**
     * Actions to be performed by monitor on task schedule.
     */
    public void onSchedule();

    /**
     * Actions to be performed by monitor on task submission.
     */
    public void onSubmission();

    /**
     * Actions to be performed by monitor when the designed worker receives the necessary data value to run the task.
     */
    public void onDataReception();

    /**
     * Actions to be performed by the monitor when a new {@code type}-value, identyfied by the Id {@code dataId}, has
     * been generated at location {@code location} according to the parameter on position {@code paramId} of the task
     * with name {@code paramName}.
     *
     * @param paramId Parameter id.
     * @param description value description
     */
    public void valueGenerated(int paramId, TaskResult description);

    /**
     * Actions to be performed by monitor on task execution abortion.
     */
    public void onAbortedExecution();

    /**
     * Actions to be performed by monitor on task execution error.
     */
    public void onErrorExecution();

    /**
     * Actions to be performed by monitor on task execution failure.
     */
    public void onFailedExecution();

    /**
     * Actions to be performed by monitor on task execution COMPSs exception.
     * 
     * @param e Exception raised during the task execution
     */
    public void onException(COMPSsException e);

    /**
     * Actions to be performed by monitor on task execution success.
     */
    public void onSuccesfulExecution();

    /**
     * Actions to be performed by monitor on task cancellation.
     */
    public void onCancellation();

    /**
     * Actions to be performed by monitor on task completion.
     */
    public void onCompletion();

    /**
     * Actions to be performed by monitor on task failure.
     */
    public void onFailure();

    /**
     * Actions to be performed by monitor on a task execution start.
     */
    public void onExecutionStart();

    /**
     * Actions to be performed by monitor on a task execution start at a given time.
     * 
     * @param ts execution start timestamp
     */
    public void onExecutionStartAt(long ts);

    /**
     * Actions to be performed by monitor on a task execution end.
     */
    public void onExecutionEnd();

    /**
     * Actions to be performed by monitor on a task execution end at a given time.
     * 
     * @param ts execution end timestamp
     */
    public void onExecutionEndAt(long ts);


    /**
     * Class representing a result of a task execution.
     */
    public static class TaskResult {

        private DataType type;
        private String dataName;
        private String dataLocation;


        public TaskResult() {
        }

        /**
         * Constructs an object describing a result from a task execution.
         * 
         * @param type type of the data produced
         * @param dataName name of the data produced
         * @param dataLocation location where the value has been stored
         */
        public TaskResult(DataType type, String dataName, String dataLocation) {
            this.type = type;
            this.dataName = dataName;
            this.dataLocation = dataLocation;
        }

        public DataType getType() {
            return type;
        }

        public void setType(DataType type) {
            this.type = type;
        }

        public String getDataName() {
            return dataName;
        }

        public void setDataName(String dataName) {
            this.dataName = dataName;
        }

        public String getDataLocation() {
            return dataLocation;
        }

        public void setDataLocation(String dataLocation) {
            this.dataLocation = dataLocation;
        }

    }

    public static class CollectionTaskResult extends TaskResult {

        private TaskResult[] subelements;


        public CollectionTaskResult() {
        }

        public CollectionTaskResult(DataType type, String dataName, String dataLocation, TaskResult[] subelements) {
            super(type, dataName, dataLocation);
            this.subelements = subelements;
        }

        public TaskResult[] getSubelements() {
            return subelements;
        }

        public void setSubelements(TaskResult[] subelements) {
            this.subelements = subelements;
        }

    }
}
