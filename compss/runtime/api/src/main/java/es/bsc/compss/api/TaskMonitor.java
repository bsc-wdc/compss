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
package es.bsc.compss.api;

import es.bsc.compss.worker.COMPSsException;


public interface TaskMonitor extends ParameterCollectionMonitor {

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

}
