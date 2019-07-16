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
package es.bsc.compss.api;

import es.bsc.compss.types.annotations.parameter.DataType;


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
     * Actions to be performed by the monitor when a new {@code type}-value, identyfied by the Id {@code dataId}, has
     * been generated at location {@code location} according to the parameter on position {@code paramId} of the task
     * with name {@code paramName}.
     *
     * @param paramId      Parameter id.
     * @param paramName    Name of the parameter.
     * @param paramType    Parameter type.
     * @param dataId       Data Management Id.
     * @param dataLocation Value location.
     */
    public void valueGenerated(int paramId, String paramName, DataType paramType, String dataId, Object dataLocation);

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
     */
    public void onException();

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
}
