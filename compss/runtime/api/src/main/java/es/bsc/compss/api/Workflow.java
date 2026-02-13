/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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


public interface Workflow {

    /**
     * Returns the id of the workflow.
     *
     * @return id of the workflow
     */
    Long getId();

    /**
     * Deregisters the workflow from the runtime.
     */
    void deregister();

    /**
     * Creates a new task group.
     *
     * @param groupName Group name.
     * @param implicitBarrier {@literal true}, if the task group requires a barrier
     */
    void openTaskGroup(String groupName, boolean implicitBarrier);

    /**
     * Closes an existing task group.
     *
     * @param groupName Group name.
     */
    void closeTaskGroup(String groupName);

    /**
     * Cancels all tasks belonging to a group of the workflow.
     */
    void cancelTaskGroup(String groupName) throws COMPSsException;

    /**
     * Cancels all tasks of the workflow.
     */
    void cancelApplicationTasks();

    /**
     * Notifies the Runtime that there are no more tasks created by the workflow.
     */
    void noMoreTasks();

    /**
     * Freezes the code execution until all previous tasks have been executed.
     */
    void barrier();

    /**
     * Freezes the code execution until all previous tasks have been executed. The noMoreTasks parameter indicates
     * whether to expect new tasks after the barrier or not.
     *
     * @param noMoreTasks Whether the application will spawn more tasks or not.
     */
    void barrier(boolean noMoreTasks);

    /**
     * Freezes the code execution until all the tasks of the group have finished execution. The name of the group to
     * wait is given as a parameter.
     *
     * @param groupName Name of the group to wait.
     * @throws COMPSsException Custom COMPSs exception to handle groups.
     */
    void barrierGroup(String groupName) throws COMPSsException;

    /**
     * Bind the last known version of a file in the runtime system with another dataId.
     *
     * @param fileName File name.
     * @param dataId data to bind the object
     * @return {@literal true} if the data was bound to a previous version; {@literal false} otherwise.
     */
    boolean bindExistingVersionToData(String fileName, String dataId);

    /**
     * Bind the last known version of an object in the runtime system with another dataId.
     *
     * @param o Object.
     * @param dataId data to bind the object
     * @return {@literal true} if the data was bound to a previous version; {@literal false} otherwise.
     */
    boolean bindExistingVersionToData(Object o, String dataId);

    /**
     * Removes the given object {@code o}.
     *
     * @param o Object.
     */
    boolean removeObject(Object o);

    /**
     * Checkpoint of the tasks and data.
     */
    void snapshot();
}
