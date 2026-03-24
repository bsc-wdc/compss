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

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
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

    /*
     * ************************************************************************************************************
     * **************************************** TASK MANAGEMENT ***************************************************
     * ************************************************************************************************************
     */

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
     * Internal execute task to make API options only as a wrapper.
     *
     * @param signature Method signature.
     * @param onFailure On failure behavior.
     * @param timeOut Amount of time for an application time out.
     * @param isPrioritary Whether the task has priority or not.
     * @param numNodes Number of associated nodes.
     * @param isReduce Whether it is a reduce task.
     * @param reduceChunkSize The size of the chunks to be reduced.
     * @param isReplicated Whether it is a replicated task or not.
     * @param isDistributed Whether the task must be round-robin distributed or not.
     * @param hasTarget Whether the task has a return value or not.
     * @param numReturns Number of return values of the task.
     * @param parameterCount Number of parameters of the task.
     * @param parameters Parameter values.
     * @return The task id.
     */
    int executeTask(String signature, OnFailure onFailure, int timeOut, boolean isPrioritary, int numNodes,
        boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget,
        Integer numReturns, int parameterCount, Object... parameters);

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

    /*
     * ************************************************************************************************************
     * **************************************** DATA MANAGEMENT ***************************************************
     * ************************************************************************************************************
     */
    /**
     * Registers a new Data value.
     *
     * @param type Data type
     * @param stub Local object representing the data
     * @param dataId already existing data with the content
     */
    void registerData(DataType type, Object stub, String dataId);

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
     * Checks if a file has been accessed by the runtime.
     *
     * @param fileName File.
     * @return True if accessed.
     */
    boolean isFileAccessed(String fileName);

    /**
     * Returns the renaming of the file version opened.
     *
     * @param fileName File.
     * @param mode Access mode.
     * @return Renaming of the current file version.
     */
    String openFile(String fileName, Direction mode);

    /**
     * Closes the given file {@code fileName}.
     *
     * @param fileName File version name.
     * @param mode Access mode.
     */
    void closeFile(String fileName, Direction mode);

    /**
     * Retrieves the last version of file with its original name.
     *
     * @param fileName File name.
     */
    void getFile(String fileName);

    /**
     * Deletes the specified version of a file.
     *
     * @param fileName File name.
     * @param waitForData Flag to indicate if we want to wait for the data ready before removing
     * @param applicationDelete {@literal true}, if the file is deleted by the user code; {@literal false}, otherwise
     * @return true if the {@literal fileName} has been deleted, false otherwise.
     */
    boolean deleteFile(String fileName, boolean waitForData, boolean applicationDelete);

    /**
     * Returns last version of directory with its original name.
     *
     * @param dirName Directory name.
     */
    void getDirectory(String dirName);

    /**
     * Returns a copy of the last version of the given object {@code o}.
     *
     * @param o Object.
     * @return In-memory copy of the last version of the given object.
     */
    <T> T getObject(T o);

    /**
     * Removes the given object {@code o}.
     *
     * @param o Object.
     */
    boolean removeObject(Object o);

    /**
     * Returns the renaming of the binding object version opened.
     *
     * @param bindingObjectName Name of the binding object.
     * @return id in the cache.
     */
    String getBindingObject(String bindingObjectName);

    /**
     * Removes the binding object from runtime.
     *
     * @param bindingObjectName Name of the binding object.
     * @return true if the {@code bindingObjectName} has been deleted, false otherwise.
     */
    boolean deleteBindingObject(String bindingObjectName);

    /**
     * Checkpoint of the tasks and data.
     */
    void snapshot();
}
