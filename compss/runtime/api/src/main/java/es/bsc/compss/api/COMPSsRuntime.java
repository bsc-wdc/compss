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

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.worker.COMPSsException;


public interface COMPSsRuntime {

    /*
     * *****************************************************************************************************************
     * START AND STOP METHODS
     ******************************************************************************************************************/
    /**
     * Starts the COMPSs Runtime.
     */
    public void startIT();

    /**
     * Stops the COMPSs Runtime and terminates it if {@code terminate} is true.
     *
     * @param terminate Whether to terminate the Runtime instance or not.
     */
    public void stopIT(boolean terminate);

    /*
     * *****************************************************************************************************************
     * CONFIGURATION
     ******************************************************************************************************************/
    /**
     * Returns the COMPSs application directory.
     *
     * @return The COMPSs application directory.
     */
    public String getApplicationDirectory();

    /**
     * Set the COMPSs application Wall Clock Limit.
     *
     * @appId Application ID.
     * @wcl Wall Clock limit in seconds.
     * @stopRT Flag to indicate if runtime must be stopped when limit exceed
     */
    public void setWallClockLimit(Long appId, long wcl, boolean stopRT);

    /**
     * Returns the number of active resources.
     *
     * @return The number of active resources.
     */
    public int getNumberOfResources();

    /**
     * Requests the creation of {@code numResources} resources.
     *
     * @param appId Application Id.
     * @param numResources Number of resources to create.
     * @param groupName Task group to notify upon resource creation.
     */
    public void requestResources(Long appId, int numResources, String groupName);

    /**
     * Requests the destruction of {@code numResources} resources.
     *
     * @param appId Application Id.
     * @param numResources Number of resources to destroy.
     * @param groupName Task group to notify upon resource destruction.
     */
    public void freeResources(Long appId, int numResources, String groupName);

    /*
     * *****************************************************************************************************************
     * TASK METHODS
     ******************************************************************************************************************/
    /**
     * Registers in the runtime a new application with with parallelism defined by a specific source.
     *
     * @param parallelismSource Element defining the task within the application
     * @param runner Element executing the application's main code.
     * @return Id of the registered application
     */
    public long registerApplication(String parallelismSource, ApplicationRunner runner);

    /**
     * Deregisters an application from the runtime.
     *
     * @param appId Id of the application.
     */
    public void deregisterApplication(Long appId);

    /**
     * Registers a new CoreElement in the Runtime.
     * 
     * @param ced Definition of the core element to add.
     */
    public void registerCoreElement(CoreElementDefinition ced);

    /**
     * Registers a new CoreElement in the Runtime.
     *
     * @param coreElementSignature The coreElement signature.
     * @param implSignature The implementation signature.
     * @param implConstraints The implementation constraints.
     * @param implType The implementation type.
     * @param implIO Whether an implementation is IO.
     * @param prolog commands to execute before task execution
     * @param epilog commands to execute after task execution
     * @param container available if @container is used with other decorators.
     * @param implTypeArgs The implementation specific arguments.
     */
    public void registerCoreElement(String coreElementSignature, String implSignature, String implConstraints,
        String implType, String implLocal, String implIO, String[] prolog, String[] epilog, String[] container,
        String... implTypeArgs);

    /**
     * New Method task for C Binding.
     *
     * @param appId The application id.
     * @param methodClass The method class.
     * @param onFailure On task failure behavior.
     * @param timeOut Amount of time for an application time out.
     * @param methodName The method name.
     * @param isPrioritary Whether the task is set as prioritary or not.
     * @param numNodes The number of nodes required to execute the task.
     * @param isReduce Whether the task is of type reduce.
     * @param reduceChunkSize The chunk size set for executing a reduce operation.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed or not.
     * @param hasTarget Whether the task has a target parameter or not.
     * @param numReturns The number of return values of the method.
     * @param parameterCount The number of parameters of the method.
     * @param parameters An object array containing the method parameters.
     * @return
     */
    public int executeTask(Long appId, String methodClass, String onFailure, int timeOut, String methodName,
        boolean isPrioritary, int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated,
        boolean isDistributed, boolean hasTarget, Integer numReturns, int parameterCount, Object... parameters);

    /**
     * New Method task for Python Binding.
     *
     * @param appId The application id.
     * @param signature The method signature.
     * @param onFailure On task failure behavior.
     * @param timeOut Amount of time for an application time out.
     * @param isPrioritary Whether the task is set as prioritary or not.
     * @param numNodes The number of nodes required to execute the task.
     * @param isReduce Whether the task is of type reduce.
     * @param reduceChunkSize The chunk size set for executing a reduce operation.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed or not.
     * @param hasTarget Whether the task has a target parameter or not.
     * @param numReturns The number of return values of the method.
     * @param parameterCount The number of parameters of the method.
     * @param parameters An object array containing the method parameters.
     * @return
     */
    public int executeTask(Long appId, String signature, String onFailure, int timeOut, boolean isPrioritary,
        int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, Integer numReturns, int parameterCount, Object... parameters);

    /**
     * New Method Task for Loader.
     *
     * @param appId The application id.
     * @param lang The application language.
     * @param methodClass The method class.
     * @param methodName The method name.
     * @param isPrioritary Whether the task is set as prioritary or not.
     * @param numNodes The number of nodes required to execute the task.
     * @param isReduce Whether the task is of type reduce.
     * @param reduceChunkSize The chunk size set for executing a reduce operation.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed or not.
     * @param hasTarget Whether the task has a target parameter or not.
     * @param parameterCount The number of parameters of the method.
     * @param onFailure On task failure behavior.
     * @param timeOut Amount of time for an application time out.
     * @param parameters An object array containing the method parameters.
     * @return
     */
    public int executeTask(Long appId, Lang lang, String methodClass, String methodName, boolean isPrioritary,
        int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, int parameterCount, OnFailure onFailure, int timeOut, Object... parameters);

    /**
     * New service task.
     *
     * @param appId The application id.
     * @param namespace The service namespace.
     * @param service The service endpoint.
     * @param port The service port.
     * @param operation The service operation.
     * @param isPrioritary Whether the task is set as prioritary or not.
     * @param numNodes The number of nodes required to execute the task.
     * @param isReduce Whether the task is of type reduce.
     * @param reduceChunkSize The chunk size set for executing a reduce operation.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed or not.
     * @param hasTarget Whether the task has a target parameter or not.
     * @param parameterCount The number of parameters of the method.
     * @param onFailure On task failure behavior.
     * @param timeOut Amount of time for a task timeOut.
     * @param parameters An object array containing the method parameters.
     * @return
     */
    public int executeTask(Long appId, String namespace, String service, String port, String operation,
        boolean isPrioritary, int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated,
        boolean isDistributed, boolean hasTarget, int parameterCount, OnFailure onFailure, int timeOut,
        Object... parameters);

    /**
     * New HTTP task.
     *
     * @param appId The application id.
     * @param isPrioritary Whether the task is set as prioritary or not.
     * @param numNodes The number of nodes required to execute the task.
     * @param isReduce Whether the task is of type reduce.
     * @param reduceChunkSize The chunk size set for executing a reduce operation.
     * @param isReplicated Whether the task must be replicated or not.
     * @param isDistributed Whether the task must be distributed or not.
     * @param hasTarget Whether the task has a target parameter or not.
     * @param parameterCount The number of parameters of the method.
     * @param onFailure On task failure behavior.
     * @param timeOut Amount of time for a task timeOut.
     * @param parameters An object array containing the method parameters.
     * @return
     */
    public int executeTask(Long appId, String declareMethodFullyQualifiedName, boolean isPrioritary, int numNodes,
        boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget,
        int parameterCount, OnFailure onFailure, int timeOut, Object... parameters);

    /**
     * Notifies the Runtime that there are no more tasks created by the current appId.
     *
     * @param appId The application id.
     */
    public void noMoreTasks(Long appId);

    /**
     * Freezes the task generation until all previous tasks have been executed.
     *
     * @param appId The application id.
     */
    public void barrier(Long appId);

    /**
     * Freezes the task generation until all previous tasks have been executed. The noMoreTasks parameter indicates
     * whether to expect new tasks after the barrier or not.
     *
     * @param appId The application id.
     * @param noMoreTasks Whether the application will spawn more tasks or not.
     */
    public void barrier(Long appId, boolean noMoreTasks);

    /**
     * Freezes the task generation until all the tasks of the group have finished execution. The name of the group to
     * wait is given as a parameter.
     *
     * @param appId The application id.
     * @param groupName Name of the group to wait.
     * @throws COMPSsException Custom COMPSs exception to handle groups.
     */
    public void barrierGroup(Long appId, String groupName) throws COMPSsException;

    /**
     * Unregisters an object to eventually free its memory.
     *
     * @param appId The application id.
     * @param o The object to register.
     */
    public void deregisterObject(Long appId, Object o);

    /**
     * Creates a new task group.
     *
     * @param groupName Group name.
     * @param implicitBarrier {@literal true}, if the task group requires a barrier
     */
    public void openTaskGroup(String groupName, boolean implicitBarrier, Long appId);

    /**
     * Closes an existing task group.
     *
     * @param groupName Group name.
     */
    public void closeTaskGroup(String groupName, Long appId);

    /**
     * Cancels all tasks of the application.
     * 
     * @param appId Application id.
     */
    public void cancelTaskGroup(String groupName, Long appId) throws COMPSsException;

    /**
     * Checkpoint of the tasks and data.
     *
     * @param appId The application id.
     */
    public void snapshot(Long appId);

    /*
     * *****************************************************************************************************************
     * DATA ACCESS METHODS
     ******************************************************************************************************************/
    /**
     * Registers a new Data value.
     *
     * @param appId Id of the application accessing the data.
     * @param type Data type
     * @param stub Local object representing the data
     * @param dataId already existing data with the content
     */
    public void registerData(Long appId, DataType type, Object stub, String dataId);

    /**
     * Checks if a file has been accessed by the runtime.
     *
     * @param appId Id of the application checking the file access
     * @param fileName File to check
     * @return True if accessed.
     */
    public boolean isFileAccessed(Long appId, String fileName);

    /**
     * Returns the renaming of the file version opened.
     *
     * @param appId Id of the application accessing the data.
     * @param fileName File name.
     * @param mode Access mode.
     * @return
     */
    public String openFile(Long appId, String fileName, Direction mode);

    /**
     * Returns the renaming of the file version opened.
     *
     * @param appId Id of the application accessing the data.
     * @param dirName Directory name.
     * @param mode Access mode.
     * @return
     */
    public String openDirectory(Long appId, String dirName, Direction mode);

    /**
     * Close the opened file version.
     *
     * @param appId Id of the application accessing the data.
     * @param fileName File name.
     * @param mode Access mode.
     */
    public void closeFile(Long appId, String fileName, Direction mode);

    /**
     * Deletes the specified version of a file.
     *
     * @param appId Id of the application requesting the file deletion
     * @param fileName File name.
     * @return true if the {@literal fileName} has been deleted, false otherwise.
     */
    public boolean deleteFile(Long appId, String fileName);

    /**
     * Deletes the specified version of a file.
     *
     * @param appId Id of the application requesting the file deletion
     * @param fileName File name.
     * @param waitForData Flag to indicate if we want to wait for the data ready before removing
     * @param applicationDelete {@literal true}, if the file is deleted by the user code; {@literal false}, otherwise
     * @return true if the {@literal fileName} has been deleted, false otherwise.
     */
    public boolean deleteFile(Long appId, String fileName, boolean waitForData, boolean applicationDelete);

    /**
     * Bind the last known version of a file in the runtime system with another dataId.
     *
     * @param appId Id of the application accessing the file
     * @param fileName File name.
     * @param dataId data to bind the object
     * @return {@literal true} if the data was bound to a previous version; {@literal false} otherwise.
     */
    public boolean bindExistingVersionToData(Long appId, String fileName, String dataId);

    /**
     * Returns last version of file with its original name.
     *
     * @param appId Application id.
     * @param fileName File name.
     */
    public void getFile(Long appId, String fileName);

    /**
     * Returns last version of directory with its original name.
     *
     * @param appId Application id.
     * @param dirName Directory name.
     */
    public void getDirectory(Long appId, String dirName);

    /**
     * Returns the renaming of the binding object version opened.
     *
     * @param appId Application id.
     * @param bindingObjectName Name of the binding object.
     * @return id in the cache.
     */
    public String getBindingObject(Long appId, String bindingObjectName);

    /**
     * Removes the binding object from runtime.
     *
     * @param appId Application id.
     * @param bindingObjectName Name of the binding object.
     * @return true if the {@code bindingObjectName} has been deleted, false otherwise.
     */
    public boolean deleteBindingObject(Long appId, String bindingObjectName);

    /**
     * Cancels all tasks of the application.
     * 
     * @param appId Application id.
     */
    public void cancelApplicationTasks(Long appId);

    /*
     * *****************************************************************************************************************
     * TOOLS ACCESS FOR BINDINGS
     ******************************************************************************************************************/
    /**
     * Emits a tracing event.
     *
     * @param type Event type.
     * @param id Event id.
     */
    public void emitEvent(int type, long id);

}
