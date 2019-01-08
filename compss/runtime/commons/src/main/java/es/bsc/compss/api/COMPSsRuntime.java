/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.annotations.parameter.Direction;


public interface COMPSsRuntime {

    /*
     * *****************************************************************************************************************
     * START AND STOP METHODS
     ******************************************************************************************************************/
    /**
     * Starts the COMPSs Runtime
     */
    public void startIT();

    /**
     * Stops the COMPSs Runtime
     *
     * @param terminate
     */
    public void stopIT(boolean terminate);

    /*
     * *****************************************************************************************************************
     * CONFIGURATION
     ******************************************************************************************************************/
    /**
     * Returns the COMPSs Application Directory
     *
     * @return
     */
    public String getApplicationDirectory();

    /*
     * *****************************************************************************************************************
     * TASK METHODS
     ******************************************************************************************************************/
    /**
     * Registers a new CoreElement in the Runtime
     *
     * @param coreElementSignature
     * @param implSignature
     * @param implConstraints
     * @param implType
     * @param implTypeArgs
     */
    public void registerCoreElement(String coreElementSignature, String implSignature, String implConstraints, String implType,
            String... implTypeArgs);

    /**
     * New Method task for C Binding
     *
     * @param appId
     * @param methodClass
     * @param methodName
     * @param isPrioritary
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, String methodClass, String methodName, boolean isPrioritary, boolean hasTarget, Integer numReturns,
            int parameterCount, Object... parameters);

    /**
     * New Method task for Python Binding
     *
     * @param appId
     * @param signature
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param numReturns
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, String signature, boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed,
            boolean hasTarget, Integer numReturns, int parameterCount, Object... parameters);

    /**
     * New Method Task for Loader
     *
     * @param appId
     * @param monitor
     * @param methodClass
     * @param methodName
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, TaskMonitor monitor, String methodClass, String methodName, boolean isPrioritary, int numNodes,
            boolean isReplicated, boolean isDistributed, boolean hasTarget, int parameterCount, Object... parameters);

    /**
     * New service task
     *
     * @param appId
     * @param monitor
     * @param namespace
     * @param service
     * @param port
     * @param operation
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    public int executeTask(Long appId, TaskMonitor monitor, String namespace, String service, String port, String operation,
            boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed, boolean hasTarget, int parameterCount,
            Object... parameters);

    /**
     * Notifies the Runtime that there are no more tasks created by the current appId
     *
     * @param appId
     */
    public void noMoreTasks(Long appId);

    /**
     * Freezes the task generation until all previous tasks have been executed
     *
     * @param appId
     */
    public void barrier(Long appId);

    /**
     * Freezes the task generation until all previous tasks have been executed. The noMoreTasks parameter indicates
     * whether to expect new tasks after the barrier or not
     *
     * @param appId
     * @param noMoreTasks
     */
    public void barrier(Long appId, boolean noMoreTasks);

    /**
     * Deregisters an object to eventually free its memory
     *
     * @param appId
     * @param o
     */
    public void deregisterObject(Long appId, Object o);

    /*
     * *****************************************************************************************************************
     * DATA ACCESS METHODS
     ******************************************************************************************************************/
    /**
     * Returns the renaming of the file version opened
     *
     * @param fileName
     * @param mode
     * @return
     */
    public String openFile(String fileName, Direction mode);

    /**
     * close the opened file version
     *
     * @param fileName
     * @param mode
     * @return
     */
    public void closeFile(String fileName, Direction mode);

    /**
     * Deletes the specified version of a file
     *
     * @param fileName
     * @return
     */
    public boolean deleteFile(String fileName);

    /**
     * Returns the renaming of the binding object version opened
     *
     * @param objectName
     * @return id in the cache
     */
    public String getBindingObject(String bindingObjectName);

    /**
     * removes the binding object from runtime
     *
     * @param objectName
     * @return id in the cache
     */
    public boolean deleteBindingObject(String bindingObjectName);

    /*
     * *****************************************************************************************************************
     * TOOLS ACCESS FOR BINDINGS
     ******************************************************************************************************************/
    /**
     * Emits a tracing event
     *
     * @param type
     * @param id
     */
    public void emitEvent(int type, long id);

}
