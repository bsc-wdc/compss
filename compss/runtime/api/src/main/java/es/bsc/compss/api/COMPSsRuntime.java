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
     * @return workflow being executed
     */
    Workflow registerWorkflow(String parallelismSource, ApplicationRunner runner);

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

    /*
     * *****************************************************************************************************************
     * DATA ACCESS METHODS
     ******************************************************************************************************************/

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
