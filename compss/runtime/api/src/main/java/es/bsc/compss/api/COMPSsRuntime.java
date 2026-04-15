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

import es.bsc.compss.types.CoreElementDefinition;


public interface COMPSsRuntime {

    /*
     * *****************************************************************************************************************
     * START AND STOP METHODS
     ******************************************************************************************************************/
    /**
     * Starts the COMPSs Runtime.
     */
    void startIT();

    /**
     * Stops the COMPSs Runtime and terminates it if {@code terminate} is true.
     *
     * @param terminate Whether to terminate the Runtime instance or not.
     */
    void stopIT(boolean terminate);

    /*
     * *****************************************************************************************************************
     * CONFIGURATION
     ******************************************************************************************************************/
    /**
     * Returns the COMPSs application log directory.
     *
     * @return The COMPSs application log directory.
     */
    String getApplicationDirectory();

    /**
     * Returns the directory where to store temporary files.
     *
     * @return The directory where to store temporary files.
     */
    String getTempDir();

    /*
     * *****************************************************************************************************************
     * Workflow METHODS
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
    void registerCoreElement(CoreElementDefinition ced);

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
    void registerCoreElement(String coreElementSignature, String implSignature, String implConstraints, String implType,
        String implLocal, String implIO, String[] prolog, String[] epilog, String[] container, String... implTypeArgs);

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
    void emitEvent(int type, long id);

}
