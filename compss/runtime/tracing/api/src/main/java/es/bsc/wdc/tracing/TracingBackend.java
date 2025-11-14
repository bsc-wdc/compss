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

package es.bsc.wdc.tracing;

import java.util.Map;


public interface TracingBackend {

    /**
     * This call enables the instrumentation of ALL created threads from here onwards.
     */
    void enablePThreads();

    /**
     * This call disables the instrumentation of any created threads from here onwards.
     */
    void disablePThreads();

    /**
     * Defines a new event type or updates in the tracing backend.
     *
     * @param type Event type
     */
    void defineEventType(EventType type);

    /**
     * Registers that an event has occurred.
     *
     * @param eventType type of event
     * @param value value of the event
     */
    void emitEvent(int eventType, long value);

    /**
     * Registers that an event has occurred and adds hardware counters.
     *
     * @param eventType type of event
     * @param value value of the event
     */
    void emitEventAndCounters(int eventType, long value);

    /**
     * Registers that a communication event has occurred.
     *
     * @param send {@literal true} if the event corresponds to the sender; {@literal fals} otherwise.
     * @param tag Transfer tag.
     * @param size Transfer size.
     * @param partnerID ID if the counterpart
     * @param ownID ID of the device
     */
    void emitCommunicationEvent(boolean send, int tag, long size, int partnerID, int ownID);

    /**
     * Finalizes the tracing backend.
     */
    void fini();

    /**
     * Checks and updates the environment variables for tracing. In particular, removes the content of the variables
     * defined in REMOVE_ENVIRONMENT_VARIABLES and also removes any link to extrae from CLEAN_ENVIRONMENT_VARIABLES.
     *
     * @param env Environment to prepare for tracing.
     * @param defineExtra Add extra variables.
     */
    void prepareSubProcessEnvironment(Map<String, String> env, Boolean defineExtra);
}
