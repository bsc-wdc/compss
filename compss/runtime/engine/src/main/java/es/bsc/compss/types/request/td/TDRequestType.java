/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.request.td;

/**
 * Task Dispatcher requests type.
 */
public enum TDRequestType {
    /*
     * WARNING: THESE EVENTS MUST ALSO BE DEFINED IN TraceEvent.java FOR TRACING
     */

    ACTION_UPDATE, // Update action
    CANCEL_TASKS, // Cancel a task
    CE_REGISTRATION, // Register new coreElement
    EXECUTE_TASKS, // Execute task
    GET_CURRENT_SCHEDULE, // get the current schedule status
    PRINT_CURRENT_GRAPH, // print current task graph
    MONITORING_DATA, // print data for monitoring
    TD_SHUTDOWN, // shutdown
    UPDATE_CEI_LOCAL, // Updates CEI locally
    WORKER_UPDATE_REQUEST // Updates a worker definition
}
