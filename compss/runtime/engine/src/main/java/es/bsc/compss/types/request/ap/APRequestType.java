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
package es.bsc.compss.types.request.ap;

/**
 * Contains the different types of request that the Access Processor can response.
 */
public enum APRequestType {
    /*
     * WARNING: THESE EVENTS MUST ALSO BE DEFINED IN TraceEvent.java FOR TRACING
     */

    ANALYSE_TASK, // Analyse a new task
    UPDATE_GRAPH, // Update task graph
    WAIT_FOR_ALL_TASKS, // Waits for all submitted tasks
    END_OF_APP, // End of application
    ALREADY_ACCESSED, // Data has already been accessed
    REGISTER_REMOTE_OBJECT, // Registers an object whose value is on remote nodes
    REGISTER_REMOTE_FILE, // Registers an object whose value is on remote nodes
    REGISTER_DATA_ACCESS, // Register a new data access and waits for it to be available (not necessarily on the node)
    TRANSFER_OPEN_FILE, // Request an open file transfer
    TRANSFER_RAW_FILE, // Request a raw file transfer
    TRANSFER_OBJECT, // Request an object transfer
    IS_OBJECT_HERE, // Checks if the given object is available
    SET_OBJECT_VERSION_VALUE, // Sets a new version to a given object
    GET_LAST_RENAMING, // Returns the last renaming of an object
    BLOCK_AND_GET_RESULT_FILES, // Locks the execution until the result files are transferred
    UNBLOCK_RESULT_FILES, // Unlocks the result files
    SHUTDOWN, // Shutdown request
    GRAPHSTATE, // Requests the task graph state
    TASKSTATE, // Requests a task state
    DELETE_FILE, // Deletes the given file
    FINISH_DATA_ACCESS, // Marks as finished the data access
    DEBUG, // Enables the debug
    DEREGISTER_OBJECT, // Unregisters a given object
    CANCEL_ALL_TASKS, // Cancels all application tasks
    CANCEL_TASK_GROUP, // Cancel all tasks in the given task group
    REMOVE_APP_DATA, // Removes all the data associated to an application
}
