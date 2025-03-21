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
package es.bsc.compss.executor.external.commands;

public interface ExternalCommand {

    public static enum CommandType {
        PING, PONG, // Test the communication channel
        QUIT, // Finish execution

        // REQUIRED BY THE CHANNEL BUILDER MANAGER
        START_WORKER, // Request to start the worker
        WORKER_STARTED, // Notification that worker is ready
        GET_ALIVE, // Request alive subcomponents
        ALIVE_REPLY, // Notification with alive subcomponents
        CREATE_CHANNEL, // Request the creation of the communication channel
        CHANNEL_CREATED, // Notify the creation of the communciation channel

        // REQUIRED BY THE WORKER
        ADD_EXECUTOR, // Add new executor
        ADD_EXECUTOR_FAILED, // New executor was not properly added
        ADDED_EXECUTOR, // Added new executor
        QUERY_EXECUTOR_ID, // Query the pid of the process hosting the
        REPLY_EXECUTOR_ID, // Response to the executor pid query
        REMOVE_EXECUTOR, // Remove executor
        REMOVED_EXECUTOR, // Removed executor

        // REQUIRED BY THE EXECUTOR
        REGISTER_CE, // Register CE
        EXECUTE_TASK, // Execute a task
        EXECUTE_NESTED_TASK, // Execute a task
        END_TASK, // Task finished
        COMPSS_EXCEPTION, // Task raised a COMPSsException
        CANCEL_TASK, // Task to be cancelled

        // REQUIRED BY THE EXECUTOR TO ACCESS DATA
        ACCESSED_FILE, // Executor checks whether a file has been accessed by a task
        GET_FILE, // Executors gets a file
        GET_DIRECTORY, // Executor gets a directory
        GET_OBJECT, // Executor gets a binding object
        OPEN_FILE, // Executor requires a file
        CLOSE_FILE, // Executor ended access to the file
        DELETE_FILE, // Executor deletes a file
        DELETE_OBJECT, // Executor deletes a binding object

        // Executor reaches flow control instructions
        BARRIER, // Barrier
        BARRIER_NEW, // Barrier
        BARRIER_GROUP, // Barrier for a task group
        OPEN_TASK_GROUP, // Open a task group
        CLOSE_TASK_GROUP, // closing a task group
        CANCEL_TASK_GROUP, // Cancel a task group
        NO_MORE_TASKS, // Sync until all previously-submitted tasks have finish

        SYNCH, // Expected synch notification
        REMOVE, // Remove data
        SERIALIZE, // Serialize data
        ERROR // Error
    }


    public static final String TOKEN_SEP = " ";


    public CommandType getType();

    public String getAsString();

}
