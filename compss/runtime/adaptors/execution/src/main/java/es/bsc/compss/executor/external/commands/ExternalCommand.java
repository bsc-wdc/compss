/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
        ADDED_EXECUTOR, // Added new executor
        QUERY_EXECUTOR_ID, // Query the pid of the process hosting the
        REPLY_EXECUTOR_ID, // Response to the executor pid query
        REMOVE_EXECUTOR, // Remove executor
        REMOVED_EXECUTOR, // Removed executor

        // REQUIRED BY THE EXECUTOR
        EXECUTE_TASK, // Execute a task
        END_TASK, // Task finished

        REMOVE, // Remove data
        SERIALIZE, // Serialize data
        ERROR, // Error
    }


    public static final String TOKEN_SEP = " ";


    public CommandType getType();

    public String getAsString();

}
