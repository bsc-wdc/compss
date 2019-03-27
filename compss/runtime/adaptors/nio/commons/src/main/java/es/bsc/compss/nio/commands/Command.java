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
package es.bsc.compss.nio.commands;

import es.bsc.comm.Connection;
import java.io.Externalizable;

import es.bsc.compss.nio.NIOAgent;


public abstract class Command implements Externalizable {

    public enum CommandType {
        NEW_TASK, // Send a new task to a node with a list of the files and its locations
        DATA_DEMAND, // Ask a node for some data
        DATA_NEGATE, // Can not send the data now
        DATA_RECEIVED, // Notify the master that the worker has received the data
        TASK_DONE, // Notify the master that the task has been done
        START_WORKER, // Tell the worker to start
        CHECK_WORKER, // Checks if the worker has started
        CHECK_WORKER_ACK, // Notify the master that the worker has been started
        STOP_WORKER, // Tell the worker to shutdown
        STOP_WORKER_ACK, // Lets the master know that the worker is stopping
        GEN_TRACE_PACKAGE, // Generate Trace package
        GEN_TRACE_PACKAGE_DONE, // Notification of the end of trace package
        GEN_WORKERS_INFO, // Generate worker debug log files
        GEN_WORKERS_INFO_DONE, // Notification of the end of worker debug log files generation
        STOP_EXECUTOR, // Tell the worker to stop the execution manager
        STOP_EXECUTOR_ACK, // Notify that the execution manager is stopped
        RESOURCES_INCREASE, // Notifies the worker that new resources are available
        RESOURCES_INCREASED, // Notifies the master that new resources are ready
        RESOURCES_REDUCE, // Notifies the worker that some resources are no longer available
        RESOURCES_REDUCED // Notifies the master that the resources have been released
    }


    protected NIOAgent agent;


    /**
     * Instantiates a new command
     */
    public Command() {
        // Only for externalization
    }

    /**
     * Instantiates a new command assigned to a given agent @agent
     * 
     * @param agent
     */
    public Command(NIOAgent agent) {
        this.agent = agent;
    }

    /**
     * Returns the agent assigned to the command
     * 
     * @return
     */
    public NIOAgent getAgent() {
        return this.agent;
    }

    /**
     * Assigns a new agent to the command
     * 
     * @param agent
     */
    public void setAgent(NIOAgent agent) {
        this.agent = agent;
    }

    /**
     * Returns the command type
     * 
     * @return
     */
    public abstract CommandType getType();

    /**
     * Invokes the command handler
     * 
     * @param c
     */
    public abstract void handle(Connection c);

}
