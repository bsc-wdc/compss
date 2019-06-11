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

import es.bsc.compss.nio.NIOAgent;

import java.io.Externalizable;


public abstract class Command implements Externalizable {

    protected NIOAgent agent;


    /**
     * Instantiates a new command.
     */
    public Command() {
        // Only for externalization
    }

    /**
     * Instantiates a new command assigned to a given agent {@code agent}.
     * 
     * @param agent Associated NIOAgent.
     */
    public Command(NIOAgent agent) {
        this.agent = agent;
    }

    /**
     * Returns the agent assigned to the command.
     * 
     * @return The agent assigned to the command.
     */
    public NIOAgent getAgent() {
        return this.agent;
    }

    /**
     * Assigns a new agent to the command.
     * 
     * @param agent New command agent.
     */
    public void setAgent(NIOAgent agent) {
        this.agent = agent;
    }

    /**
     * Returns the command type.
     * 
     * @return The command type.
     */
    public abstract CommandType getType();

    /**
     * Invokes the command handler.
     * 
     * @param c Connection.
     */
    public abstract void handle(Connection c);

}
