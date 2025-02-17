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
package es.bsc.compss.nio.commands;

import es.bsc.comm.Connection;

import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOData;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;


public class CommandShutdown implements Command {

    // List of files to send to the master before shutting down
    private LinkedList<NIOData> filesToSend;


    /**
     * Creates a new CommandShutdown for externalization.
     */
    public CommandShutdown() {
    }

    /**
     * Creates a new CommandShutdown instance.
     *
     * @param filesToSend List of files to send.
     */
    public CommandShutdown(LinkedList<NIOData> filesToSend) {
        this.filesToSend = filesToSend;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        agent.receivedShutdown(c, this.filesToSend);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.filesToSend = (LinkedList<NIOData>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.filesToSend);
    }

    @Override
    public String toString() {
        return "Shutdown";
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        agent.handleShutdownCommandError(c, this);

    }

}
