/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.nio.NIOAgent;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.LinkedList;

import es.bsc.comm.Connection;


public class CommandShutdown extends Command implements Externalizable {

    // List of files to send to the master before shutting down
    private LinkedList<NIOData> filesToSend;


    public CommandShutdown() {
    }

    public CommandShutdown(NIOAgent agent, LinkedList<NIOData> l) {
        super(agent);
        this.filesToSend = l;
    }

    @Override
    public CommandType getType() {
        return CommandType.STOP_WORKER;
    }

    @Override
    public void handle(Connection c) {
        agent.receivedShutdown(c, filesToSend);
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        filesToSend = (LinkedList<NIOData>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(filesToSend);
    }

    @Override
    public String toString() {
        return "Shutdown";
    }

}
