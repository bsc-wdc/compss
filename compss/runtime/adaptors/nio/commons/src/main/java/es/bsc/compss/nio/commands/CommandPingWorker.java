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
import es.bsc.comm.nio.NIONode;
import es.bsc.compss.nio.NIOAgent;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandPingWorker implements Command {

    private String uuid;
    private String nodeName;


    /**
     * Creates a new CommandPingWorker for externalization.
     */
    public CommandPingWorker() {
        super();
    }

    /**
     * Creates a new CommandPingWorker instance.
     *
     * @param uuid Associated application UUID.
     * @param nodeName Worker node name.
     */
    public CommandPingWorker(String uuid, String nodeName) {
        this.uuid = uuid;
        this.nodeName = nodeName;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        if (agent.isMyUuid(this.uuid, this.nodeName)) {
            CommandPingWorkerACK cmd = new CommandPingWorkerACK(this.uuid, this.nodeName);
            c.sendCommand(cmd);
        }
        c.finishConnection();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.uuid = (String) in.readUTF();
        this.nodeName = (String) in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(this.uuid);
        out.writeUTF(this.nodeName);
    }

    @Override
    public String toString() {
        return "CommandPingWorker for deployment ID " + this.uuid + " on nodeName " + this.nodeName;
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        agent.handleNodeIsDownError(this.nodeName);
    }

}
