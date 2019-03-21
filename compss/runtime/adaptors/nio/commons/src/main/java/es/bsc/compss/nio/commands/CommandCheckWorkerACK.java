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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandCheckWorkerACK extends Command implements Externalizable {

    private String uuid;
    private String nodeName;


    public CommandCheckWorkerACK() {
        super();
    }

    public CommandCheckWorkerACK(String uuid, String nodeName) {
        super();
        this.uuid = uuid;
        this.nodeName = nodeName;
    }

    @Override
    public CommandType getType() {
        return CommandType.CHECK_WORKER_ACK;
    }

    @Override
    public void handle(Connection c) {
        agent.setWorkerIsReady(nodeName);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        uuid = (String) in.readUTF();
        nodeName = (String) in.readUTF();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeUTF(uuid);
        out.writeUTF(nodeName);
    }

    @Override
    public String toString() {
        return "CommandCheckWorkerACK for deployment ID " + uuid;
    }

}
