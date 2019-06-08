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
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandDataReceived extends Command implements Externalizable {

    private int transfergroupID;


    /**
     * Creates a new CommandDataReceived for externalization.
     */
    public CommandDataReceived() {
        super();
    }

    /**
     * Creates a new CommandDataReceived instance.
     * 
     * @param agent Associated NIOAgent.
     * @param transfergroupID Transfer group Id.
     */
    public CommandDataReceived(NIOAgent agent, int transfergroupID) {
        super(agent);
        this.transfergroupID = transfergroupID;
    }

    @Override
    public CommandType getType() {
        return CommandType.DATA_RECEIVED;
    }

    @Override
    public void handle(Connection c) {
        c.finishConnection();
        this.agent.copiedData(this.transfergroupID);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.transfergroupID = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.transfergroupID);
    }

    @Override
    public String toString() {
        return "Data for transfer group" + this.transfergroupID + "has been received in the remote worker";
    }

}
