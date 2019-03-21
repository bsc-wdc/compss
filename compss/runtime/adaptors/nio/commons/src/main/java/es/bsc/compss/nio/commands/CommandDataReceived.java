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
import es.bsc.compss.nio.NIOAgent;


public class CommandDataReceived extends Command implements Externalizable {

    private int transfergroupID;


    public CommandDataReceived() {
    }

    public CommandDataReceived(NIOAgent ng, int transfergroupID) {
        super(ng);
        this.transfergroupID = transfergroupID;
    }

    @Override
    public CommandType getType() {
        return CommandType.DATA_RECEIVED;
    }

    @Override
    public void handle(Connection c) {
        c.finishConnection();
        NIOAgent nm = (NIOAgent) agent;
        nm.copiedData(transfergroupID);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        transfergroupID = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(transfergroupID);
    }

    @Override
    public String toString() {
        return "Data for transfer group" + transfergroupID + "has been received in the remote worker";
    }

}
