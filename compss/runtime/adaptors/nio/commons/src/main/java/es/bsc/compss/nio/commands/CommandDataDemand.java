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


public class CommandDataDemand implements Command {

    private NIOData d;
    private int id;


    /**
     * Creates a new CommandDataDemand for externalization.
     */
    public CommandDataDemand() {
        super();
    }

    /**
     * Creates a new CommandDataDemand instance.
     *
     * @param d Data to request.
     * @param receiverID Receiver Id.
     */
    public CommandDataDemand(NIOData d, int receiverID) {
        this.d = d;
        this.id = receiverID;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        boolean slot = agent.tryAcquireSendSlot(c);
        if (!slot) {
            // There are no slots available
            // TODO: ENABLE DATA NEGATE COMMANDS
            agent.sendData(c, this.d, this.id);
            // agent.sendDataNegate(c, d, true);
        } else {
            // There is a slot and the data exists
            agent.sendData(c, this.d, this.id);
        }
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.d = (NIOData) in.readObject();
        this.id = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.d);
        out.writeInt(this.id);
    }

    @Override
    public String toString() {
        return "Request for sending data " + this.d;
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        agent.checkAndHandleRequestedDataNotAvailableError(c);
    }

}
