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

import es.bsc.compss.nio.NIOParam;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandDataFetch implements Command {

    private NIOParam param;
    private int transferId;


    /**
     * Creates a new CommandDataFetch for externalization.
     */
    public CommandDataFetch() {
        super();
    }

    /**
     * Creates a new CommandDataFetch instance.
     *
     * @param p Parameter to fetch.
     * @param transferId Transfer Id.
     */
    public CommandDataFetch(NIOParam p, int transferId) {
        this.param = p;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        agent.receivedNewDataFetchOrder(this.param, this.transferId);
        c.finishConnection();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.param = (NIOParam) in.readObject();
        this.transferId = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.param);
        out.writeInt(this.transferId);
    }

    @Override
    public String toString() {
        return "Data Fetch " + this.param;
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        // Nothing to do

    }

}
