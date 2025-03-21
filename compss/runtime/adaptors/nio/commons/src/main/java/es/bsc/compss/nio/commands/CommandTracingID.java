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

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandTracingID implements Command {

    private int id;
    private int tag;


    /**
     * Creates a new CommandTracingID for externalization.
     */
    public CommandTracingID() {
        super();
    }

    /**
     * Creates a new CommandTracingID instance.
     *
     * @param id Trancing Id.
     * @param tag Tracing tag.
     */
    public CommandTracingID(int id, int tag) {
        this.id = id;
        this.tag = tag;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        agent.addConnectionAndPartner(c, this.id, this.tag);
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.id = in.readInt();
        this.tag = in.readInt();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeInt(this.id);
        out.writeInt(this.tag);
    }

    @Override
    public String toString() {
        return "Request with sender ID: " + this.id;
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        // Nothing to do

    }

}
