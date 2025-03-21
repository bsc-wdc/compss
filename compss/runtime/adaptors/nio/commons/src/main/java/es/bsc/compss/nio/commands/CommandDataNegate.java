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


public class CommandDataNegate implements Command {

    private NIOData d;

    // Whether the node has the file or not
    private boolean hosted;


    /**
     * Creates a new CommandDataNegate for externalization.
     */
    public CommandDataNegate() {
        super();
    }

    /**
     * Creates a new CommandDataNegate instance.
     *
     * @param d Data to negate.
     * @param hosted Whether it is hosted or not.
     */
    public CommandDataNegate(NIOData d, boolean hosted) {
        this.d = d;
        this.hosted = hosted;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.d = (NIOData) in.readObject();
        this.hosted = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.d);
        out.writeBoolean(this.hosted);
    }

    @Override
    public String toString() {
        return "Data " + this.d + " can't be send"
            + (this.hosted ? ", although it is in the node" : " since the node does not have it");
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        // Nothing to do

    }

}
