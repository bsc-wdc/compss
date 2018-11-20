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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.nio.NIOAgent;
import es.bsc.comm.Connection;


public class CommandDataNegate extends Command implements Externalizable {

    private NIOData d;

    // Whether the node has the file or not
    private boolean hosted;


    public CommandDataNegate(NIOAgent ng, NIOData d, boolean hosted) {
        super(ng);
        this.d = d;
        this.hosted = hosted;
    }

    @Override
    public CommandType getType() {
        return CommandType.DATA_NEGATE;
    }

    @Override
    public void handle(Connection c) {
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        d = (NIOData) in.readObject();
        hosted = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(d);
        out.writeBoolean(hosted);
    }

    @Override
    public String toString() {
        return "Data " + d + " can't be send" + (hosted ? ", although it is in the node" : " since the node does not have it");
    }

}
