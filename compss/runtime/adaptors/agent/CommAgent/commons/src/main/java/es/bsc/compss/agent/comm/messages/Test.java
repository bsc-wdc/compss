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

package es.bsc.compss.agent.comm.messages;

import es.bsc.comm.Connection;
import es.bsc.compss.agent.comm.CommAgent;

import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.commands.Command;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Comm Agent message to validate that the agent is up.
 */
public class Test implements Command {

    private int val;


    public Test() {
    }

    public Test(int val) {
        this.val = val;
    }

    @Override
    public void handle(NIOAgent agent, Connection cnctn) {
        ((CommAgent) agent).print(this);
        cnctn.finishConnection();
    }

    @Override
    public String toString() {
        return "Test with value " + val;
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeInt(val);
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        val = oi.readInt();
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        System.err.println("Error processing test command in connection " + c.hashCode());

    }

}
