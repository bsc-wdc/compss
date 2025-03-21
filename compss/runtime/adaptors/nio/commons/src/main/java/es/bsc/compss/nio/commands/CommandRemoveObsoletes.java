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
import java.util.List;


public class CommandRemoveObsoletes extends RetriableCommand {

    // List of the data to erase
    private List<String> obsolete;


    /**
     * Creates a new CommandNewTask for externalization.
     */
    public CommandRemoveObsoletes() {
        super();
    }

    /**
     * Creates a new CommandNewTask instance.
     *
     * @param obsolete List of obsolete files.
     */
    public CommandRemoveObsoletes(List<String> obsolete) {
        this.obsolete = obsolete;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        agent.receivedRemoveObsoletes((NIONode) c.getNode(), this.obsolete);
        c.finishConnection();
    }

    @SuppressWarnings("unchecked")
    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.obsolete = (List<String>) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.obsolete);
    }

    @Override
    public String toString() {
        return "Remove obsoletes " + this.obsolete;
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        agent.handleRemoveObsoletesCommandError(c, this);
    }

    public List<String> getObsolete() {
        return obsolete;
    }

}
