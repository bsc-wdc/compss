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
package es.bsc.compss.nio.commands.workerfiles;

import es.bsc.comm.Connection;
import es.bsc.compss.nio.NIOAgent;

import es.bsc.compss.nio.commands.Command;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;
import java.util.Set;


public class CommandGenerateDebugFilesDone implements Command {

    Set<String> logPaths;


    public CommandGenerateDebugFilesDone() {
        super();
    }

    public CommandGenerateDebugFilesDone(Set<String> logPaths) {
        super();
        this.logPaths = logPaths;
    }

    @Override
    public void handle(NIOAgent agent, Connection c) {
        agent.notifyDebugFilesDone(logPaths);
    }

    @Override
    public String toString() {
        return "GeneratingWorkerDebugFilesDone";
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        this.logPaths = (Set<String>) oi.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        oo.writeObject(this.logPaths);
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        agent.handleGenerateWorkerDebugDoneCommandError(c, this);

    }

}
