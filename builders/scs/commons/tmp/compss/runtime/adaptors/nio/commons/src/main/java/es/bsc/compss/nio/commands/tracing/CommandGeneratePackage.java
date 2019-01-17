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
package es.bsc.compss.nio.commands.tracing;

import es.bsc.compss.nio.commands.Command;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.comm.Connection;


public class CommandGeneratePackage extends Command implements Externalizable {

    public CommandGeneratePackage() {
        super();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to write
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to read
    }

    @Override
    public CommandType getType() {
        return CommandType.GEN_TRACE_PACKAGE;
    }

    @Override
    public void handle(Connection c) {
        agent.generatePackage(c);
    }

    @Override
    public String toString() {
        return "GenerateTraceCommand";
    }

}
