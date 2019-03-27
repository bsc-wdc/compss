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

import es.bsc.comm.Connection;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class CommandResourcesIncrease extends Command implements Externalizable {

    private MethodResourceDescription description;

    public CommandResourcesIncrease() {
        super();
    }

    public CommandResourcesIncrease(MethodResourceDescription description) {
        super();
        this.description = description;
    }

    @Override
    public CommandType getType() {
        return CommandType.RESOURCES_INCREASE;
    }

    @Override
    public void handle(Connection c) {
        agent.increaseResources(description);
        c.sendCommand(new CommandResourcesReduced());
        c.finishConnection();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        description = (MethodResourceDescription) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(description);
    }

    @Override
    public String toString() {
        return "CommandIncreaseResources " + description;
    }

}
