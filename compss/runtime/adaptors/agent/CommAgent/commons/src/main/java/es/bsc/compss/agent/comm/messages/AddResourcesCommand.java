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
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.commands.Command;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


/**
 * Command to add new resources into the resource pool of the agent.
 */
public class AddResourcesCommand implements Command {

    private static final long serialVersionUID = 1L;

    private Resource<?, ?> res;


    public AddResourcesCommand() {
    }

    public AddResourcesCommand(Resource<?, ?> res) {
        this.res = res;
    }

    @Override
    public void handle(NIOAgent agent, Connection cnctn) {
        ((CommAgent) agent).addResources(this.res);
        cnctn.finishConnection();
    }

    @Override
    public void writeExternal(ObjectOutput oo) throws IOException {
        try {
            oo.writeObject(this.res);
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void readExternal(ObjectInput oi) throws IOException, ClassNotFoundException {
        try {
            this.res = (Resource<?, ?>) oi.readObject();
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public void error(NIOAgent agent, Connection c) {
        System.err.println("Error processing add resources command in connection " + c.hashCode());

    }

}
