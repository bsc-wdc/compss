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

package es.bsc.compss.agent.comm;

import es.bsc.comm.nio.NIONode;
import es.bsc.compss.agent.comm.messages.types.CommTask;
import es.bsc.compss.agent.types.Resource;


/**
 * Operations offered by the Comm Agent.
 */
public interface CommAgent {

    public void print(Object o);

    public void addResources(Resource<?, ?> res);

    public void removeResources(Resource<?, ?> node);

    public void removeNode(String node);

    public void lostNode(String node);

    public void receivedNewTask(NIONode master, CommTask request);

    /**
     * Returns or creates the host from a resource description.
     *
     * @param res remote resource description
     * @return Internal Resource corresponding to the host passed in as parameter
     */
    public es.bsc.compss.types.resources.Resource getNodeFromResource(Resource<?, ?> res);

}
