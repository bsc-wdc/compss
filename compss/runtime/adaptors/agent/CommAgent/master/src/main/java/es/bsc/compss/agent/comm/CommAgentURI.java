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
import es.bsc.compss.agent.comm.messages.types.CommResource;
import es.bsc.compss.nio.NIOUri;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.data.location.ProtocolType;


/**
 * Class to represent internal URIs in Comm Agents.
 */
public class CommAgentURI extends NIOUri {

    private CommResource agent;


    public CommAgentURI() {
        super();
    }

    public CommAgentURI(CommResource agent, NIONode host, String path, ProtocolType schema) {
        super(host, path, schema);
        this.agent = agent;
    }

    /**
     * Constructs a new CommAgentURI out of a NIOUri.
     *
     * @param uri NIOUri to convert to CommAgentURI
     */
    public CommAgentURI(NIOUri uri) {
        super(uri.getHost(), uri.getPath(), uri.getProtocol());
        NIONode host = uri.getHost();
        String hostName = host.getIp();
        if (hostName == null) {
            hostName = COMPSsNode.getMasterName();
        }
        this.agent = new CommResource(hostName, host.getPort());
    }

    public CommResource getAgent() {
        return agent;
    }

}
