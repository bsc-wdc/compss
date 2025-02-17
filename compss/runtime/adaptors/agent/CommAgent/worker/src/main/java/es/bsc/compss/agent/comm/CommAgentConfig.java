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

import es.bsc.compss.agent.AgentInterfaceConfig;


/**
 * Class containing the description of the configuration of an agent when starting the Comm interface.
 */
public class CommAgentConfig extends AgentInterfaceConfig {

    private final int port;


    public CommAgentConfig(CommAgentImpl agentItf, int port) {
        super(agentItf);
        this.port = port;
    }

    /**
     * Returns the port where the Comm interface server should be listenning on.
     *
     * @return port number on which the server should be listenning.
     */
    public int getPort() {
        return port;
    }

    public String getHostName() {
        return "localhost";
    }

}
