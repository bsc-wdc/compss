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

package es.bsc.compss.agent.rest.types;

import java.util.List;


/**
 * Interface to to interact with the Agent hosting the Agent Request.
 */
public interface RESTAgentRequestHandler {

    /**
     * Notifies the end of the Request to an external orchestrator process through REST.
     * 
     * @param host agent that requested the request execution.
     * @param method HTTP method used to notify the agent
     * @param operation path of the host to invoke
     */
    public void notifyOrchestrator(String host, OrchestratorNotification.HttpMethod method, String operation);

    /**
     * Shut down the agent that handled the request execution.
     * 
     * @param forwardTo List of other agents to shutdown.
     */
    public void powerOff(List<String> forwardTo);

}
