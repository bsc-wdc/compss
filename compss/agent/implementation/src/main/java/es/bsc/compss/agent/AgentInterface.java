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
package es.bsc.compss.agent;

import org.json.JSONObject;


/**
 * Interface that any external component might call to invoke the agent.
 *
 * @param <T> Class describing the interface configuration
 */
public interface AgentInterface<T extends AgentInterfaceConfig> {

    /**
     * Constructs the configuration description for the agent parsing a string.
     *
     * @param conf JSONObject containing the necessary elements to startup the agent interface.
     * @return Configuration object obtained from parsing the string.
     * @throws AgentException Could not properly parse the string to generate a configuration description.
     */
    public T configure(JSONObject conf) throws AgentException;

    /**
     * Starts any underlying mechanism required by the interface to receive messages from other processes.
     *
     * @param conf Description of the configuration values required by the interface.
     * @throws AgentException Some error raised during the Interface start up.
     */
    public void start(T conf) throws AgentException;

    /**
     * Stops the mechanism supporting the external messages reception.
     */
    public void stop();

}
