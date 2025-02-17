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

import jakarta.xml.bind.annotation.XmlTransient;


/**
 * Interface used by agent's requests to encapsulate what to do after they are processed.
 */
@XmlTransient
public interface RESTAgentRequestListener {

    /**
     * The request being monitored has completed its execution.
     * 
     * @param handler element requesting the operation being monitored
     */
    void requestCompleted(RESTAgentRequestHandler handler);

}
