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
package es.bsc.compss.agent.rest.types.messages;

import es.bsc.compss.types.resources.MethodResourceDescription;
import jakarta.xml.bind.annotation.XmlRootElement;


@XmlRootElement(name = "reduceNode")
public class ReduceNodeRequest {

    private String workerName;
    private MethodResourceDescription resources;


    public ReduceNodeRequest() {
    }

    public ReduceNodeRequest(String workerName, MethodResourceDescription mrd) {
        this.workerName = workerName;
        this.resources = mrd;
    }

    public String getWorkerName() {
        return workerName;
    }

    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    public MethodResourceDescription getResources() {
        return resources;
    }

    public void setResources(MethodResourceDescription resources) {
        this.resources = resources;
    }
}
