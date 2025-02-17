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
package es.bsc.compss.types.implementations;

import es.bsc.compss.types.implementations.definition.HTTPDefinition;
import es.bsc.compss.types.resources.HTTPResourceDescription;

import java.util.ArrayList;


public class HTTPImplementation extends Implementation {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Generate Dummy HTTP implementation.
     *
     * @return Dummy HTTP implementation.
     */
    public static HTTPImplementation generateDummy() {
        return new HTTPImplementation(null, null,
            new ImplementationDescription<>(new HTTPDefinition("", "", "", "", "", "", "", ""), "", false,
                new HTTPResourceDescription(new ArrayList<String>(), 0), null, null));
    }

    public HTTPImplementation() {
        super();
    }

    public HTTPImplementation(Integer coreId, Integer implId,
        ImplementationDescription<HTTPResourceDescription, HTTPDefinition> implDesc) {

        super(coreId, implId, implDesc);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ImplementationDescription<HTTPResourceDescription, HTTPDefinition> getDescription() {
        return (ImplementationDescription<HTTPResourceDescription, HTTPDefinition>) this.implDescription;
    }

    @Override
    public HTTPResourceDescription getRequirements() {
        return this.getDescription().getConstraints();
    }

    public String getRequest() {
        return getDescription().getDefinition().getRequest();
    }

    public String getResource() {
        return getDescription().getDefinition().getResource();
    }

    public String getPayload() {
        return getDescription().getDefinition().getPayload();
    }

    public String getPayloadType() {
        return getDescription().getDefinition().getPayloadType();
    }

    public String getProduces() {
        return getDescription().getDefinition().getProduces();
    }

    public String getUpdates() {
        return getDescription().getDefinition().getUpdates();
    }

    public String getDefaultReturn() {
        return getDescription().getDefinition().getDefReturn();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.HTTP;
    }
}
