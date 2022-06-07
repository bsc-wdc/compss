/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.types.implementations.definition.ServiceDefinition;
import es.bsc.compss.types.resources.ServiceResourceDescription;


public class ServiceImplementation extends Implementation {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Generate Dummy service implementation.
     * 
     * @return Dummy service implementation.
     */
    public static ServiceImplementation generateDummy() {
        return new ServiceImplementation(null, null,
            new ImplementationDescription<>(new ServiceDefinition("", "", "", ""), "", false,
                new ServiceResourceDescription("", "", "", 0), null, null));
    }

    public ServiceImplementation() {
        super();
    }

    public ServiceImplementation(Integer coreId, Integer implId,
        ImplementationDescription<ServiceResourceDescription, ServiceDefinition> implDesc) {
        super(coreId, implId, implDesc);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ImplementationDescription<ServiceResourceDescription, ServiceDefinition> getDescription() {
        return (ImplementationDescription<ServiceResourceDescription, ServiceDefinition>) this.implDescription;

    }

    @Override
    public ServiceResourceDescription getRequirements() {
        return this.getDescription().getConstraints();

    }

    public String getOperation() {
        return getDescription().getDefinition().getOperation();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.SERVICE;
    }

}
