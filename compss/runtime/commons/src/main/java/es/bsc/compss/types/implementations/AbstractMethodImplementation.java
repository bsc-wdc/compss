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

import es.bsc.compss.types.implementations.definition.AbstractMethodImplementationDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;


public class AbstractMethodImplementation extends Implementation {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;


    /**
     * Generate a dummy method implementation.
     *
     * @param constraints Method resource description for the dummy implementation
     * @return Dummy abstract method implementation.
     */
    public static AbstractMethodImplementation generateDummy(MethodResourceDescription constraints) {
        return new AbstractMethodImplementation(null, null,
            new ImplementationDescription<>(new MethodDefinition("", ""), "", false, constraints, null, null));
    }

    public AbstractMethodImplementation() {
        // For serialization
        super();
    }

    public AbstractMethodImplementation(Integer coreId, Integer implId,
        ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition> implDesc) {
        super(coreId, implId, implDesc);
    }

    @Override
    public ImplementationDescription<MethodResourceDescription, AbstractMethodImplementationDefinition>
        getDescription() {
        return (ImplementationDescription<MethodResourceDescription,
            AbstractMethodImplementationDefinition>) this.implDescription;

    }

    @Override
    public MethodResourceDescription getRequirements() {
        return this.getDescription().getConstraints();

    }

    public AbstractMethodImplementationDefinition getDefinition() {
        return getDescription().getDefinition();
    }

    public String getMethodDefinition() {
        return getDescription().getDefinition().toJSON();
    }

    public MethodType getMethodType() {
        return getDescription().getDefinition().getMethodType();
    }

    @Override
    public TaskType getTaskType() {
        return TaskType.METHOD;
    }

}
