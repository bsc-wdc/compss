/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.implementations.definition;

import es.bsc.compss.types.implementations.ContainerImplementation;
import es.bsc.compss.types.implementations.ContainerImplementation.ContainerExecutionType;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ContainerDescription;
import es.bsc.compss.types.resources.MethodResourceDescription;


/**
 * Class containing all the necessary information to generate a Container implementation of a CE.
 */
public class ContainerDefinition extends ImplementationDefinition<MethodResourceDescription> {

    private final ContainerExecutionType internalExecutionType;
    private final String internalFunc;
    private final String internalBinary;

    private final String workingDir;
    private final boolean failByEV;

    private final ContainerDescription container;


    /**
     * Creates a new ImplementationDefinition to create a container core element implementation.
     * 
     * @param signature Binary signature.
     * @param internalExecutionType ContainerExecutionType "PYTHON"/"BINARY"
     * @param internalFunc Python function path.
     * @param internalBinary Binary path.
     * @param workingDir Working directory.
     * @param failByEV Flag to enable failure with EV.
     * @param container Container Description.
     * @param implConstraints Binary requirements.
     */
    public ContainerDefinition(String signature, ContainerExecutionType internalExecutionType, String internalFunc,
        String internalBinary, String workingDir, boolean failByEV, ContainerDescription container,
        MethodResourceDescription implConstraints) {

        super(signature, implConstraints);

        this.internalExecutionType = internalExecutionType;
        this.internalBinary = internalBinary;
        this.internalFunc = internalFunc;

        this.workingDir = workingDir;
        this.failByEV = failByEV;

        this.container = container;
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new ContainerImplementation(this.internalExecutionType, this.internalFunc, this.internalBinary,
            this.workingDir, this.failByEV, this.container, coreId, implId, getSignature(), getConstraints());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Container Implementation \n");
        sb.append("\t Signature: ").append(getSignature()).append("\n");
        sb.append("\t Container: ").append(this.container).append("\n");
        sb.append("\t InternalExecutionType: ").append(this.internalExecutionType).append("\n");
        sb.append("\t InternalBinary: ").append(this.internalBinary).append("\n");
        sb.append("\t InternalFunction: ").append(this.internalFunc).append("\n");
        sb.append("\t Constraints: ").append(getConstraints());
        return sb.toString();
    }

}
