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

import es.bsc.compss.types.implementations.BinaryImplementation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.MethodResourceDescription;


/**
 * Class containing all the necessary information to generate a Binary implementation of a CE.
 */
public class BinaryDefinition extends ImplementationDefinition<MethodResourceDescription> {

    private final String binary;
    private final String workingDir;
    private final boolean failByEV;

    protected BinaryDefinition(String signature, String binary, String workingDir, boolean failByEV,
    		MethodResourceDescription implConstraints) {
    	
        super(signature, implConstraints);
        
        this.binary = binary;
        this.workingDir = workingDir;
        this.failByEV = failByEV;
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {

        return new BinaryImplementation(binary, workingDir, failByEV, coreId, implId,
        		this.getSignature(), this.getConstraints());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("Binary Implementation \n");
        sb.append("\t Signature: ").append(this.getSignature()).append("\n");
        sb.append("\t Binary: ").append(this.binary).append("\n");
        sb.append("\t IO: ").append(!this.getConstraints().usesCPUs()).append("\n");
        sb.append("\t Working directory: ").append(this.workingDir).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Constraints: ").append(this.getConstraints());
        return sb.toString();
    }
}
