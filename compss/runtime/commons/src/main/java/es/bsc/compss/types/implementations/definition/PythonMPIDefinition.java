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

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.PythonMPIImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;


public class PythonMPIDefinition extends ImplementationDefinition<MethodResourceDescription> {

    private final String declaringClass;
    private final String methodName;
    private final String workingDir;
    private final String mpiRunner;


    protected PythonMPIDefinition(String implSignature, String declaringClass, String methodName, String workingDir,
        String mpiRunner, MethodResourceDescription implConstraints) {

        super(implSignature, implConstraints);

        this.workingDir = workingDir;
        this.mpiRunner = mpiRunner;
        this.declaringClass = declaringClass;
        this.methodName = methodName;
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new PythonMPIImplementation(declaringClass, methodName, workingDir, mpiRunner, coreId, implId,
            this.getSignature(), this.getConstraints());
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("PYTHON MPI Implementation \n");
        sb.append("\t Signature: ").append(this.getSignature()).append("\n");
        sb.append("\t Declaring class: ").append(declaringClass).append("\n");
        sb.append("\t Method name: ").append(methodName).append("\n");
        sb.append("\t MPI runner: ").append(mpiRunner).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Constraints: ").append(this.getConstraints());
        return sb.toString();
    }

}
