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
    private final String mpiFlags;
    private final boolean scaleByCU;
    private final boolean failByEV;


    public class CollectionLayout {

        private String paramName;
        private int blockCount;
        private int blockLen;
        private int blockStride;
    }


    // This parameter has to be a list because if you have multiple layouts for multiple parameters
    private final CollectionLayout cl;


    protected PythonMPIDefinition(String implSignature, String declaringClass, String methodName, String workingDir,
        String mpiRunner, String mpiFlags, boolean scaleByCU, boolean failByEV,
        String paramName, int blockCount, int blockLen, int blockStride,
        MethodResourceDescription implConstraints) {

        super(implSignature, implConstraints);

        this.workingDir = workingDir;
        this.mpiRunner = mpiRunner;
        this.mpiFlags = mpiFlags;
        this.declaringClass = declaringClass;
        this.methodName = methodName;
        this.scaleByCU = scaleByCU;
        this.failByEV = failByEV;
        this.cl = new CollectionLayout();
        this.cl.paramName = paramName;
        this.cl.blockCount = blockCount;
        this.cl.blockLen = blockLen;
        this.cl.blockStride = blockStride;
    }

    @Override
    public Implementation getImpl(int coreId, int implId) {
        return new PythonMPIImplementation(declaringClass, methodName, workingDir, mpiRunner, mpiFlags, scaleByCU,
            failByEV, coreId, implId, this.cl.paramName, this.cl.blockCount, this.cl.blockLen, this.cl.blockStride,
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
        sb.append("\t MPI flags: ").append(mpiFlags).append("\n");
        sb.append("\t IO: ").append(!this.getConstraints().usesCPUs()).append("\n");
        sb.append("\t Working directory: ").append(workingDir).append("\n");
        sb.append("\t Scale by Computing Units: ").append(scaleByCU).append("\n");
        sb.append("\t Fail by EV: ").append(this.failByEV).append("\n");
        sb.append("\t Collection Layouts: ").append("\n");
        sb.append("\t\t - Parameter Name: ").append(this.cl.paramName).append("\n");
        sb.append("\t\t - Block Count: ").append(this.cl.blockCount).append("\n");
        sb.append("\t\t - Block Length: ").append(this.cl.blockLen).append("\n");
        sb.append("\t\t - Block Stride: ").append(this.cl.blockStride).append("\n");
        sb.append("\t Constraints: ").append(this.getConstraints());
        return sb.toString();
    }

}
