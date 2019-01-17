/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import es.bsc.compss.types.resources.MethodResourceDescription;


public class MPIImplementation extends AbstractMethodImplementation implements Externalizable {

    public static final int NUM_PARAMS = 3;

    private String mpiRunner;
    private String binary;
    private String workingDir;


    public MPIImplementation() {
        // For externalizable
        super();
    }

    public MPIImplementation(String binary, String workingDir, String mpiRunner, Integer coreId, Integer implementationId,
            MethodResourceDescription annot) {

        super(coreId, implementationId, annot);

        this.mpiRunner = mpiRunner;
        this.workingDir = workingDir;
        this.binary = binary;
    }

    public String getBinary() {
        return this.binary;
    }

    public String getWorkingDir() {
        return this.workingDir;
    }

    public String getMpiRunner() {
        return this.mpiRunner;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MPI;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MPI RUNNER=").append(this.mpiRunner);
        sb.append(", BINARY=").append(this.binary);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " MPI Method with binary " + this.binary + " and MPIrunner " + this.mpiRunner;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.mpiRunner = (String) in.readObject();
        this.binary = (String) in.readObject();
        this.workingDir = (String) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.mpiRunner);
        out.writeObject(this.binary);
        out.writeObject(this.workingDir);
    }

}
