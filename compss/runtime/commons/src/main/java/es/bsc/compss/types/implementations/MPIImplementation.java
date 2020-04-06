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
package es.bsc.compss.types.implementations;

import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class MPIImplementation extends AbstractMethodImplementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 6;

    private String mpiRunner;
    private String mpiFlags;
    private String binary;
    private String workingDir;
    private boolean scaleByCU;
    private boolean failByEV;


    /**
     * Creates a new MPIImplementation for serialization.
     */
    public MPIImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new MPIImplementation instance from the given parameters.
     * 
     * @param binary MPI binary path.
     * @param workingDir Binary working directory.
     * @param mpiRunner Path to the MPI command.
     * @param scaleByCU Scale by computing units property.
     * @param failByEV Flag to enable failure with EV.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param signature MPI method signature.
     * @param annot MPI requirements.
     */
    public MPIImplementation(String binary, String workingDir, String mpiRunner, String mpiFlags, boolean scaleByCU,
        boolean failByEV, Integer coreId, Integer implementationId, String signature, MethodResourceDescription annot) {

        super(coreId, implementationId, signature, annot);

        this.mpiRunner = mpiRunner;
        this.mpiFlags = mpiFlags;
        this.workingDir = workingDir;
        this.binary = binary;
        this.scaleByCU = scaleByCU;
        this.failByEV = failByEV;
    }

    /**
     * Returns the binary path.
     * 
     * @return The binary path.
     */
    public String getBinary() {
        return this.binary;
    }

    /**
     * Returns the binary working directory.
     * 
     * @return The binary working directory.
     */
    public String getWorkingDir() {
        return this.workingDir;
    }

    /**
     * Returns the path to the MPI command.
     * 
     * @return The path to the MPI command.
     */
    public String getMpiRunner() {
        return this.mpiRunner;
    }

    /**
     * Returns the flags for the MPI command.
     * 
     * @return Flags for the MPI command.
     */
    public String getMpiFlags() {
        return this.mpiFlags;
    }

    /**
     * Returns the scale by computing units property.
     * 
     * @return scale by computing units property value.
     */
    public boolean getScaleByCU() {
        return this.scaleByCU;
    }

    /**
     * Check if fail by exit value is enabled.
     * 
     * @return True is fail by exit value is enabled.
     */
    public boolean isFailByEV() {
        return failByEV;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.MPI;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[MPI RUNNER=").append(this.mpiRunner);
        sb.append(", MPI_FLAGS=").append(this.mpiFlags);
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
        this.mpiFlags = (String) in.readObject();
        this.binary = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.scaleByCU = in.readBoolean();
        this.failByEV = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.mpiRunner);
        out.writeObject(this.mpiFlags);
        out.writeObject(this.binary);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.scaleByCU);
        out.writeBoolean(this.failByEV);
    }

}
