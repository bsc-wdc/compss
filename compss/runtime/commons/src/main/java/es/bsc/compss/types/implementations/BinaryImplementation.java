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

import es.bsc.compss.types.resources.BinaryContainerDescription;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class BinaryImplementation extends AbstractMethodImplementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 5;
    public static final String SIGNATURE = "binary.BINARY";

    private String binary;
    private String workingDir;
    private boolean failByEV;
    private BinaryContainerDescription container;


    /**
     * Creates a new BinaryImplementation instance for serialization.
     */
    public BinaryImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new BinaryImplementation from the given parameters.
     * 
     * @param binary Binary path.
     * @param workingDir Working directory.
     * @param failByEV Flag to enable failure with EV.
     * @param container Container Description.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param signature Binary signature.
     * @param annot Binary requirements.
     */
    public BinaryImplementation(String binary, String workingDir, boolean failByEV, BinaryContainerDescription container,Integer coreId,
        Integer implementationId, String signature, MethodResourceDescription annot) {

        super(coreId, implementationId, signature, annot);

        this.binary = binary;
        this.workingDir = workingDir;
        this.failByEV = failByEV;
        this.container = container;
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
     * Check if fail by exit value is enabled.
     * 
     * @return True is fail by exit value is enabled.
     */
    public boolean isFailByEV() {
        return failByEV;
    }
    
	/**
     * Returns the container.
     * 
     * @return The container implementation.
     */
    public BinaryContainerDescription getContainer() {
        return container;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.BINARY;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[BINARY=").append(binary);
        sb.append(",IO=").append(this.isIO());
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return super.toString() + " Binary Method with binary " + this.binary;
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);
        this.binary = (String) in.readObject();
        this.workingDir = (String) in.readObject();
        this.failByEV = in.readBoolean();
        this.container = (BinaryContainerDescription) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);
        out.writeObject(this.binary);
        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);
        out.writeObject(this.container);
    }

}
