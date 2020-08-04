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

import es.bsc.compss.types.resources.ContainerDescription;
import es.bsc.compss.types.resources.MethodResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public class ContainerImplementation extends AbstractMethodImplementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    public static final int NUM_PARAMS = 7;
    public static final String SIGNATURE = "container.CONTAINER";

    private String internalType;
    private String internalBinary;
    private String internalFunc;

    private String workingDir;
    private boolean failByEV;

    private ContainerDescription container;


    /**
     * Creates a new ContainerImplementation instance for serialization.
     */
    public ContainerImplementation() {
        // For externalizable
        super();
    }

    /**
     * Creates a new ContainerImplementation from the given parameters.
     * 
     * @param internalType "PYTHON"/"BINARY"
     * @param internalFunc Python function path.
     * @param internalBinary Binary path.
     * @param workingDir Working directory.
     * @param failByEV Flag to enable failure with EV.
     * @param container Container Description.
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param signature Binary signature.
     * @param annot Binary requirements.
     */

    public ContainerImplementation(String internalType, String internalFunc, String internalBinary, String workingDir,
        boolean failByEV, ContainerDescription container, Integer coreId, Integer implementationId, String signature,
        MethodResourceDescription annot) {

        super(coreId, implementationId, signature, annot);

        this.internalType = internalType;
        this.internalBinary = internalBinary;
        this.internalFunc = internalFunc;

        this.workingDir = workingDir;
        this.failByEV = failByEV;

        this.container = container;
    }

    /**
     * Returns the internal type.
     * 
     * @return The internal type.
     */
    public String getType() {
        return this.internalType;
    }

    /**
     * Returns the python function path.
     * 
     * @return The python function path.
     */
    public String getFunction() {
        return this.internalFunc;
    }

    /**
     * Returns the binary path.
     * 
     * @return The binary path.
     */
    public String getBinary() {
        return this.internalBinary;
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
    public ContainerDescription getContainer() {
        return container;
    }

    @Override
    public MethodType getMethodType() {
        return MethodType.CONTAINER;
    }

    @Override
    public String getMethodDefinition() {
        StringBuilder sb = new StringBuilder();
        sb.append("[CONTAINER=").append(container);
        sb.append("]");

        return sb.toString();
    }

    @Override
    public String toString() {
        return "ContainerImplementation [container=" + container + ", binary=" + internalBinary + "]";
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        super.readExternal(in);

        this.internalType = (String) in.readObject();
        this.internalFunc = (String) in.readObject();
        this.internalBinary = (String) in.readObject();

        this.workingDir = (String) in.readObject();
        this.failByEV = in.readBoolean();

        this.container = (ContainerDescription) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        super.writeExternal(out);

        out.writeObject(this.internalType);
        out.writeObject(this.internalFunc);
        out.writeObject(this.internalBinary);

        out.writeObject(this.workingDir);
        out.writeBoolean(this.failByEV);

        out.writeObject(this.container);
    }

}
