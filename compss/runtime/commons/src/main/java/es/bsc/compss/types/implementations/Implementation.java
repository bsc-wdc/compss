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

import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;


public abstract class Implementation implements Externalizable {

    /**
     * Runtime Objects have serialization ID 1L.
     */
    private static final long serialVersionUID = 1L;

    protected Integer coreId;
    protected Integer implementationId;
    protected String signature;
    protected boolean io;
    protected WorkerResourceDescription requirements;


    /**
     * Creates a new Implementation instance for serialization.
     */
    public Implementation() {
        // For externalizable
    }

    /**
     * Creates a new Implementation instance from the given parameters.
     *
     * @param coreId Core Id.
     * @param implementationId Implementation Id.
     * @param signature Signature of the implementation.
     * @param requirements Implementation requirements.
     */
    public Implementation(Integer coreId, Integer implementationId, String signature,
        WorkerResourceDescription requirements) {

        this.coreId = coreId;
        this.implementationId = implementationId;
        this.signature = signature;
        this.requirements = requirements;
        if(requirements != null) {
        	this.io = !requirements.usesCPUs();
        }else {
        	this.io = false;
        }
    }

    /**
     * Returns the core Id.
     *
     * @return The core Id.
     */
    public Integer getCoreId() {
        return this.coreId;
    }

    /**
     * Returns the implementation Id.
     *
     * @return The implementation Id.
     */
    public Integer getImplementationId() {
        return this.implementationId;
    }

    /**
     * Returns the signature of the implemention operation.
     *
     * @return signature of the implemention operation.
     */
    public String getSignature() {
        return signature;
    }

    /**
     * Returns whether the implementation is IO or not.
     *
     * @return true if the implementation is IO, false otherwise.
     */
    public boolean isIO() {
        return this.io;
    }

    /**
     * Returns the implementation requirements.
     *
     * @return The implementation requirements.
     */
    public WorkerResourceDescription getRequirements() {
        return this.requirements;
    }

    /**
     * Returns the global task type.
     *
     * @return The global task type.
     */
    public abstract TaskType getTaskType();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Implementation ").append(this.implementationId);
        sb.append(" for core ").append(this.coreId);
        sb.append(":");
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.coreId = (Integer) in.readObject();
        this.implementationId = (Integer) in.readObject();
        this.signature = in.readUTF();
        this.io = (boolean) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.coreId);
        out.writeObject(this.implementationId);
        out.writeUTF(signature);
        out.writeObject(this.io);
    }

}
