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

import es.bsc.compss.types.implementations.definition.ImplementationDefinition;
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
    protected boolean localProcessing;
    protected boolean io;
    protected ImplementationDescription<? extends WorkerResourceDescription,
        ? extends ImplementationDefinition> implDescription;


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
     * @param implDesc Implementation Description
     */
    public Implementation(Integer coreId, Integer implementationId,
        ImplementationDescription<? extends WorkerResourceDescription, ? extends ImplementationDefinition> implDesc) {
        this.coreId = coreId;
        this.implementationId = implementationId;
        this.implDescription = implDesc;
        if (implDesc.getConstraints() != null) {
            this.io = !implDesc.getConstraints().usesCPUs();
        } else {
            this.io = false;
        }
        this.localProcessing = implDesc.isLocal();
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
     * Returns whether the implementation is IO or not.
     *
     * @return true if the implementation is IO, false otherwise.
     */
    public boolean isIO() {
        return this.io;
    }

    /**
     * Returns whether the implementation is to be run in local or not.
     *
     * @return {@literal true}, if the implementation is IO; {@literal false} otherwise.
     */
    public boolean isLocalProcessing() {
        return localProcessing;
    }

    /**
     * Returns an ImplementationDefinition describing the implementation.
     *
     * @return description of the implementation.
     */
    public ImplementationDescription<? extends WorkerResourceDescription, ? extends ImplementationDefinition>
        getDescription() {
        return implDescription;
    }

    public String toJSON() {
        return "{" + "\"core_id\":" + this.coreId + ",\"impl_id\":" + this.implementationId + ",\"description\":"
            + (this.implDescription != null ? this.implDescription.toJSON() : "null") + "}";
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Implementation ").append(this.implementationId);
        sb.append(" for core ").append(this.coreId);
        sb.append(":").append(this.implDescription);
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        this.coreId = (Integer) in.readObject();
        this.implementationId = (Integer) in.readObject();
        this.implDescription = (ImplementationDescription<? extends WorkerResourceDescription,
            ? extends ImplementationDefinition>) in.readObject();
        this.io = (boolean) in.readObject();
        this.localProcessing = in.readBoolean();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.coreId);
        out.writeObject(this.implementationId);
        out.writeObject(this.implDescription);
        out.writeObject(this.io);
        out.writeBoolean(this.localProcessing);
    }

    public abstract TaskType getTaskType();

    public abstract WorkerResourceDescription getRequirements();

    public String getSignature() {
        return this.implDescription.getSignature();
    }

}
