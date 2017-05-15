package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.types.resources.WorkerResourceDescription;


public abstract class Implementation<T extends WorkerResourceDescription> implements Externalizable {

    public enum TaskType {
        METHOD, // Generic method type
        SERVICE // Services type
    }


    protected Integer coreId;
    protected Integer implementationId;
    protected T requirements;


    public Implementation() {
        // For externalizable
    }

    public Implementation(Integer coreId, Integer implementationId, T annot) {
        this.coreId = coreId;
        this.implementationId = implementationId;
        this.requirements = annot;
    }

    public Integer getCoreId() {
        return this.coreId;
    }

    public Integer getImplementationId() {
        return this.implementationId;
    }

    public T getRequirements() {
        return this.requirements;
    }

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
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(this.coreId);
        out.writeObject(this.implementationId);
    }

}
