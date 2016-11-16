package integratedtoolkit.types.implementations;

import java.io.Externalizable;
import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.types.resources.WorkerResourceDescription;


public abstract class Implementation<T extends WorkerResourceDescription> implements Externalizable {

    public enum TaskType {
        METHOD, 
        SERVICE
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
        return coreId;
    }

    public Integer getImplementationId() {
        return implementationId;
    }

    public T getRequirements() {
        return requirements;
    }

    public abstract TaskType getTaskType();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Implementation ").append(implementationId);
        sb.append(" for core ").append(coreId);
        sb.append(":");
        return sb.toString();
    }

    @Override
    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        coreId = (Integer) in.readObject();
        implementationId = (Integer) in.readObject();
    }

    @Override
    public void writeExternal(ObjectOutput out) throws IOException {
        out.writeObject(coreId);
        out.writeObject(implementationId);
    }

}
