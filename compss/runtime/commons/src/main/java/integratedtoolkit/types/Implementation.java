package integratedtoolkit.types;

import integratedtoolkit.types.resources.WorkerResourceDescription;


public abstract class Implementation <T extends WorkerResourceDescription> {

    public enum Type {
        METHOD,
        SERVICE
    }

    protected final Integer coreId;
    protected final Integer implementationId;
    protected T requirements;

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

    public abstract Type getType();

    public String toString() {
        StringBuilder sb = new StringBuilder("Implementation ").append(implementationId);
        sb.append(" for core ").append(coreId);
        sb.append(":");
        return sb.toString();
    }

}
