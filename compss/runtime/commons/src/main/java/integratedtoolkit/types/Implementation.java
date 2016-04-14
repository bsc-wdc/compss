package integratedtoolkit.types;

import integratedtoolkit.types.resources.ResourceDescription;


public abstract class Implementation <T extends ResourceDescription> {

    public enum Type {
        METHOD,
        SERVICE
    }

    protected final int coreId;
    protected final int implementationId;
    protected T requirements;

    public Implementation(int coreId, int implementationId, T annot) {
        this.coreId = coreId;
        this.implementationId = implementationId;
        this.requirements = annot;
    }

    public int getCoreId() {
        return coreId;
    }

    public int getImplementationId() {
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
