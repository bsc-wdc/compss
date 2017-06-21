package integratedtoolkit.types.resources.updates;

import integratedtoolkit.types.resources.ResourceDescription;

public abstract class ResourceUpdate< T extends ResourceDescription> {

    public static enum Type {

        INCREASE,
        REDUCE
    }
    private final T modification;

    protected ResourceUpdate(T modification) {
        this.modification = modification;
    }

    public abstract Type getType();

    public final T getModification() {
        return modification;
    }

    public abstract boolean checkCompleted();

    public abstract void waitForCompletion() throws InterruptedException;

}
