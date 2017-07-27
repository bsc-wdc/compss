package es.bsc.compss.types.resources.updates;

import es.bsc.compss.types.resources.ResourceDescription;


public abstract class ResourceUpdate<T extends ResourceDescription> {

    public static enum Type {
        INCREASE, // Increasing resource capabilities
        REDUCE // Reducing resource capabilities
    }


    private final T modification;


    protected ResourceUpdate(T modification) {
        this.modification = modification;
    }

    public final T getModification() {
        return modification;
    }

    public abstract Type getType();

    public abstract boolean checkCompleted();

    public abstract void waitForCompletion() throws InterruptedException;

}
