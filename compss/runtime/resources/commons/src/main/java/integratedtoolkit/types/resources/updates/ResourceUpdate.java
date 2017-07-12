package integratedtoolkit.types.resources.updates;

import integratedtoolkit.types.resources.ResourceDescription;


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
    
    public boolean isIncrease(){
    	return getType().equals(Type.INCREASE);
    }
    
    public boolean isReduce(){
    	return getType().equals(Type.REDUCE);
    }
    

    public abstract Type getType();

    public abstract boolean checkCompleted();

    public abstract void waitForCompletion() throws InterruptedException;

}
