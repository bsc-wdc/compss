package integratedtoolkit.scheduler.types;

import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public interface ActionOrchestrator<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    public void actionCompletion(AllocatableAction<P, T, I> action);

    public void actionError(AllocatableAction<P, T, I> action);

}