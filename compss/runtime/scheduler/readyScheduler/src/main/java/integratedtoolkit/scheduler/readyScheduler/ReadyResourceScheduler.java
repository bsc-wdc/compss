package integratedtoolkit.scheduler.readyScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Implementation for the ReadyResourceScheduler
 *
 * @param <P>
 * @param <T>
 * @param <I>
 */
public abstract class ReadyResourceScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ResourceScheduler<P, T, I> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public ReadyResourceScheduler(Worker<T, I> w) {
        super(w);
    }

    @Override
    public abstract Score generateBlockedScore(AllocatableAction<P, T, I> action);

    @Override
    public abstract Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore);

    @Override
    public abstract Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl,
            Score resourceScore);

    @Override
    public abstract String toString();

}
