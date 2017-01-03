package integratedtoolkit.scheduler.readyScheduler;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;


/**
 * Implementation for the ReadyResourceScheduler
 *
 * @param <P>
 * @param <T>
 */
public abstract class ReadyResourceScheduler<P extends Profile, T extends WorkerResourceDescription> extends ResourceScheduler<P, T> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public ReadyResourceScheduler(Worker<T> w) {
        super(w);
    }

    @Override
    public abstract Score generateResourceScore(AllocatableAction<P, T> action, TaskDescription params, Score actionScore);

    @Override
    public abstract Score generateWaitingScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl,
            Score resourceScore);

    @Override
    public abstract Score generateImplementationScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl,
            Score resourceScore);

    @Override
    public abstract String toString();

}
