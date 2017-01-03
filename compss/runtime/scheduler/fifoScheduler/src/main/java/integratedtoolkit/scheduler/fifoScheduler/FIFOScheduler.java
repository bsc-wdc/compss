package integratedtoolkit.scheduler.fifoScheduler;

import java.util.Collection;

import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FIFOScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Scheduler that considers only ready tasks
 *
 * @param <P>
 * @param <T>
 */
public class FIFOScheduler<P extends Profile, T extends WorkerResourceDescription> extends ReadyScheduler<P, T> {

    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public FIFOScheduler() {
        super();
    }

    @Override
    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new FIFOResourceScheduler<P, T>(w);
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T> action) {
        return new FIFOScore(action.getPriority(), 0, 0, 0);
    }

    @Override
    protected Score generateFullScore(AllocatableAction<P, T> action, ResourceScheduler<P, T> resource, Score actionScore) {
        return new FIFOScore(actionScore.getActionScore(), 0, 0, 1.0 / (double) action.getId());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ResourceScheduler<P, T>[] getWorkers() {
        synchronized (workers) {
            Collection<ResourceScheduler<P, T>> resScheds = workers.values();
            FIFOResourceScheduler<P, T>[] scheds = new FIFOResourceScheduler[resScheds.size()];
            workers.values().toArray(scheds);
            return scheds;
        }
    }

}
