package integratedtoolkit.scheduler.resourceEmptyScheduler;

import java.util.Collection;

import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.ResourceEmptyScore;
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
public class ResourceEmptyScheduler<P extends Profile, T extends WorkerResourceDescription> extends ReadyScheduler<P, T> {

    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public ResourceEmptyScheduler() {
        super();
    }

    @Override
    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new ResourceEmptyResourceScheduler<P, T>(w);
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T> action) {
        return new ResourceEmptyScore(action.getPriority(), 0, 0, 0);
    }

    @Override
    protected Score generateFullScore(AllocatableAction<P, T> action, ResourceScheduler<P, T> resource, Score actionScore) {
        return action.schedulingScore(resource, actionScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ResourceScheduler<P, T>[] getWorkers() {
        synchronized (workers) {
            Collection<ResourceScheduler<P, T>> resScheds = workers.values();
            ResourceEmptyResourceScheduler<P, T>[] scheds = new ResourceEmptyResourceScheduler[resScheds.size()];
            workers.values().toArray(scheds);
            return scheds;
        }
    }

}
