package integratedtoolkit.scheduler.fullGraphScheduler;

import java.util.Collection;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FullGraphScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Implementation of a Scheduler that handles the full task graph
 *
 * @param <P>
 * @param <T>
 */
public class FullGraphScheduler<P extends Profile, T extends WorkerResourceDescription> extends TaskScheduler<P, T> {

    private final FullGraphScore<P, T> dummyScore = new FullGraphScore<>(0, 0, 0, 0, 0);
    private final ScheduleOptimizer<P, T> optimizer = new ScheduleOptimizer<>(this);


    /**
     * Constructs a new scheduler.
     * 
     * scheduleAction(Action action) Behaves as the basic Task Scheduler, as tasks arrive their executions are scheduled
     * into a worker node
     */
    public FullGraphScheduler() {
        super();
        optimizer.start();
    }

    @Override
    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new FullGraphResourceScheduler<>(w);
    }

    @Override
    public SchedulingInformation<P, T> generateSchedulingInformation() {
        return new FullGraphSchedulingInformation<>();
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T> action) {
        long actionScore = FullGraphScore.getActionScore(action);
        long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
        return new FullGraphScore<P, T>(actionScore, dataTime, 0, 0, 0);
    }

    @Override
    public void shutdown() {
        try {
            optimizer.shutdown();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }
    
    @SuppressWarnings("unchecked")
    @Override
    public ResourceScheduler<P, T>[] getWorkers() {
        synchronized (workers) {
            Collection<ResourceScheduler<P, T>> resScheds = workers.values();
            FullGraphResourceScheduler<P, T>[] scheds = new FullGraphResourceScheduler[resScheds.size()];
            workers.values().toArray(scheds);
            return scheds;
        }
    }

}
