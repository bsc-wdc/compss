package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.types.DefaultScore;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

public class DefaultScheduler<P extends Profile, T extends WorkerResourceDescription> extends TaskScheduler<P, T> {

    private final DefaultScore<P, T> dummyScore = new DefaultScore<P, T>(0, 0, 0, 0);
    private final ScheduleOptimizer optimizer = new ScheduleOptimizer(this);

    /*
     * scheduleAction(Action action)
     * Behaves as the basic Task Scheduler, as tasks arrive their executions are
     * scheduled into a worker node
     * 
     */
    public DefaultScheduler() {
        optimizer.start();
    }

    @Override
    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new DefaultResourceScheduler<P, T>(w);
    }

    @Override
    public SchedulingInformation<P, T> generateSchedulingInformation() {
        return new DefaultSchedulingInformation<P, T>();
    }

    @Override
    public Score getActionScore(AllocatableAction<P, T> action) {
        long actionScore = DefaultScore.getActionScore(action);
        long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
        return new DefaultScore<P, T>(actionScore, dataTime, 0, 0);
    }

    public void shutdown() {
        try {
            optimizer.shutdown();
        } catch (InterruptedException ie) {
            //No need to do anything.
        }
    }

}
