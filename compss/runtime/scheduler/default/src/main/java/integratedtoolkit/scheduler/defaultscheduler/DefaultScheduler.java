package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.types.DefaultScore;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.resources.Worker;


public class DefaultScheduler extends TaskScheduler {

    private final SchedulingOptimizer optimizer = new SchedulingOptimizer(this);
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
    public ResourceScheduler<?> generateSchedulerForResource(Worker<?> w) {
        return new DefaultResourceScheduler(w);
    }

    @Override
    public SchedulingInformation generateSchedulingInformation() {
        return new DefaultSchedulingInformation();
    }

    @Override
    public Score getActionScore(AllocatableAction action, TaskParams params) {
        long actionScore = DefaultScore.getActionScore(params);
        long dataTime = DefaultScore.getDataPredecessorTime(action.getDataPredecessors());
        return new DefaultScore(actionScore, dataTime, 0, 0);
    }

    public void shutdown() {
        try {
            optimizer.shutdown();
        } catch (InterruptedException ie) {
            //No need to do anything.
        }
    }

}
