package integratedtoolkit.scheduler.fullGraphScheduler;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FullGraphScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Implementation of a Scheduler that handles the full task graph
 *
 * @param <P>
 * @param <T>
 */
public class FullGraphScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends TaskScheduler<P, T, I> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    private final FullGraphScore<P, T, I> dummyScore = new FullGraphScore<>(0, 0, 0, 0, 0);
    private final ScheduleOptimizer<P, T, I> optimizer = new ScheduleOptimizer<>(this);


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
    public void shutdown() {
        try {
            optimizer.shutdown();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    @Override
    public SchedulingInformation<P, T, I> generateSchedulingInformation() {
        LOGGER.info("[FGScheduler] Generate empty scheduling information");
        return new FullGraphSchedulingInformation<>();
    }

    @Override
    public ResourceScheduler<P, T, I> generateSchedulerForResource(Worker<T, I> w) {
        LOGGER.info("[FGScheduler] Generate scheduler for resource " + w.getName());
        return new FullGraphResourceScheduler<>(w, getOrchestrator());
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T, I> action) {
        LOGGER.info("[FGScheduler] Generate Action Score for " + action);
        long actionScore = FullGraphScore.getActionScore(action);
        long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
        return new FullGraphScore<P, T, I>(actionScore, dataTime, 0, 0, 0);
    }

}
