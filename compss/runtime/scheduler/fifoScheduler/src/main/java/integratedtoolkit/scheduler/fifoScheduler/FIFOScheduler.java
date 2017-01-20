package integratedtoolkit.scheduler.fifoScheduler;

import java.util.LinkedList;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FIFOScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode
 * 
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class FIFOScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyScheduler<P, T, I> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);


    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public FIFOScheduler() {
        super();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    @Override
    public ResourceScheduler<P, T, I> generateSchedulerForResource(Worker<T, I> w) {
        LOGGER.info("[FIFOScheduler] Generate scheduler for resource " + w.getName());
        return new FIFOResourceScheduler<P, T, I>(w);
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T, I> action) {
        LOGGER.info("[FIFOScheduler] Generate Action Score for " + action);
        return new FIFOScore(action.getPriority(), -(double) action.getId(), 0, 0);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    @Override
    public void handleDependencyFreeActions(LinkedList<AllocatableAction<P, T, I>> executionCandidates,
            LinkedList<AllocatableAction<P, T, I>> unassignedCandidates, LinkedList<AllocatableAction<P, T, I>> blockedCandidates) {

        LOGGER.info("[FIFOScheduler] Treating dependency free actions");

        for (AllocatableAction<P, T, I> action : executionCandidates) {
            this.dependingActions.removeAction(action);
            this.unassignedReadyActions.addAction(action);
        }

        // We leave on executionCandidates empty since none of the actions is ready to be launched
        executionCandidates.clear();
    }

}
