package integratedtoolkit.scheduler.dataScheduler;

import java.util.LinkedList;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.DataScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in data locality
 * 
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class DataScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyScheduler<P, T, I> {

    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public DataScheduler() {
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
        // LOGGER.debug("[DataScheduler] Generate scheduler for resource " + w.getName());
        return new DataResourceScheduler<P, T, I>(w);
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T, I> action) {
        // LOGGER.debug("[DataScheduler] Generate Action Score for " + action);
        return new DataScore(action.getPriority(), 0, 0, 0);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    
    @Override
    protected void purgeFreeActions(LinkedList<AllocatableAction<P, T, I>> dataFreeActions,
            LinkedList<AllocatableAction<P, T, I>> resourceFreeActions, LinkedList<AllocatableAction<P, T, I>> blockedCandidates,
            ResourceScheduler<P, T, I> resource){
        LOGGER.debug("[DataScheduler] Treating dependency free actions");
        
        LinkedList<AllocatableAction<P, T, I>> unassignedReadyActions = getUnassignedActions();
        this.unassignedReadyActions.removeAllActions();
        dataFreeActions.addAll(unassignedReadyActions);
        
    }

}
