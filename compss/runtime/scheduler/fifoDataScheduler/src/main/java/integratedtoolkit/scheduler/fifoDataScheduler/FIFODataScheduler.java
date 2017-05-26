package integratedtoolkit.scheduler.fifoDataScheduler;

import java.util.LinkedList;
import java.util.PriorityQueue;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FIFODataScore;
import integratedtoolkit.scheduler.types.ObjectValue;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode + data locality
 * 
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class FIFODataScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyScheduler<P, T, I> {

    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public FIFODataScheduler() {
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
        // LOGGER.debug("[FIFODataScheduler] Generate scheduler for resource " + w.getName());
        return new FIFODataResourceScheduler<P, T, I>(w);
    }

    @Override
    public FIFODataScore generateActionScore(AllocatableAction<P, T, I> action) {
        // LOGGER.debug("[FIFODataScheduler] Generate Action Score for " + action);
        return new FIFODataScore(action.getPriority(), 0, 0, 0);
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
        
        PriorityQueue<ObjectValue<AllocatableAction<P, T, I>>> executableActions = new PriorityQueue<>();
        for (AllocatableAction<P, T, I> action : dataFreeActions) {
            FIFODataScore actionScore = this.generateActionScore(action);
            FIFODataScore fullScore = (FIFODataScore) action.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(action, fullScore);
            executableActions.add(obj);
        }
        dataFreeActions.clear();
        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction<P, T, I>> obj = executableActions.poll();
            AllocatableAction<P, T, I> freeAction = obj.getObject();
            try {
                scheduleAction(freeAction, resource, obj.getScore());
                tryToLaunch(freeAction);
            } catch (BlockedActionException e) {
                removeFromReady(freeAction);
                addToBlocked(freeAction);
            } catch (UnassignedActionException e) {
                dataFreeActions.add(freeAction);
            }
        }
        LinkedList<AllocatableAction<P, T, I>> unassignedReadyActions = getUnassignedActions();
        this.unassignedReadyActions.removeAllActions();
        dataFreeActions.addAll(unassignedReadyActions);
        
    }

}
