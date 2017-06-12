package integratedtoolkit.scheduler.readyScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.ObjectValue;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ActionSet;

import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Representation of a Scheduler that considers only ready tasks
 *
 * @param <P>
 * @param <T>
 * @param <I>
 */
public abstract class ReadyScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends TaskScheduler<P, T, I> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    protected final ActionSet<P, T, I> dependingActions;
    protected final ActionSet<P, T, I> unassignedReadyActions;


    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public ReadyScheduler() {
        super();

        this.dependingActions = new ActionSet<>();
        this.unassignedReadyActions = new ActionSet<>();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    @Override
    public void workerLoadUpdate(ResourceScheduler<P, T, I> resource) {
        // LOGGER.debug("[ReadyScheduler] WorkerLoad update on resource " + resource.getName());
    }

    @Override
    public abstract Score generateActionScore(AllocatableAction<P, T, I> action);

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    protected void scheduleAction(AllocatableAction<P, T, I> action, Score actionScore) throws BlockedActionException {
        if (!action.hasDataPredecessors()) {
            try {
                action.schedule(actionScore);
            } catch (UnassignedActionException ex) {
                this.unassignedReadyActions.addAction(action);
            }
        }
    }

    protected void scheduleAction(AllocatableAction<P, T, I> action, ResourceScheduler<P, T, I> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

        if (!action.hasDataPredecessors()) {
            action.schedule(targetWorker, actionScore);
        }
    }
    
    protected abstract void purgeFreeActions(LinkedList<AllocatableAction<P, T, I>> dataFreeActions,
            LinkedList<AllocatableAction<P, T, I>> resourceFreeActions, LinkedList<AllocatableAction<P, T, I>> blockedCandidates,
            ResourceScheduler<P, T, I> resource);

    @Override
    public void handleDependencyFreeActions(LinkedList<AllocatableAction<P, T, I>> dataFreeActions,
            LinkedList<AllocatableAction<P, T, I>> resourceFreeActions, LinkedList<AllocatableAction<P, T, I>> blockedCandidates,
            ResourceScheduler<P, T, I> resource){
        
        purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
        
        tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
    }

    @Override
    public LinkedList<AllocatableAction<P, T, I>> getUnassignedActions() {
        return unassignedReadyActions.getAllActions();
    }
    
    protected void tryToLaunchFreeActions(LinkedList<AllocatableAction<P, T, I>> dataFreeActions,
            LinkedList<AllocatableAction<P, T, I>> resourceFreeActions, LinkedList<AllocatableAction<P, T, I>> blockedCandidates,
            ResourceScheduler<P, T, I> resource) {

        // Try to launch all the data free actions and the resource free actions
        PriorityQueue<ObjectValue<AllocatableAction<P, T, I>>> executableActions = new PriorityQueue<>();
        for (AllocatableAction<P, T, I> freeAction : dataFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(freeAction, fullScore);
            executableActions.add(obj);
        }
        for (AllocatableAction<P, T, I> freeAction : resourceFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(freeAction, fullScore);
            if (!executableActions.contains(obj)) {
                executableActions.add(obj);
            }
        }

        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction<P, T, I>> obj = executableActions.poll();
            AllocatableAction<P, T, I> freeAction = obj.getObject();

            // LOGGER.debug("Trying to launch action " + freeAction);
            try {
                scheduleAction(freeAction, obj.getScore());
                tryToLaunch(freeAction);
            } catch (BlockedActionException e) {
                removeFromReady(freeAction);
                addToBlocked(freeAction);
            }
        }
    }

}
