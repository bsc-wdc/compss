package integratedtoolkit.scheduler.readyScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.ObjectValue;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ActionSet;

import java.util.LinkedList;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of a Scheduler that considers only ready tasks
 *
 */
public abstract class ReadyScheduler extends TaskScheduler {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    protected final ActionSet unassignedReadyActions;

    /**
     * Constructs a new Ready Scheduler instance
     *
     */
    public ReadyScheduler() {
        super();
        this.unassignedReadyActions = new ActionSet();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> void workerLoadUpdate(ResourceScheduler<T> resource) {

    }

    @Override
    public abstract Score generateActionScore(AllocatableAction action);

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
        if (!action.hasDataPredecessors()) {
            try {
                action.schedule(actionScore);
            } catch (UnassignedActionException ex) {
                this.unassignedReadyActions.addAction(action);
            }
        }
    }

    protected <T extends WorkerResourceDescription> void scheduleAction(AllocatableAction action, ResourceScheduler<T> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {
        if (!action.hasDataPredecessors()) {
            action.schedule(targetWorker, actionScore);
        }
    }

    @Override
    public LinkedList<AllocatableAction> getUnassignedActions() {
        return unassignedReadyActions.getAllActions();
    }

    @Override
    public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
            LinkedList<AllocatableAction> dataFreeActions,
            LinkedList<AllocatableAction> resourceFreeActions,
            LinkedList<AllocatableAction> blockedCandidates,
            ResourceScheduler<T> resource) {
        purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
        tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
    }

    protected abstract <T extends WorkerResourceDescription> void purgeFreeActions(LinkedList<AllocatableAction> dataFreeActions,
            LinkedList<AllocatableAction> resourceFreeActions,
            LinkedList<AllocatableAction> blockedCandidates,
            ResourceScheduler<T> resource);

    private <T extends WorkerResourceDescription> void tryToLaunchFreeActions(LinkedList<AllocatableAction> dataFreeActions, LinkedList<AllocatableAction> resourceFreeActions, LinkedList<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
        // Try to launch all the data free actions and the resource free actions
        PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
        for (AllocatableAction freeAction : dataFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, fullScore);
            executableActions.add(obj);
        }
        for (AllocatableAction freeAction : resourceFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, fullScore);
            if (!executableActions.contains(obj)) {
                executableActions.add(obj);
            }
        }

        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();

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
