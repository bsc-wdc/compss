package integratedtoolkit.scheduler.fifoDataScheduler;

import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FIFODataScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Scheduler that considers only ready tasks
 *
 * @param <P>
 * @param <T>
 */
public class FIFODataScheduler<P extends Profile, T extends WorkerResourceDescription> extends ReadyScheduler<P, T> {

    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public FIFODataScheduler() {
        super();
    }

    @Override
    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new FIFODataResourceScheduler<P, T>(w);
    }
    

    @Override
    public void actionCompleted(AllocatableAction<P, T> action) {
        ResourceScheduler<P, T> resource = action.getAssignedResource();
        if (action.getImplementations().length > 0) {
            Integer coreId = action.getImplementations()[0].getCoreId();
            if (coreId != null) {
                readyCounts[coreId]--;
            }
        }
        LinkedList<AllocatableAction<P, T>> dataFreeActions = action.completed();
        for (AllocatableAction<P, T> dataFreeAction : dataFreeActions) {
            if (dataFreeAction != null && dataFreeAction.isNotScheduling()) {
                if (dataFreeAction.getImplementations().length > 0) {
                    Integer coreId = dataFreeAction.getImplementations()[0].getCoreId();
                    if (coreId != null) {
                        readyCounts[coreId]++;
                    }
                }

                try {
                    dependencyFreeAction(dataFreeAction);
                } catch (BlockedActionException bae) {
                    if (!dataFreeAction.isLocked() && !dataFreeAction.isRunning()) {
                        logger.info("Blocked Action: " + dataFreeAction);
                        blockedActions.addAction(dataFreeAction);
                    }
                }
            }
        }

        LinkedList<AllocatableAction<P, T>> resourceFree = resource.unscheduleAction(action);
        workerLoadUpdate((ResourceScheduler<P, T>) action.getAssignedResource());
        HashSet<AllocatableAction<P, T>> freeTasks = new HashSet<>();
        freeTasks.addAll(dataFreeActions);
        freeTasks.addAll(resourceFree);
        for (AllocatableAction<P, T> a : freeTasks) {
            if (a != null && !a.isLocked() && !a.isRunning()) {
                try {
                    try {
                        a.tryToLaunch();
                    } catch (InvalidSchedulingException ise) {
                        Score aScore = generateActionScore(a);
                        boolean keepTrying = true;
                        for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                            AllocatableAction<P, T> pre = action.getConstrainingPredecessors().get(i);
                            action.schedule(pre.getAssignedResource(), aScore);
                            try {
                                action.tryToLaunch();
                                keepTrying = false;
                            } catch (InvalidSchedulingException ise2) {
                                // Try next predecessor
                                keepTrying = true;
                            }
                        }
                    }

                } catch (UnassignedActionException ure) {
                    StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
                    info.append(action.toString());
                    ErrorManager.fatal(info.toString());
                } catch (BlockedActionException bae) {
                    if (a != null && !a.isLocked() && !a.isRunning()) {
                        logger.info("Blocked Action: " + a, bae);
                        blockedActions.addAction(a);
                    }
                }
            }
        }

    }
    

    @Override
    public void dependencyFreeAction(AllocatableAction<P, T> action) throws BlockedActionException {
        dependingActions.removeAction(action);
        try {
            Score actionScore = generateActionScore(action);
            action.schedule(actionScore);
            try {
                action.tryToLaunch();
            } catch (InvalidSchedulingException ise) {
                boolean keepTrying = true;
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                    AllocatableAction<P, T> pre = action.getConstrainingPredecessors().get(i);
                    action.schedule(pre.getAssignedResource(), actionScore);
                    try {
                        action.tryToLaunch();
                        keepTrying = false;
                    } catch (InvalidSchedulingException ise2) {
                        // Try next predecessor
                        keepTrying = true;
                    }
                }
            }
        } catch (UnassignedActionException ex) {
            logger.debug("Adding action " + action + " to unassigned list");
            unassignedReadyActions.addAction(action);
        }
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T> action) {
        return new FIFODataScore(action.getPriority(), 0, 0, 0);
    }

    @Override
    protected Score generateFullScore(AllocatableAction<P, T> action, ResourceScheduler<P, T> resource, Score actionScore) {
        return new FIFODataScore(actionScore.getActionScore(), 0, 0, 1.0 / (double) action.getId());
    }

    @SuppressWarnings("unchecked")
    @Override
    public ResourceScheduler<P, T>[] getWorkers() {
        synchronized (workers) {
            Collection<ResourceScheduler<P, T>> resScheds = workers.values();
            FIFODataResourceScheduler<P, T>[] scheds = new FIFODataResourceScheduler[resScheds.size()];
            workers.values().toArray(scheds);
            return scheds;
        }
    }

}
