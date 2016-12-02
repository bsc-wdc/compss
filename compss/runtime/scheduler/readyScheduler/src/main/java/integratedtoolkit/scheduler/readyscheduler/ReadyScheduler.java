package integratedtoolkit.scheduler.readyscheduler;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.ObjectValue;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ActionSet;
import integratedtoolkit.util.CoreManager;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;


/**
 * Representation of a Scheduler that considers only ready tasks
 *
 * @param <P>
 * @param <T>
 */
public class ReadyScheduler<P extends Profile, T extends WorkerResourceDescription> extends TaskScheduler<P, T> {

    private static final int THRESHOLD = 50;

    private ActionSet<P, T> dependingActions = new ActionSet<>();
    private ActionSet<P, T> unassignedReadyActions = new ActionSet<>();


    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public ReadyScheduler() {
        super();
    }

    @Override
    protected final void scheduleAction(AllocatableAction<P, T> action, Score actionScore) throws BlockedActionException {
        if (action.hasDataPredecessors()) {
            dependingActions.addAction(action);
        } else {
            try {
                action.schedule(actionScore);
            } catch (UnassignedActionException ex) {
                logger.debug("Adding action " + action + " to unassigned list");
                unassignedReadyActions.addAction(action);
            }
        }
    }

    @Override
    public void dependencyFreeAction(AllocatableAction<P, T> action) throws BlockedActionException {
        dependingActions.removeAction(action);
        try {
            Score actionScore = getActionScore(action);
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
    protected void workerDetected(ResourceScheduler<P, T> resource) {
        // There are no internal structures worker-related. No need to do
        // anything.
    }

    @Override
    protected void workerRemoved(ResourceScheduler<P, T> resource) {
        // There are no internal structures worker-related. No need to do
        // anything.
    }

    @SuppressWarnings("unchecked")
    @Override
    public void workerLoadUpdate(ResourceScheduler<P, T> resource) {
        Worker<T> worker = resource.getResource();
        // Resource capabilities had already been taken into account when
        // assigning the actions. No need to change the assignation.
        PriorityQueue<ObjectValue<AllocatableAction<P, T>>>[] actions = new PriorityQueue[CoreManager.getCoreCount()];

        // Selecting runnable actions and priorizing them
        LinkedList<Integer> runnableCores = new LinkedList<>();
        LinkedList<Implementation<T>>[] fittingImpls = new LinkedList[CoreManager.getCoreCount()];
        for (int coreId : (LinkedList<Integer>) worker.getExecutableCores()) {
            fittingImpls[coreId] = worker.getRunnableImplementations(coreId);
            if (!fittingImpls[coreId].isEmpty() && unassignedReadyActions.getActionCounts()[coreId] > 0) {
                actions[coreId] = sortActionsForResource(unassignedReadyActions.getActions(coreId), resource);
                // check actions[coreId] is not empty
                if (!actions[coreId].isEmpty()) {
                    runnableCores.add(coreId);
                }
            }
        }

        while (!runnableCores.isEmpty()) {

            // Pick Best Action
            Integer bestCore = null;
            Score bestScore = null;
            for (Integer i : runnableCores) {
                Score coreScore = actions[i].peek().getScore();
                if (Score.isBetter(coreScore, bestScore)) {
                    bestScore = coreScore;
                    bestCore = i;
                }
            }

            ObjectValue<AllocatableAction<P, T>> ov = actions[bestCore].poll();
            AllocatableAction<P, T> selectedAction = ov.getObject();

            if (actions[bestCore].isEmpty()) {
                runnableCores.remove(bestCore);
            }
            unassignedReadyActions.removeAction(selectedAction);

            // Get the best Implementation
            try {
                Score actionScore = getActionScore(selectedAction);
                selectedAction.schedule(resource, actionScore);
                try {
                    selectedAction.tryToLaunch();
                } catch (InvalidSchedulingException ise) {
                    boolean keepTrying = true;
                    for (int i = 0; i < selectedAction.getConstrainingPredecessors().size() && keepTrying; ++i) {
                        AllocatableAction<P, T> pre = selectedAction.getConstrainingPredecessors().get(i);
                        selectedAction.schedule(pre.getAssignedResource(), actionScore);
                        try {
                            selectedAction.tryToLaunch();
                            keepTrying = false;
                        } catch (InvalidSchedulingException ise2) {
                            // Try next predecessor
                            keepTrying = true;
                        }
                    }
                    if (keepTrying) {
                        // Action couldn't be assigned
                        unassignedReadyActions.addAction(selectedAction);
                    }
                }
            } catch (UnassignedActionException uae) {
                // Action stays unassigned and ready
                unassignedReadyActions.addAction(selectedAction);
                continue;
            } catch (BlockedActionException bae) {
                // Never happens!
                unassignedReadyActions.addAction(selectedAction);
                continue;
            }

            // Update Runnable Cores
            Iterator<Integer> coreIter = runnableCores.iterator();
            while (coreIter.hasNext()) {
                int coreId = coreIter.next();
                fittingImpls[coreId] = worker.getRunnableImplementations(coreId);
                Iterator<Implementation<T>> implIter = fittingImpls[coreId].iterator();
                while (implIter.hasNext()) {
                    Implementation<?> impl = implIter.next();
                    T requirements = (T) impl.getRequirements();
                    if (!worker.canRunNow(requirements)) {
                        implIter.remove();
                    }
                }
                if (fittingImpls[coreId].isEmpty() || unassignedReadyActions.getActionCounts()[coreId] == 0) {
                    coreIter.remove();
                }
            }
        }
    }

    @Override
    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new ReadyResourceScheduler<P, T>(w);
    }

    private PriorityQueue<ObjectValue<AllocatableAction<P, T>>> sortActionsForResource(LinkedList<AllocatableAction<P, T>> actions,
            ResourceScheduler<P, T> resource) {

        PriorityQueue<ObjectValue<AllocatableAction<P, T>>> pq = new PriorityQueue<>();
        int counter = 0;
        for (AllocatableAction<P, T> action : actions) {
            Score actionScore = getActionScore(action);
            Score score = action.schedulingScore(resource, actionScore);
            if (score == null) {
                continue;
            }
            ObjectValue<AllocatableAction<P, T>> ov = new ObjectValue<AllocatableAction<P, T>>(action, score);
            pq.offer(ov);
            counter++;
            if (counter == THRESHOLD) {
                break;
            }
        }

        return pq;
    }

}
