package integratedtoolkit.scheduler.readyscheduler;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.ObjectValue;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ActionSet;
import integratedtoolkit.util.CoreManager;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

public class ReadyScheduler<P extends Profile, T extends WorkerResourceDescription> extends TaskScheduler<P, T> {

    private static final int THRESHOLD = 50;
    private ActionSet<P, T> dependingActions = new ActionSet<P, T>();
    private ActionSet<P, T> unassignedReadyActions = new ActionSet<P, T>();

    @Override
    protected final void scheduleAction(AllocatableAction<P, T> action, Score actionScore) throws BlockedActionException {
        if (action.hasDataPredecessors()) {
            dependingActions.addAction(action);
        } else {
            try {
                action.schedule(actionScore);
            } catch (UnassignedActionException ex) {
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
                action.schedule(action.getConstrainingPredecessor().getAssignedResource(), actionScore);
                try {
                    action.tryToLaunch();
                } catch (InvalidSchedulingException ise2) {
                    //Impossible exception. 
                }
            }
        } catch (UnassignedActionException ex) {
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

    @Override
    public void workerLoadUpdate(ResourceScheduler<P, T> resource) {
        Worker worker = resource.getResource();
        // Resource capabilities had already been taken into account when
        // assigning the actions. No need to change the assignation.
        PriorityQueue<ObjectValue<AllocatableAction<P, T>>>[] actions = new PriorityQueue[CoreManager.getCoreCount()];

        //Selecting runnable actions and priorizing them
        LinkedList<Integer> runnableCores = new LinkedList<Integer>();
        LinkedList<Implementation<T>>[] fittingImpls = new LinkedList[CoreManager.getCoreCount()];
        for (int coreId : (LinkedList<Integer>) worker.getExecutableCores()) {
            fittingImpls[coreId] = worker.getRunnableImplementations(coreId);
            if (!fittingImpls[coreId].isEmpty() && unassignedReadyActions.getActionCounts()[coreId] > 0) {
                runnableCores.add(coreId);
                actions[coreId] = sortActionsForResource(unassignedReadyActions.getActions(coreId), resource);
            }
        }

        while (!runnableCores.isEmpty()) {
            //Pick Best Action
            Integer bestCore = null;
            Score bestScore = null;
            for (Integer i : runnableCores) {
                Score coreScore = actions[i].peek().value;
                if (Score.isBetter(coreScore, bestScore)) {
                    bestScore = coreScore;
                    bestCore = i;
                }
            }
            ObjectValue<AllocatableAction<P, T>> ov = actions[bestCore].poll();
            AllocatableAction<P, T> selectedAction = ov.o;

            //Get the best Implementation
            try {
                Score actionScore = getActionScore(selectedAction);
                selectedAction.schedule(resource, actionScore);
                try {
                    selectedAction.tryToLaunch();
                } catch (InvalidSchedulingException ise) {
                    selectedAction.schedule(selectedAction.getConstrainingPredecessor().getAssignedResource(), actionScore);
                    try {
                        selectedAction.tryToLaunch();
                    } catch (InvalidSchedulingException ise2) {
                        //Impossible exception. 
                    }
                }
            } catch (UnassignedActionException uae) {
                //Action stays unassigned and ready
                continue;
            } catch (BlockedActionException bae) {
                //Never happens!
                continue;
            }
            //Task was assigned to the resource.
            //Remove from pending task sets
            unassignedReadyActions.removeAction(selectedAction);

            //Update Runnable Cores
            Iterator<Integer> coreIter = runnableCores.iterator();
            while (coreIter.hasNext()) {
                int coreId = coreIter.next();
                Iterator<Implementation<T>> implIter = fittingImpls[coreId].iterator();
                while (implIter.hasNext()) {
                    Implementation<?> impl = implIter.next();
                    if (!worker.canRunNow(impl.getRequirements())) {
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

    private PriorityQueue<ObjectValue<AllocatableAction<P, T>>> sortActionsForResource(LinkedList<AllocatableAction<P, T>> actions, ResourceScheduler<P, T> resource) {
        PriorityQueue<ObjectValue<AllocatableAction<P, T>>> pq = new PriorityQueue<ObjectValue<AllocatableAction<P, T>>>();
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
