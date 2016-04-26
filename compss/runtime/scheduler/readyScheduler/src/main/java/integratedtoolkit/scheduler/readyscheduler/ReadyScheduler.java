package integratedtoolkit.scheduler.readyscheduler;

import integratedtoolkit.components.impl.TaskScheduler;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.AllocatableAction.BlockedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction.InvalidSchedulingException;
import integratedtoolkit.scheduler.types.AllocatableAction.UnassignedActionException;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.ObjectValue;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.ActionSet;
import integratedtoolkit.util.CoreManager;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

public class ReadyScheduler extends TaskScheduler {

    private static final int THRESHOLD = 50;
    private ActionSet dependingActions = new ActionSet();
    private ActionSet unassignedReadyActions = new ActionSet();

    @Override
    protected final void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
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
    public void dependencyFreeAction(AllocatableAction action) throws BlockedActionException {
        dependingActions.removeAction(action);
        try {
            Score actionScore = action.schedulingScore(this);
            action.schedule(actionScore);
        } catch (UnassignedActionException ex) {
            unassignedReadyActions.addAction(action);
        }
    }

    @Override
    protected void workerDetected(ResourceScheduler resource) {
        // There are no internal structures worker-related. No need to do 
        // anything.
    }

    @Override
    protected void workerRemoved(ResourceScheduler resource) {
        // There are no internal structures worker-related. No need to do 
        // anything.
    }

    @Override
    public void workerUpdate(ResourceScheduler resource) {
        Worker worker = resource.getResource();
        // Resource capabilities had already been taken into account when
        // assigning the actions. No need to change the assignation.
        PriorityQueue<ObjectValue<AllocatableAction>>[] actions = new PriorityQueue[CoreManager.getCoreCount()];

        //Selecting runnable actions and priorizing them
        LinkedList<Integer> runnableCores = new LinkedList<Integer>();
        LinkedList<Implementation>[] fittingImpls = new LinkedList[CoreManager.getCoreCount()];
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
            ObjectValue<AllocatableAction> ov = actions[bestCore].poll();
            AllocatableAction selectedAction = ov.o;

            //Get the best Implementation
            try {
                Score actionScore = selectedAction.schedulingScore(this);
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
                Iterator<Implementation> implIter = fittingImpls[coreId].iterator();
                while (implIter.hasNext()) {
                    Implementation impl = implIter.next();
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
    public ResourceScheduler generateSchedulerForResource(Worker w) {
        return new ReadyResourceScheduler(w);
    }

    private PriorityQueue<ObjectValue<AllocatableAction>> sortActionsForResource(LinkedList<AllocatableAction> actions, ResourceScheduler resource) {
        PriorityQueue<ObjectValue<AllocatableAction>> pq = new PriorityQueue();
        int counter = 0;
        for (AllocatableAction action : actions) {
            Score actionScore = action.schedulingScore(this);
            Score score = action.schedulingScore(resource, actionScore);
            if (score == null) {
                continue;
            }
            ObjectValue<AllocatableAction> ov = new ObjectValue(action, score);
            pq.offer(ov);
            counter++;
            if (counter == THRESHOLD) {
                break;
            }
        }

        return pq;
    }

}
