package integratedtoolkit.scheduler.fifoDataScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.ObjectValue;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.List;
import java.util.PriorityQueue;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode + data locality
 *
 */
public class FIFODataScheduler extends ReadyScheduler {

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
    public <T extends WorkerResourceDescription> FIFODataResourceScheduler<T> generateSchedulerForResource(Worker<T> w, JSONObject json) {
        // LOGGER.debug("[FIFODataScheduler] Generate scheduler for resource " + w.getName());
        return new FIFODataResourceScheduler<>(w, json);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        // LOGGER.debug("[FIFODataScheduler] Generate Action Score for " + action);
        return new Score(action.getPriority(), 0, 0, 0);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> void purgeFreeActions(List<AllocatableAction> dataFreeActions,
            List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {

        LOGGER.debug("[DataScheduler] Treating dependency free actions");

        PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
        for (AllocatableAction action : dataFreeActions) {
            Score actionScore = this.generateActionScore(action);
            Score fullScore = action.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
            executableActions.add(obj);
        }
        dataFreeActions.clear();
        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();
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

        List<AllocatableAction> unassignedReadyActions = this.unassignedReadyActions.getAllActions();
        this.unassignedReadyActions.removeAllActions();
        dataFreeActions.addAll(unassignedReadyActions);
    }

}
