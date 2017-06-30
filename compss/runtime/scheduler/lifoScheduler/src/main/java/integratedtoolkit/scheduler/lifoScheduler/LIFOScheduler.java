package integratedtoolkit.scheduler.lifoScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.List;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in LIFO mode
 *
 */
public class LIFOScheduler extends ReadyScheduler {

    /**
     * Constructs a new Ready Scheduler instance
     *
     */
    public LIFOScheduler() {
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
    public <T extends WorkerResourceDescription> LIFOResourceScheduler<T> generateSchedulerForResource(Worker<T> w, JSONObject json) {
        // LOGGER.info("[LIFOScheduler] Generate scheduler for resource " + w.getName());
        return new LIFOResourceScheduler<>(w, json);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        // LOGGER.info("[LIFOScheduler] Generate Action Score for " + action);
        return new Score(action.getPriority(), action.getId(), 0, 0);
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

        List<AllocatableAction> unassignedReadyActions = this.unassignedReadyActions.getAllActions();
        this.unassignedReadyActions.removeAllActions();
        dataFreeActions.addAll(unassignedReadyActions);

    }

}
