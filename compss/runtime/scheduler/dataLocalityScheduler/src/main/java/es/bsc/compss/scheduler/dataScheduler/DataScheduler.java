package es.bsc.compss.scheduler.dataScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.readyScheduler.ReadyScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.List;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in data locality
 *
 */
public class DataScheduler extends ReadyScheduler {

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
    public <T extends WorkerResourceDescription> DataResourceScheduler<T> generateSchedulerForResource(Worker<T> w, JSONObject resJSON,
            JSONObject implJSON) {
        // LOGGER.debug("[DataScheduler] Generate scheduler for resource " + w.getName());
        return new DataResourceScheduler<>(w, resJSON, implJSON);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        // LOGGER.debug("[DataScheduler] Generate Action Score for " + action);
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

        // Schedules all possible free actions (LIFO type)
        LOGGER.debug("[DataScheduler] Treating dependency free actions");

        List<AllocatableAction> unassignedReadyActions = this.unassignedReadyActions.getAllActions();
        this.unassignedReadyActions.removeAllActions();
        dataFreeActions.addAll(unassignedReadyActions);
    }

}
