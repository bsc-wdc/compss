package es.bsc.compss.scheduler.fifoDataScheduler;

import es.bsc.compss.scheduler.readyScheduler.ReadyResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.json.JSONObject;


public class FIFODataResourceScheduler<T extends WorkerResourceDescription> extends ReadyResourceScheduler<T> {

    /**
     * New ready resource scheduler instance
     *
     * @param w
     * @param resJSON
     * @param implJSON
     */
    public FIFODataResourceScheduler(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        // LOGGER.debug("[FIFODataResourceScheduler] Generate blocked score for action " + action);
        long actionPriority = action.getPriority();
        long resourceScore = -action.getId();
        long waitingScore = 0;
        long implementationScore = 0;

        return new Score(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[FIFODataResourceScheduler] Generate resource score for action " + action);

        long actionPriority = actionScore.getActionScore();
        long resourceScore = -action.getId();
        long waitingScore = 0;
        // double resourceScore = Math.min(1.5, 1.0 / (double) myWorker.getUsedTaskCount());
        long implementationScore = 0;

        return new Score(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl, Score resourceScore) {

        // LOGGER.debug("[FIFODataResourceScheduler] Generate implementation score for action " + action);
        if (myWorker.canRunNow((T) impl.getRequirements())) {
            long actionPriority = resourceScore.getActionScore();
            long resourcePriority = -action.getId();
            long waitingScore = 0;
            long implScore = 0;

            return new Score(actionPriority, resourcePriority, waitingScore, implScore);
        } else {
            // Implementation cannot be run
            return null;
        }
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "FIFODataResourceScheduler@" + getName();
    }

}
