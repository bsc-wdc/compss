package es.bsc.compss.scheduler.loadBalancingScheduler;

import es.bsc.compss.scheduler.readyScheduler.ReadyResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.schedulerloadBalancingScheduler.types.LoadBalancingScore;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.parameter.Parameter;
import org.json.JSONObject;


public class LoadBalancingResourceScheduler<T extends WorkerResourceDescription> extends ReadyResourceScheduler<T> {

    /**
     * New ready resource scheduler instance
     *
     * @param w
     * @param resJSON
     * @param implJSON
     */
    public LoadBalancingResourceScheduler(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate blocked score for action " + action);
        long actionPriority = action.getPriority();
        long resourceScore = 0;
        long waitingScore = this.blocked.size();
        long implementationScore = 0;

        return new LoadBalancingScore(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate resource score for action " + action);

        // Gets the action priority
        long actionPriority = actionScore.getActionScore();

        // Computes the resource waiting score
        long waitingScore = -action.getId();
        // Computes the priority of the resource
        long resourceScore = calculateResourceScore(params);
        // Computes the priority of the implementation (should not be computed)
        long implementationScore = -100;

        LoadBalancingScore score = new LoadBalancingScore(actionPriority, resourceScore, waitingScore, implementationScore);
        // LOGGER.debug("[LoadBalancingScheduler] Resource Score " + score + " " + actionPriority + " " + resourceScore
        // + " " + waitingScore
        // + " " + implementationScore);

        return score;
    }

    public long calculateResourceScore(TaskDescription params) {
        long resourceScore = 0;
        if (params != null) {
            Parameter[] parameters = params.getParameters();
            if (parameters.length == 0) {
                return 1;
            }
            resourceScore = 2 * Score.calculateDataLocalityScore(params, myWorker);
        }
        return resourceScore;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl, Score resourceScore) {
        // LOGGER.debug("[LoadBalancing] Generate implementation score for action " + action);

        if (myWorker.canRunNow((T) impl.getRequirements())) {
            long actionPriority = resourceScore.getActionScore();
            long resourcePriority = resourceScore.getResourceScore();
            long waitingScore = -action.getId();
            long implScore = -this.getProfile(impl).getAverageExecutionTime();

            return new LoadBalancingScore(actionPriority, resourcePriority, waitingScore, implScore);
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
        return "LoadBalancingResourceScheduler@" + getName();
    }
}
