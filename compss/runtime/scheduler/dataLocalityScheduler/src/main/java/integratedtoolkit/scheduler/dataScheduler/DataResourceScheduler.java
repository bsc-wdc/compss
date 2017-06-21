package integratedtoolkit.scheduler.dataScheduler;

import integratedtoolkit.scheduler.readyScheduler.ReadyResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import org.json.JSONObject;


public class DataResourceScheduler<T extends WorkerResourceDescription> extends ReadyResourceScheduler<T> {

    /**
     * New ready resource scheduler instance
     *
     * @param w
     * @param json
     */
    public DataResourceScheduler(Worker<T> w, JSONObject json) {
        super(w, json);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        // LOGGER.debug("[DataResourceScheduler] Generate blocked score for action " + action);

        long actionPriority = action.getPriority();
        long waitingScore = -this.blocked.size();
        long resourceScore = 0;
        long implementationScore = 0;

        return new Score(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[DataResourceScheduler] Generate resource score for action " + action);

        long actionPriority = actionScore.getActionScore();
        long waitingScore = -this.blocked.size();
        long resourceScore = Score.calculateDataLocalityScore(params, this.myWorker);
        return new Score(actionPriority, resourceScore, waitingScore, 0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl, Score resourceScore) {
        // LOGGER.debug("[DataResourceScheduler] Generate implementation score for action " + action);

        if (this.myWorker.canRunNow((T) impl.getRequirements())) {
            long actionPriority = resourceScore.getActionScore();
            long waitingScore = resourceScore.getWaitingScore();
            long resourcePriority = resourceScore.getResourceScore();
            long implScore = -this.getProfile(impl).getAverageExecutionTime();

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
        return "DataResourceScheduler@" + getName();
    }

}
