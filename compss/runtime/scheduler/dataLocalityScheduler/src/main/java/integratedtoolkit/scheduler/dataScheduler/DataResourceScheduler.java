package integratedtoolkit.scheduler.dataScheduler;

import integratedtoolkit.scheduler.readyScheduler.ReadyResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.DataScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.implementations.Implementation;


public class DataResourceScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyResourceScheduler<P, T, I> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public DataResourceScheduler(Worker<T, I> w) {
        super(w);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction<P, T, I> action) {
        // LOGGER.debug("[DataResourceScheduler] Generate blocked score for action " + action);
        double actionPriority = action.getPriority();
        double waitingScore = 2.0;
        if (this.blocked.size() > 0) {
            waitingScore = (double) (1 / (double) this.blocked.size());
        }
        double resourceScore = 0;
        double implementationScore = 0;

        return new DataScore(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[DataResourceScheduler] Generate resource score for action " + action);

        double actionPriority = actionScore.getActionScore();

        double waitingScore = 2.0;
        if (this.blocked.size() > 0) {
            waitingScore = (double) (1 / (double) this.blocked.size());
        }

        double resourceScore = actionScore.calculateResourceScore(params, this.myWorker);

        return new DataScore(actionPriority, resourceScore, waitingScore, 0);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl, Score resourceScore) {
        // LOGGER.debug("[DataResourceScheduler] Generate implementation score for action " + action);

        if (this.myWorker.canRunNow(impl.getRequirements())) {
            double actionPriority = resourceScore.getActionScore();
            double waitingScore = resourceScore.getWaitingScore();
            double resourcePriority = resourceScore.getResourceScore();
            double implScore = 1.0 / ((double) this.getProfile(impl).getAverageExecutionTime());

            return new DataScore(actionPriority, resourcePriority, waitingScore, implScore);
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
