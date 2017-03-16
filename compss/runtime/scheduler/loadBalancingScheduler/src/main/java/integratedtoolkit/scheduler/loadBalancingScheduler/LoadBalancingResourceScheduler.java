package integratedtoolkit.scheduler.loadBalancingScheduler;

import integratedtoolkit.scheduler.readyScheduler.ReadyResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.LoadBalancingScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.implementations.Implementation;


public class LoadBalancingResourceScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyResourceScheduler<P, T, I> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public LoadBalancingResourceScheduler(Worker<T, I> w) {
        super(w);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction<P, T, I> action) {
        //LOGGER.debug("[ResourceEmptyScheduler] Generate blocked score for action " + action);
        double actionPriority = action.getPriority();
        double waitingScore = 2.0;
        if (this.blocked.size() > 0) {
            waitingScore = (double) (1.0 / (double) this.blocked.size());
        }
        double resourceScore = 0;
        double implementationScore = 0;

        return new LoadBalancingScore(actionPriority, waitingScore, resourceScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore) {
        //LOGGER.debug("[ResourceEmptyScheduler] Generate resource score for action " + action);

        // Gets the action priority
        double actionPriority = actionScore.getActionScore();

        // Computes the resource waiting score
        double waitingScore = 2.0;
        if (this.blocked.size() > 0) {
            waitingScore = (double) (1.0 / (double) this.blocked.size());
        }
        // Computes the priority of the resource
        double resourceScore = LoadBalancingScore.calculateScore(params, this.myWorker);
        // Computes the priority of the implementation (should not be computed)
        double implementationScore = 0;

        LoadBalancingScore score = new LoadBalancingScore(actionPriority, waitingScore, resourceScore, implementationScore);
        //LOGGER.debug(score);

        return score;
    }

    @Override
    public Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl, Score resourceScore) {
        //LOGGER.debug("[ResourceScheduler] Generate implementation score for action " + action);

        if (myWorker.canRunNow(impl.getRequirements())) {
            double actionPriority = resourceScore.getActionScore();
            double waitingScore = resourceScore.getWaitingScore();
            double resourcePriority = resourceScore.getResourceScore();
            double implScore = 1.0 / ((double) this.getProfile(impl).getAverageExecutionTime());

            LoadBalancingScore score = new LoadBalancingScore(actionPriority, waitingScore, resourcePriority, implScore);
            //LOGGER.debug(score);

            return score;
        } else {
            // Implementation cannot be run
            //LOGGER.debug("ResourceEmtpyScore evaluated to null");
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
        return "ResourceEmptyResourceScheduler@" + getName();
    }
}
