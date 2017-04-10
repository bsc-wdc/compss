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
        LOGGER.debug("[LoadBalancingScheduler] Generate blocked score for action " + action);
        double actionPriority = action.getPriority();
        double resourceScore = 0;
        double waitingScore = 2.0;
        if (this.blocked.size() > 0) {
            waitingScore = (double) (1.0 / (double) this.blocked.size());
        }
        double implementationScore = 0;

        return new LoadBalancingScore(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate resource score for action " + action);

        // Gets the action priority
        double actionPriority = actionScore.getActionScore();

        // Computes the resource waiting score
        double waitingScore = (double) -action.getId();
        // Computes the priority of the resource
        double resourceScore = actionScore.calculateResourceScore(params, this.myWorker);
        // Computes the priority of the implementation (should not be computed)
        double implementationScore = -(double) 100;

        LoadBalancingScore score = new LoadBalancingScore(actionPriority, resourceScore, waitingScore, implementationScore);
        // LOGGER.debug("[LoadBalancingScheduler] Resource Score " + score + " " + actionPriority + " " + resourceScore + " " + waitingScore
        //        + " " + implementationScore);

        return score;
    }

    @Override
    public Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl, Score resourceScore) {
        // LOGGER.debug("[LoadBalancing] Generate implementation score for action " + action);

        if (myWorker.canRunNow(impl.getRequirements())) {
            double actionPriority = resourceScore.getActionScore();
            double resourcePriority = resourceScore.getResourceScore();
            double waitingScore = (double) -action.getId();
            double implScore = (double) -this.getProfile(impl).getAverageExecutionTime();

            LoadBalancingScore score = new LoadBalancingScore(actionPriority, resourcePriority, waitingScore, implScore);
            // LOGGER.debug("[LoadBalancingScheduler] Implementation Score " + score);

            return score;
        } else {
            // Implementation cannot be run
            // LOGGER.debug("LoadBalancingScore evaluated to null");
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
