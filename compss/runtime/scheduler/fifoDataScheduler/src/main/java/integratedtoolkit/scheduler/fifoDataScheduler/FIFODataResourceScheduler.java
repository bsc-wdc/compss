package integratedtoolkit.scheduler.fifoDataScheduler;

import integratedtoolkit.scheduler.readyScheduler.ReadyResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FIFODataScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.implementations.Implementation;


public class FIFODataResourceScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyResourceScheduler<P, T, I> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public FIFODataResourceScheduler(Worker<T, I> w) {
        super(w);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction<P, T, I> action) {
        //LOGGER.debug("[FIFODataResourceScheduler] Generate blocked score for action " + action);
        double actionPriority = action.getPriority();
        double waitingScore = -(double) action.getId();
        double resourceScore = 0;
        double implementationScore = 0;

        return new FIFODataScore(actionPriority, waitingScore, resourceScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore) {
        //LOGGER.debug("[FIFODataResourceScheduler] Generate resource score for action " + action);

        double actionPriority = actionScore.getActionScore();
        double waitingScore = -(double) action.getId();
        double resourceScore = FIFODataScore.calculateScore(params, this.myWorker);
        // double resourceScore = Math.min(1.5, 1.0 / (double) myWorker.getUsedTaskCount());
        double implementationScore = 0;

        return new FIFODataScore(actionPriority, waitingScore, resourceScore, implementationScore);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl, Score resourceScore) {
        //LOGGER.debug("[FIFODataResourceScheduler] Generate implementation score for action " + action);

        if (myWorker.canRunNow(impl.getRequirements())) {
            double actionPriority = resourceScore.getActionScore();
            double waitingScore = resourceScore.getWaitingScore();
            double resourcePriority = resourceScore.getResourceScore();
            double implScore = -(double) action.getId();

            return new FIFODataScore(actionPriority, waitingScore, resourcePriority, implScore);
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
