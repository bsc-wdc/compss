package integratedtoolkit.scheduler.fifoScheduler;

import integratedtoolkit.scheduler.readyScheduler.ReadyResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FIFOScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.implementations.Implementation;


public class FIFOResourceScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ReadyResourceScheduler<P, T, I> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public FIFOResourceScheduler(Worker<T, I> w) {
        super(w);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[FIFOResourceScheduler] Generate blocked score for action " + action);
        double actionPriority = action.getPriority();
        double waitingScore = -(double) action.getId();
        double resourceScore = 0;
        double implementationScore = 0;

        return new FIFOScore(actionPriority, waitingScore, resourceScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore) {
        LOGGER.debug("[FIFOResourceScheduler] Generate resource score for action " + action);

        double actionPriority = actionScore.getActionScore();
        double waitingScore = -(double) action.getId();
        double resourceScore = 0;
        double implementationScore = 0;

        return new FIFOScore(actionPriority, waitingScore, resourceScore, implementationScore);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl, Score resourceScore) {
        LOGGER.debug("[FIFOResourceScheduler] Generate implementation score for action " + action);

        if (myWorker.canRunNow(impl.getRequirements())) {
            double actionPriority = resourceScore.getActionScore();
            double waitingScore = resourceScore.getWaitingScore();
            double resourcePriority = resourceScore.getResourceScore();
            double implScore = -(double) action.getId();

            return new FIFOScore(actionPriority, waitingScore, resourcePriority, implScore);
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
        return "FIFOResourceScheduler@" + getName();
    }

}
