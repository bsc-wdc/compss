package integratedtoolkit.scheduler.resourceEmptyScheduler;

import integratedtoolkit.scheduler.readyScheduler.ReadyResourceScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.ResourceEmptyScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.implementations.Implementation;


public class ResourceEmptyResourceScheduler<P extends Profile, T extends WorkerResourceDescription> extends ReadyResourceScheduler<P, T> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public ResourceEmptyResourceScheduler(Worker<T> w) {
        super(w);
    }

    @Override
    public Score generateResourceScore(AllocatableAction<P, T> action, TaskDescription params, Score actionScore) {
        double resourceScore = ResourceEmptyScore.calculateScore(params, myWorker);
        return new ResourceEmptyScore(actionScore.getActionScore(), 0, resourceScore, 0);

    }

    @Override
    public Score generateWaitingScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl, Score resourceScore) {
        double waitingScore = 2.0;
        if (blocked.size() > 0) {
            waitingScore = (double) (1 / (double) blocked.size());
        }
        return new Score(resourceScore.getActionScore(), resourceScore.getResourceScore(), waitingScore, 0);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl,
            Score resourceScore) {

        Worker<T> w = myWorker;
        if (w.canRunNow(impl.getRequirements())) {
            long implScore = this.getProfile(impl).getAverageExecutionTime();
            return new ResourceEmptyScore(resourceScore.getActionScore(), 3, resourceScore.getResourceScore(),
                    (double) (1 / (double) implScore));
        } else {
            // return super.getImplementationScore(action, params, impl, resourceScore);
            return null;
        }
    }

    @Override
    public String toString() {
        return "ResourceEmptyResourceScheduler@" + getName();
    }
}
