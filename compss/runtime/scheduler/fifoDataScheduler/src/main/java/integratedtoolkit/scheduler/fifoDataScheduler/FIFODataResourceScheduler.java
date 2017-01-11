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


public class FIFODataResourceScheduler<P extends Profile, T extends WorkerResourceDescription> extends ReadyResourceScheduler<P, T> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public FIFODataResourceScheduler(Worker<T> w) {
        super(w);
    }

    @Override
    public Score generateResourceScore(AllocatableAction<P, T> action, TaskDescription params, Score actionScore) {
        return new FIFODataScore(actionScore.getActionScore(), 0, Math.min(1.5, 1.0 / (double) myWorker.getUsedTaskCount()), 0);

    }

    @Override
    public Score generateWaitingScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl, Score resourceScore) {
        return new Score(resourceScore.getActionScore(), 1.0 / (double) action.getId(), 0, 0);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl,
            Score resourceScore) {
        Worker<T> w = myWorker;
        if (w.canRunNow(impl.getRequirements())) {
            return new FIFODataScore(resourceScore.getActionScore(), 0, 0, 1.0 / (double) action.getId());
        } else {
            return null;
        }
    }

    @Override
    public String toString() {
        return "FIFOResourceScheduler@" + getName();
    }

}
