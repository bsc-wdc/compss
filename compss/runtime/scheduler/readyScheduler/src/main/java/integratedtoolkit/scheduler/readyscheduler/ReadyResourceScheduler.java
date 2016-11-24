package integratedtoolkit.scheduler.readyscheduler;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;


/**
 * Implementation for the ReadyResourceScheduler
 *
 * @param <P>
 * @param <T>
 */
public class ReadyResourceScheduler<P extends Profile, T extends WorkerResourceDescription> extends ResourceScheduler<P, T> {

    /**
     * New ready resource scheduler instance
     * 
     * @param w
     */
    public ReadyResourceScheduler(Worker<T> w) {
        super(w);
    }

    /*
     * It filters the implementations that can be executed at the moment. If there are no available resources in the
     * worker to host the implementation execution, it ignores the implementation.
     */
    @Override
    public Score getImplementationScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl,
            Score resourceScore) {
        
        Worker<T> w = myWorker;
        if (w.canRunNow(impl.getRequirements())) {
            long implScore = this.getProfile(impl).getAverageExecutionTime();
            return new Score(resourceScore, 3, (double) (1 / (double) implScore));
        } else {
            // return super.getImplementationScore(action, params, impl, resourceScore);
            return null;
        }
    }

}
