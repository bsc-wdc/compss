package integratedtoolkit.scheduler.dataScheduler;

import java.util.Collection;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.readyScheduler.ReadyScheduler;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.DataScore;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * Representation of a Scheduler that considers only ready tasks
 *
 * @param <P>
 * @param <T>
 */
public class DataScheduler<P extends Profile, T extends WorkerResourceDescription> extends ReadyScheduler<P, T> {

    /**
     * Constructs a new Ready Scheduler instance
     * 
     */
    public DataScheduler() {
        super();
    }

    @Override
    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new ResourceDataScheduler<P, T>(w);
    }

    @Override
    public Score generateActionScore(AllocatableAction<P, T> action) {
        return new DataScore(action.getPriority(), 0, 0, 0);
    }

    @Override
    protected Score generateFullScore(AllocatableAction<P, T> action, ResourceScheduler<P, T> resource, Score actionScore) {
        return action.schedulingScore(resource, actionScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public ResourceScheduler<P, T>[] getWorkers() {
        synchronized (workers) {
            Collection<ResourceScheduler<P, T>> resScheds = workers.values();
            ResourceDataScheduler<P, T>[] scheds = new ResourceDataScheduler[resScheds.size()];
            workers.values().toArray(scheds);
            return scheds;
        }
    }
    
    @Override
    public void dependencyFreeAction(AllocatableAction<P, T> action) throws BlockedActionException {
        dependingActions.removeAction(action); 
        try { 
            Score actionScore = generateActionScore(action); 
            action.schedule(actionScore); 
            try {
                action.tryToLaunch(); 
            } catch (InvalidSchedulingException ise) { 
                boolean keepTrying = true; 
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) { 
                    AllocatableAction<P, T> pre = action.getConstrainingPredecessors().get(i); 
                    action.schedule(pre.getAssignedResource(), actionScore); 
                    try {
                        action.tryToLaunch(); 
                        keepTrying = false; 
                    } catch (InvalidSchedulingException ise2) { // Try next predecessor
                        keepTrying = true; 
                    } 
                } 
            }
        } catch (UnassignedActionException ex) {
            logger.debug("Adding action " + action + " to unassigned list");
            unassignedReadyActions.addAction(action);
        }
    }

}
