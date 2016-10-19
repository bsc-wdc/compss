package integratedtoolkit.types.allocatableactions;

import java.util.LinkedList;

import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;


public class SingleExecution<P extends Profile, T extends WorkerResourceDescription> extends ExecutionAction<P, T> {

    private T resourceConsumption;
    private final ResourceScheduler<P,T> forcedResource;


    public SingleExecution(SchedulingInformation<P, T> schedulingInformation, TaskProducer producer, Task task, 
            ResourceScheduler<P,T> forcedResource) {
        
        super(schedulingInformation, producer, task);
        this.forcedResource = forcedResource;
    }

    @Override
    protected boolean areEnoughResources() {
        Worker<T> w = selectedMainResource.getResource();
        return w.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected void reserveResources() {
        Worker<T> w = selectedMainResource.getResource();
        resourceConsumption = w.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected void releaseResources() {
        Worker<T> w = selectedMainResource.getResource();
        w.endTask(resourceConsumption);
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return targetWorker.getResourceScore(this, task.getTaskDescription(), actionScore);
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");
        
        // COMPUTE RESOURCE CANDIDATES
        LinkedList<ResourceScheduler<?, ?>> candidates = new LinkedList<ResourceScheduler<?, ?>>();
        if (this.forcedResource != null) {
            // The scheduling is forced to a given resource
            candidates.add(this.forcedResource);
        } else if (isSchedulingConstrained()) {
            // The scheduling is constrained by dependencies
            for (AllocatableAction<P, T> a : this.getConstrainingPredecessors()) {
                candidates.add(a.getAssignedResource());
            }
        } else {
            // Free scheduling
            candidates = getCompatibleWorkers();
        }

        // COMPUTE BEST IMPLEMENTATIONS
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;
        int usefulResources = 0;
        for (ResourceScheduler<?, ?> w : candidates) {
            ResourceScheduler<P, T> worker = (ResourceScheduler<P, T>) w;
            if (executingResources.contains(w)) {
                continue;
            }
            Score resourceScore = worker.getResourceScore(this, task.getTaskDescription(), actionScore);
            usefulResources++;
            for (Implementation<T> impl : getCompatibleImplementations(worker)) {
                Score implScore = worker.getImplementationScore(this, task.getTaskDescription(), impl, resourceScore);
                debugString.append(" Resource ").append(w.getName()).append(" ").append(" Implementation ")
                        .append(impl.getImplementationId()).append(" ").append(" Score ").append(implScore).append("\n");
                if (Score.isBetter(implScore, bestScore)) {
                    bestWorker = worker;
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }
        
        // CHECK SCHEDULING RESULT
        if (bestWorker == null) {
            logger.debug(debugString.toString());
            if (usefulResources == 0) {
                logger.info("No worker can run " + this);
                throw new BlockedActionException();
            } else {
                logger.info("No worker has available resources to run " + this);
                throw new UnassignedActionException();
            }
        }

        this.assignImplementation(bestImpl);
        this.assignResources(bestWorker, null);
        logger.debug(debugString.toString());
        logger.info("Assigning action " + this + " to worker" + bestWorker + " with implementation " + bestImpl.getImplementationId());
        bestWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;

        if ( // Resource is not compatible with the Core
                !targetWorker.getResource().canRun(task.getTaskDescription().getId())
                // already ran on the resource
                || executingResources.contains(targetWorker)) {
            throw new UnassignedActionException();
        }
        Score resourceScore = targetWorker.getResourceScore(this, task.getTaskDescription(), actionScore);
        debugString.append("\t Resource ").append(targetWorker.getName()).append("\n");

        for (Implementation<T> impl : getCompatibleImplementations(targetWorker)) {
            Score implScore = targetWorker.getImplementationScore(this, task.getTaskDescription(), impl, resourceScore);
            debugString.append("\t\t Implementation ").append(impl.getImplementationId()).append(implScore).append("\n");
            if (Score.isBetter(implScore, bestScore)) {
                bestWorker = targetWorker;
                bestImpl = impl;
                bestScore = implScore;
            }
        }

        if (bestWorker == null) {
            logger.info("\tWorker " + targetWorker.getName() + "has available resources to run " + this);
            throw new UnassignedActionException();
        }

        this.assignImplementation(bestImpl);
        this.assignResources(bestWorker, null);
        logger.info("\t Worker" + bestWorker + " Implementation " + bestImpl.getImplementationId());
        logger.debug(debugString.toString());
        bestWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Implementation<T> impl)
            throws BlockedActionException, UnassignedActionException {
        
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");

        if ( // Resource is not compatible with the implementation
                !targetWorker.getResource().canRun(impl)
                // already ran on the resource
                || executingResources.contains(targetWorker)) {
            throw new UnassignedActionException();
        }

        this.assignImplementation(impl);
        this.assignResources(targetWorker, null);
        logger.info("\t Worker" + targetWorker + " Implementation " + impl.getImplementationId());
        logger.debug(debugString.toString());
        targetWorker.initialSchedule(this);
    }

}
