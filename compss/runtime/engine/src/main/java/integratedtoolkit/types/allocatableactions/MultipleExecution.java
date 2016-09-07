package integratedtoolkit.types.allocatableactions;

import java.util.LinkedList;

import integratedtoolkit.components.impl.TaskDispatcher.TaskProducer;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;


public class MultipleExecution<P extends Profile, T extends WorkerResourceDescription> extends ExecutionAction<P, T> {

    private WorkerResourceDescription mainResourceConsumption;
    private WorkerResourceDescription slavesResourceConsumption;


    public MultipleExecution(SchedulingInformation<P, T> schedulingInformation, TaskProducer producer, Task task) {
        super(schedulingInformation, producer, task);
    }

    @Override
    protected boolean areEnoughResources() {
        boolean canRun = false;

        // Check main resource
        Worker<T> w = selectedMainResource.getResource();
        canRun = w.canRunNow(selectedImpl.getRequirements());

        // Check slaves
        for (ResourceScheduler<P, T> rs : selectedSlaveResources) {
            Worker<T> slave = rs.getResource();
            canRun = canRun || slave.canRunNow(selectedImpl.getRequirements());
        }

        return canRun;
    }

    @Override
    protected void reserveResources() {
        // Reserve main
        Worker<T> w = selectedMainResource.getResource();
        mainResourceConsumption = w.runTask(selectedImpl.getRequirements());

        // Reserve slaves
        for (ResourceScheduler<P, T> rs : selectedSlaveResources) {
            Worker<T> slave = rs.getResource();
            slave.runTask(selectedImpl.getRequirements());
        }
    }

    @Override
    protected void releaseResources() {
        // Release main
        Worker w = selectedMainResource.getResource();
        w.endTask(mainResourceConsumption);

        // Release slaves
        for (ResourceScheduler<P, T> rs : selectedSlaveResources) {
            Worker slave = rs.getResource();
            slave.endTask(slavesResourceConsumption);
        }
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return targetWorker.getResourceScore(this, task.getTaskParams(), actionScore);
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution:\n");
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;
        LinkedList<ResourceScheduler<?, ?>> candidates;
        if (isSchedulingConstrained()) {
            candidates = new LinkedList<ResourceScheduler<?, ?>>();
            candidates.add(this.getConstrainingPredecessor().getAssignedResource());
        } else {
            candidates = getCompatibleWorkers();
        }
        int usefulResources = 0;
        for (ResourceScheduler<?, ?> w : candidates) {
            ResourceScheduler<P, T> worker = (ResourceScheduler<P, T>) w;
            if (executingResources.contains(w)) {
                continue;
            }
            Score resourceScore = worker.getResourceScore(this, task.getTaskParams(), actionScore);
            usefulResources++;
            for (Implementation<T> impl : getCompatibleImplementations(worker)) {
                Score implScore = worker.getImplementationScore(this, task.getTaskParams(), impl, resourceScore);
                debugString.append(" Resource ").append(w.getName()).append(" ").append(" Implementation ")
                        .append(impl.getImplementationId()).append(" ").append(" Score ").append(implScore).append("\n");
                if (Score.isBetter(implScore, bestScore)) {
                    bestWorker = worker;
                    bestImpl = impl;
                    bestScore = implScore;
                }
            }
        }
        if (bestWorker == null) {
            if (usefulResources == 0) {
                logger.debug(debugString.toString());
                logger.info("No worker can run " + this + "\n");
                throw new BlockedActionException();
            } else {
                logger.debug(debugString.toString());
                logger.info("No worker has available resources to run " + this + "\n");
                throw new UnassignedActionException();
            }
        }

        this.assignImplementation(bestImpl);
        this.assignResources(bestWorker, null);
        logger.debug(debugString.toString());
        logger.info(
                "Assigning action " + this + " to worker" + bestWorker + " with implementation " + bestImpl.getImplementationId() + "\n");
        bestWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        StringBuilder debugString = new StringBuilder("Scheduling " + this + " execution for worker " + targetWorker + ":\n");
        ResourceScheduler<P, T> bestWorker = null;
        Implementation<T> bestImpl = null;
        Score bestScore = null;

        if ( // Resource is not compatible with the Core
        !targetWorker.getResource().canRun(task.getTaskParams().getId())
                // already ran on the resource
                || executingResources.contains(targetWorker)) {
            throw new UnassignedActionException();
        }
        Score resourceScore = targetWorker.getResourceScore(this, task.getTaskParams(), actionScore);
        debugString.append("\t Resource ").append(targetWorker.getName()).append("\n");

        for (Implementation<T> impl : getCompatibleImplementations(targetWorker)) {
            Score implScore = targetWorker.getImplementationScore(this, task.getTaskParams(), impl, resourceScore);
            debugString.append("\t\t Implementation ").append(impl.getImplementationId()).append(implScore).append("\n");
            if (Score.isBetter(implScore, bestScore)) {
                bestWorker = targetWorker;
                bestImpl = impl;
                bestScore = implScore;
            }
        }

        if (bestWorker == null) {
            logger.info("\tWorker " + targetWorker.getName() + "has available resources to run " + this + "\n");
            throw new UnassignedActionException();
        }

        this.assignImplementation(bestImpl);
        this.assignResources(bestWorker, null);
        logger.info("\t Worker" + bestWorker + " Implementation " + bestImpl.getImplementationId() + "\n");
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
        logger.info("\t Worker" + targetWorker + " Implementation " + impl.getImplementationId() + "\n");
        logger.debug(debugString.toString());
        targetWorker.initialSchedule(this);
    }

}
