package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;

import java.util.LinkedList;


public class OptimizationAction<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

    public OptimizationAction() {
        super(new DefaultSchedulingInformation<P, T>());
    }

    @Override
    protected boolean areEnoughResources() {
        return true;
    }

    @Override
    protected void reserveResources() {
    }

    @Override
    protected void releaseResources() {

    }

    @Override
    protected void doAction() {

    }

    @Override
    protected void doCompleted() {

    }

    @Override
    protected void doError() throws FailedActionException {

    }

    @Override
    protected void doFailed() {

    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public Implementation<T>[] getImplementations() {
        return null;
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return true;
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r) {
        return null;
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Implementation<T> impl)
            throws BlockedActionException, UnassignedActionException {

    }

}
