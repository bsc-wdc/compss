package integratedtoolkit.types.fake;

import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;

import java.util.Arrays;
import java.util.LinkedList;


public class FakeAllocatableAction<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

    private int id;
    private int priority;
    private Implementation<T>[] impls;


    public FakeAllocatableAction(int id, int priority, Implementation<T>[] impls) {
        super(new DefaultSchedulingInformation<P, T>());
        this.id = id;
        this.priority = priority;
        this.impls = impls;
    }

    @Override
    public void doAction() {
        profile.start();
    }

    @Override
    public void doCompleted() {
        profile.end();
    }

    @Override
    public void doError() throws FailedActionException {
    }

    @Override
    public void doFailed() {
    }

    public String toString() {
        return "AllocatableAction " + id;
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r) {
        LinkedList<Implementation<T>> ret = new LinkedList<>();
        ret.addAll(Arrays.asList(impls));
        return ret;
    }

    @Override
    public LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public Implementation<T>[] getImplementations() {
        return this.impls;
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return true;
    }

    @Override
    protected boolean areEnoughResources() {
        Worker<T> r = selectedResource.getResource();
        return r.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected void reserveResources() {
        Worker<T> r = selectedResource.getResource();
        r.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected void releaseResources() {
        Worker<T> r = selectedResource.getResource();
        r.endTask(selectedImpl.getRequirements());
    }

    public void selectExecution(ResourceScheduler<P, T> resource, Implementation<T> impl) {
        selectedResource = resource;
        selectedImpl = impl;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.assignImplementation(impls[0]);
        this.assignResources(targetWorker);
        targetWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Implementation<T> impl)
            throws BlockedActionException, UnassignedActionException {
        this.assignImplementation(impl);
        this.assignResources(targetWorker);
        targetWorker.initialSchedule(this);
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return null;
    }

    public String dependenciesDescription() {
        StringBuilder sb = new StringBuilder("Action" + id + "\n");
        DefaultSchedulingInformation<P, T> dsi = (DefaultSchedulingInformation<P, T>) this.getSchedulingInfo();
        sb.append("\t depends on\n");
        sb.append("\t\tData : ").append(this.getDataPredecessors()).append("\n");
        sb.append("\t\tResource : ").append(dsi.getPredecessors()).append("\n");
        sb.append("\t enables\n");
        sb.append("\t\tData : ").append(this.getDataSuccessors()).append("\n");
        sb.append("\t\tResource : ").append(dsi.getSuccessors()).append("\n");

        return sb.toString();
    }

    @Override
    public Integer getCoreId() {
        if (impls == null || impls.length == 0) {
            return null;
        } else {
            return impls[0].getCoreId();
        }
    }

    @Override
    public int getPriority() {
        return priority;
    }

}
