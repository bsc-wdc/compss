package integratedtoolkit.scheduler.types.fake;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.resources.Worker;

import java.util.Arrays;
import java.util.LinkedList;


public class FakeAllocatableAction extends AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation> {

    private int id;
    private int priority;
    private FakeImplementation[] impls;


    public FakeAllocatableAction(ActionOrchestrator<FakeProfile, FakeResourceDescription, FakeImplementation> orchestrator, int id,
            int priority, FakeImplementation[] impls) {

        super(new FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>(), orchestrator);

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

    @Override
    public String toString() {
        return "AllocatableAction " + id;
    }

    @Override
    public LinkedList<FakeImplementation> getCompatibleImplementations(
            ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> r) {
        LinkedList<FakeImplementation> ret = new LinkedList<>();
        ret.addAll(Arrays.asList(impls));

        return ret;
    }

    @Override
    public LinkedList<ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public FakeImplementation[] getImplementations() {
        return this.impls;
    }

    @Override
    public boolean isCompatible(Worker<FakeResourceDescription, FakeImplementation> r) {
        return true;
    }

    @Override
    public boolean areEnoughResources() {
        Worker<FakeResourceDescription, FakeImplementation> r = selectedResource.getResource();
        return r.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected void reserveResources() {
        Worker<FakeResourceDescription, FakeImplementation> r = selectedResource.getResource();
        r.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected void releaseResources() {
        Worker<FakeResourceDescription, FakeImplementation> r = selectedResource.getResource();
        r.endTask(selectedImpl.getRequirements());
    }

    public void selectExecution(ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> resource,
            FakeImplementation impl) {
        selectedResource = resource;
        selectedImpl = impl;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {
        this.assignImplementation(impls[0]);
        this.assignResources(targetWorker);
        targetWorker.scheduleAction(this);
    }

    @Override
    public void schedule(ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> targetWorker, FakeImplementation impl)
            throws BlockedActionException, UnassignedActionException {
        this.assignImplementation(impl);
        this.assignResources(targetWorker);
        targetWorker.scheduleAction(this);
    }

    @Override
    public Score schedulingScore(ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation> targetWorker,
            Score actionScore) {
        return null;
    }

    public String dependenciesDescription() {
        StringBuilder sb = new StringBuilder("Action" + id + "\n");
        FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation> dsi = (FullGraphSchedulingInformation<FakeProfile, FakeResourceDescription, FakeImplementation>) this
                .getSchedulingInfo();
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
