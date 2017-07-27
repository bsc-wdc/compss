package es.bsc.compss.scheduler.types.fake;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;

import java.util.LinkedList;
import java.util.List;


public class FakeAllocatableAction extends AllocatableAction<FakeProfile, FakeResourceDescription, FakeImplementation> {

    private final int id;
    private final int priority;
    private final List<Implementation<?>> impls;


    public FakeAllocatableAction(ActionOrchestrator<FakeProfile, FakeResourceDescription, FakeImplementation> orchestrator, int id,
            int priority, List<Implementation<?>> impls) {

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
        for (Implementation<?> impl : this.impls) {
            ret.add((FakeImplementation) impl);
        }

        return ret;
    }

    @Override
    public LinkedList<ResourceScheduler<FakeProfile, FakeResourceDescription, FakeImplementation>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public FakeImplementation[] getImplementations() {
        int implsSize = this.impls.size();
        FakeImplementation[] implementations = new FakeImplementation[implsSize];
        for (int i = 0; i < implsSize; ++i) {
            implementations[i] = (FakeImplementation) this.impls.get(i);
        }
        return implementations;
    }

    @Override
    public boolean isCompatible(Worker<FakeResourceDescription, FakeImplementation> r) {
        return true;
    }

    @Override
    public boolean isToReserveResources() {
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
        return true;
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

        FakeImplementation impl = (FakeImplementation) impls.get(0);
        this.assignImplementation(impl);
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
        if (impls == null || impls.size() == 0) {
            return null;
        } else {
            return impls.get(0).getCoreId();
        }
    }

    @Override
    public int getPriority() {
        return priority;
    }

}
