package integratedtoolkit.types.fake;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.ResourceScheduler;

import java.util.Arrays;
import java.util.LinkedList;

public class FakeAllocatableAction extends AllocatableAction {

    private int id;
    private Implementation<?>[] impls;

    public FakeAllocatableAction(int id, Implementation<?>[] impls) {
        super(new DefaultSchedulingInformation());
        this.id = id;
        this.impls = impls;
    }

    @Override
    public void doAction() {

    }

    @Override
    public void doCompleted() {

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
    public LinkedList<Implementation<?>> getCompatibleImplementations(ResourceScheduler<?> r) {
        LinkedList<Implementation<?>> ret = new LinkedList<Implementation<?>>();
        ret.addAll(Arrays.asList(impls));
        return ret;
    }

    @Override
    public LinkedList<ResourceScheduler<?>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public Implementation<?>[] getImplementations() {
        return this.impls;
    }

    @Override
    public boolean isCompatible(Worker<?> r) {
        return true;
    }

    @Override
    protected boolean areEnoughResources() {
    	Worker r = selectedResource.getResource();
        return r.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected void reserveResources() {
    	Worker r = selectedResource.getResource();
        r.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected void releaseResources() {
    	Worker r = selectedResource.getResource();
        r.endTask(selectedImpl.getRequirements());
    }

    public void selectExecution(ResourceScheduler resource, Implementation impl) {
        selectedResource = resource;
        selectedImpl = impl;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public Score schedulingScore(TaskScheduler ts) {
        return null;
    }

    @Override
    public Score schedulingScore(ResourceScheduler<?> targetWorker, Score actionScore) {
        return null;
    }

    public String dependenciesDescription() {
        StringBuilder sb = new StringBuilder("Action" + id + "\n");
        DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) this.getSchedulingInfo();
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

}
