package integratedtoolkit.types.fake;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Worker;

import java.util.LinkedList;


public class FakeAllocatableAction extends AllocatableAction<Profile, MethodResourceDescription, FakeImplementation> {

    private static int[] executions;
    private static int[] error;
    private static int[] failed;

    private int fakeId;


    public FakeAllocatableAction(ActionOrchestrator<Profile, MethodResourceDescription, FakeImplementation> td, int id) {
        super(new FakeSI(), td);
        this.fakeId = id;
    }

    public int getFakeId() {
        return this.fakeId;
    }
    
    public static void resize(int size) {
        FakeAllocatableAction.executions = new int[size];
        FakeAllocatableAction.error = new int[size];
        FakeAllocatableAction.failed = new int[size];
    }
    
    public static int getSize() {
        return FakeAllocatableAction.executions.length;
    }
    
    public static int getExecution(int id) {
        return FakeAllocatableAction.executions[id];
    }
    
    public static int getError(int id) {
        return FakeAllocatableAction.error[id];
    }
    
    public static int getFailed(int id) {
        return FakeAllocatableAction.failed[id];
    }

    @Override
    public void doAction() {
        executions[this.fakeId]++;
    }

    @Override
    public void doCompleted() {

    }

    @Override
    public void doError() throws FailedActionException {
        error[this.fakeId]++;
        if (error[this.fakeId] == 2) {
            throw new FailedActionException();
        }
    }

    @Override
    public void doFailed() {
        failed[this.fakeId]++;
    }

    @Override
    public String toString() {
        return "AllocatableAction " + this.fakeId;
    }

    @Override
    public LinkedList<FakeImplementation> getCompatibleImplementations(
            ResourceScheduler<Profile, MethodResourceDescription, FakeImplementation> r) {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<Profile, MethodResourceDescription, FakeImplementation>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public FakeImplementation[] getImplementations() {
        return new FakeImplementation[0];
    }

    @Override
    public boolean isCompatible(Worker<MethodResourceDescription, FakeImplementation> r) {
        return true;
    }

    @Override
    public boolean areEnoughResources() {
        return true;
    }

    @Override
    protected void reserveResources() {

    }

    @Override
    protected void releaseResources() {

    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<Profile, MethodResourceDescription, FakeImplementation> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<Profile, MethodResourceDescription, FakeImplementation> targetWorker, FakeImplementation impl)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public Score schedulingScore(ResourceScheduler<Profile, MethodResourceDescription, FakeImplementation> targetWorker,
            Score actionScore) {
        return null;
    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public int getPriority() {
        return 0;
    }

}
