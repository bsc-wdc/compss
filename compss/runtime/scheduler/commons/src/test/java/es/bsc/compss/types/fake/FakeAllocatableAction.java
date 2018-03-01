package es.bsc.compss.types.fake;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.LinkedList;


public class FakeAllocatableAction extends AllocatableAction {

    private static int[] executions;
    private static int[] error;
    private static int[] failed;

    private int fakeId;


    public FakeAllocatableAction(ActionOrchestrator td, int id) {
        super(new FakeSI(null), td);
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
    public <T extends WorkerResourceDescription> LinkedList<Implementation> getCompatibleImplementations(ResourceScheduler<T> r) {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public FakeImplementation[] getImplementations() {
        return new FakeImplementation[0];
    }

    @Override
    public <T extends WorkerResourceDescription> boolean isCompatible(Worker<T> r) {
        return true;
    }

    @Override
    public boolean isToReserveResources() {
        return false;
    }

    @Override
    public boolean isToReleaseResources() {
        return false;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }
    
    @Override
    public void tryToSchedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker, Score actionScore) {
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
