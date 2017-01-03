package integratedtoolkit.types.allocatiableaction;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.allocatiableaction.AllocatableActionTest.ResourceDependencies;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;

import java.util.LinkedList;


public class AllocatableActionImpl<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

    private int id;
    public static int[] executions;
    public static int[] error;
    public static int[] failed;


    public AllocatableActionImpl(int id) {
        super(new ResourceDependencies<P, T>());
        this.id = id;
    }

    @Override
    public void doAction() {
        executions[id]++;
    }

    @Override
    public void doCompleted() {

    }

    @Override
    public void doError() throws FailedActionException {
        error[id]++;
        if (error[id] == 2) {
            throw new FailedActionException();
        }
    }

    @Override
    public void doFailed() {
        failed[id]++;
    }

    @Override
    public String toString() {
        return "AllocatableAction " + id;
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r) {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers() {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Implementation<T>[] getImplementations() {
        return new Implementation[0];
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return true;
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
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Implementation<T> impl)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return null;
    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public long getId() {
        return id;
    }

    @Override
    public int getPriority() {
        return 0;
    }

}
