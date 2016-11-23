package commons;

import java.util.HashMap;
import java.util.LinkedList;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceScheduler;


@SuppressWarnings({ "rawtypes", "unchecked" })
public class Action<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

    final int coreId;


    public Action(int coreId) {
        super(new SchedulingInformation());
        this.coreId = coreId;
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

    @Override
    public LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers() {
        return getCoreElementExecutors(coreId);
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r) {
        return r.getExecutableImpls(coreId);
    }

    @Override
    public Implementation<T>[] getImplementations() {
        return (Implementation<T>[]) CoreManager.getCoreImplementations(coreId);
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return r.canRun(coreId);
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
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return new Score(0, 0, 0, 0);
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

    public HashMap<Worker<T>, LinkedList<Implementation<T>>> findAvailableWorkers() {
        HashMap<Worker<T>, LinkedList<Implementation<T>>> m = new HashMap<Worker<T>, LinkedList<Implementation<T>>>();

        LinkedList<ResourceScheduler<?, ?>> compatibleWorkers = getCoreElementExecutors(coreId);
        for (ResourceScheduler<?, ?> ui : compatibleWorkers) {
            Worker<T> r = (Worker<T>) ui.getResource();
            LinkedList<Implementation<T>> compatibleImpls = r.getExecutableImpls(coreId);
            LinkedList<Implementation<T>> runnableImpls = new LinkedList<Implementation<T>>();
            for (Implementation<?> impl : compatibleImpls) {
                if (r.canRunNow((T) impl.getRequirements())) {
                    runnableImpls.add((Implementation<T>) impl);
                }
            }
            if (runnableImpls.size() > 0) {
                m.put(r, runnableImpls);
            }
        }
        return m;
    }

    @Override
    public Integer getCoreId() {
        return coreId;
    }

    @Override
    public int getPriority() {
        return 0;
    }
}
