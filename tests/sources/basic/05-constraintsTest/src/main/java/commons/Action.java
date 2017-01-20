package commons;

import java.util.HashMap;
import java.util.LinkedList;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;


public class Action extends AllocatableAction<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> {

    private final int coreId;


    public Action(ActionOrchestrator<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> orchestrator,
            int coreId) {

        super(new SchedulingInformation<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>(), orchestrator);
        this.coreId = coreId;
    }

    @Override
    public boolean areEnoughResources() {
        Worker<WorkerResourceDescription, Implementation<WorkerResourceDescription>> r = selectedResource.getResource();
        return r.canRunNow(selectedImpl.getRequirements());
    }

    @Override
    protected void reserveResources() {
        Worker<WorkerResourceDescription, Implementation<WorkerResourceDescription>> r = selectedResource.getResource();
        r.runTask(selectedImpl.getRequirements());
    }

    @Override
    protected void releaseResources() {
        Worker<WorkerResourceDescription, Implementation<WorkerResourceDescription>> r = selectedResource.getResource();
        r.endTask(selectedImpl.getRequirements());
    }

    @Override
    public LinkedList<ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>> getCompatibleWorkers() {
        return getCoreElementExecutors(coreId);
    }

    @Override
    public LinkedList<Implementation<WorkerResourceDescription>> getCompatibleImplementations(
            ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> r) {
        return r.getExecutableImpls(coreId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Implementation<WorkerResourceDescription>[] getImplementations() {
        return (Implementation<WorkerResourceDescription>[]) CoreManager.getCoreImplementations(coreId);
    }

    @Override
    public boolean isCompatible(Worker<WorkerResourceDescription, Implementation<WorkerResourceDescription>> r) {
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
    public Score schedulingScore(
            ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> targetWorker,
            Score actionScore) {
        return new Score(0, 0, 0, 0);
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> targetWorker,
            Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public void schedule(ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> targetWorker,
            Implementation<WorkerResourceDescription> impl) throws BlockedActionException, UnassignedActionException {

    }

    public HashMap<Worker<?, ?>, LinkedList<Implementation<?>>> findAvailableWorkers() {
        HashMap<Worker<?, ?>, LinkedList<Implementation<?>>> m = new HashMap<>();

        LinkedList<ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>> compatibleWorkers = getCoreElementExecutors(
                coreId);
        for (ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> ui : compatibleWorkers) {
            Worker<WorkerResourceDescription, Implementation<WorkerResourceDescription>> r = ui.getResource();
            LinkedList<Implementation<WorkerResourceDescription>> compatibleImpls = r.getExecutableImpls(coreId);
            LinkedList<Implementation<?>> runnableImpls = new LinkedList<>();
            for (Implementation<WorkerResourceDescription> impl : compatibleImpls) {
                if (r.canRunNow(impl.getRequirements())) {
                    runnableImpls.add(impl);
                }
            }
            if (runnableImpls.size() > 0) {
                m.put((Worker<?, ?>) r, runnableImpls);
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
