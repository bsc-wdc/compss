package commons;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;

public class Action extends AllocatableAction {

    private final int coreId;

    public Action(ActionOrchestrator orchestrator, int coreId) {

        super(new SchedulingInformation(null), orchestrator);
        this.coreId = coreId;
    }

    @Override
    public boolean isToReserveResources() {
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
        return true;
    }

    @Override
    public LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return getCoreElementExecutors(coreId);
    }

    @Override
    public <T extends WorkerResourceDescription> LinkedList<Implementation> getCompatibleImplementations(
            ResourceScheduler<T> r) {
        return r.getExecutableImpls(coreId);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Implementation[] getImplementations() {
        List<Implementation> impls = CoreManager.getCoreImplementations(coreId);

        int implsSize = impls.size();
        Implementation[] resultImpls = new Implementation[implsSize];
        for (int i = 0; i < implsSize; ++i) {
            resultImpls[i] = (Implementation) impls.get(i);
        }
        return resultImpls;
    }

    @Override
    public <T extends WorkerResourceDescription> boolean isCompatible(Worker<T> r) {
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
    public <T extends WorkerResourceDescription> Score schedulingScore(
            ResourceScheduler<T> targetWorker,
            Score actionScore) {
        return new Score(0, 0, 0, 0);
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker,
            Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker,
            Implementation impl) throws BlockedActionException, UnassignedActionException {

    }

    public HashMap<Worker<?>, LinkedList<Implementation>> findAvailableWorkers() {
        HashMap<Worker<?>, LinkedList<Implementation>> m = new HashMap<>();

        LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = getCoreElementExecutors(
                coreId);
        for (ResourceScheduler<? extends WorkerResourceDescription> ui : compatibleWorkers) {
            Worker<WorkerResourceDescription> r = (Worker<WorkerResourceDescription>)ui.getResource();
            LinkedList<Implementation> compatibleImpls = r.getExecutableImpls(coreId);
            LinkedList<Implementation> runnableImpls = new LinkedList<>();
            for (Implementation impl : compatibleImpls) {
                if (r.canRunNow(impl.getRequirements())) {
                    runnableImpls.add(impl);
                }
            }
            if (runnableImpls.size() > 0) {
                m.put((Worker<?>) r, runnableImpls);
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
