
package commons;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.worker.COMPSsException;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class Action extends AllocatableAction {

    private final CoreElement core;

    public Action(ActionOrchestrator orchestrator, CoreElement core) {

        super(new SchedulingInformation(null), orchestrator);
        this.core = core;
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
    public boolean isToStopResource() {
        return true;
    }

    @Override
    public List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return getCoreElementExecutors(core.getCoreId());
    }

    @Override
    public <T extends WorkerResourceDescription> List<Implementation> getCompatibleImplementations(
            ResourceScheduler<T> r) {
        return r.getExecutableImpls(core.getCoreId());
    }

    @Override
    public Implementation[] getImplementations() {
        List<Implementation> impls = core.getImplementations();

        int implsSize = impls.size();
        Implementation[] resultImpls = new Implementation[implsSize];
        for (int i = 0; i < implsSize; ++i) {
            resultImpls[i] = (Implementation) impls.get(i);
        }
        return resultImpls;
    }

    @Override
    public <T extends WorkerResourceDescription> boolean isCompatible(Worker<T> r) {
        return r.canRun(core.getCoreId());
    }

    @Override
    protected void doAction() {

    }

    @Override
    protected void doAbort() {

    }

    @Override
    protected void doCompleted() {

    }

    @Override
    protected void doFailIgnored() {

    }

    @Override
    protected void doError() throws FailedActionException {

    }

    @Override
    protected void doFailed() {

    }

    @Override
    protected void doCanceled() {

    }

    @Override
    protected void doException(COMPSsException e) {
        
    }
    
    @Override
    public <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
            Score actionScore) {
        return new Score(0, 0, 0, 0);
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {

    }

    @SuppressWarnings("unchecked")
    public Map<Worker<?>, List<Implementation>> findAvailableWorkers() {
        Map<Worker<?>, List<Implementation>> m = new HashMap<>();

        List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers = getCoreElementExecutors(
                core.getCoreId());
        for (ResourceScheduler<? extends WorkerResourceDescription> ui : compatibleWorkers) {
            Worker<WorkerResourceDescription> r = (Worker<WorkerResourceDescription>) ui.getResource();
            List<Implementation> compatibleImpls = r.getExecutableImpls(core.getCoreId());
            List<Implementation> runnableImpls = new LinkedList<>();
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
        return core.getCoreId();
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public OnFailure getOnFailure() {
        return OnFailure.RETRY;
    }

    @Override
    public boolean taskIsReadyForExecution() {
        return true;
    }

    @Override
    protected void treatDependencyFreeAction(List<AllocatableAction> freeTasks) {
        
    }
    
    @Override
    public boolean checkIfCanceled(AllocatableAction aa) {
        return false;
    }
}
