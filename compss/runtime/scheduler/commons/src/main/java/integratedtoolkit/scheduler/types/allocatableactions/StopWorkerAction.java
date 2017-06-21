package integratedtoolkit.scheduler.types.allocatableactions;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.ServiceImplementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource.Type;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.updates.ResourceUpdate;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.resources.updates.PendingReduction;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;


public class StopWorkerAction extends AllocatableAction {

    private final ResourceScheduler<? extends WorkerResourceDescription> worker;
    private final Implementation impl;
    private final PendingReduction<WorkerResourceDescription> ru;


    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */
    @SuppressWarnings("unchecked")
    public StopWorkerAction(SchedulingInformation schedulingInformation, ResourceScheduler<? extends WorkerResourceDescription> worker,
            TaskScheduler ts, ResourceUpdate<? extends WorkerResourceDescription> modification) {

        super(schedulingInformation, ts.getOrchestrator());
        this.worker = worker;
        this.ru = (PendingReduction<WorkerResourceDescription>) modification;
        if (worker.getResource().getType() == Type.WORKER) {
            impl = new MethodImplementation("", "", null, null, new MethodResourceDescription());
        } else {
            impl = new ServiceImplementation(null, "", "", "", "");
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION AND LIFECYCLE MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public boolean isToReserveResources() {
        return false;
    }

    @Override
    public boolean isToReleaseResources() {
        return false;
    }

    @Override
    protected void doAction() {
        (new Thread() {

            @SuppressWarnings("unchecked")
            @Override
            public void run() {
                Worker<WorkerResourceDescription> wResource = (Worker<WorkerResourceDescription>) worker.getResource();
                Thread.currentThread().setName(wResource.getName() + " stopper");
                wResource.retrieveData(true);
                Semaphore sem = new Semaphore(0);
                ShutdownListener sl = new ShutdownListener(sem);
                wResource.stop(sl);

                sl.enable();
                try {
                    sem.acquire();
                } catch (Exception e) {
                    LOGGER.error("ERROR: Exception raised on worker shutdown", e);
                    ErrorManager.warn("Exception stopping worker. Check runtime.log for more details", e);
                    notifyError();
                }
                notifyCompleted();

            }
        }).start();
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */
    @Override
    protected void doCompleted() {
        CloudMethodWorker cmw = (CloudMethodWorker) worker.getResource();
        ResourceManager.terminateResource(cmw, (CloudMethodResourceDescription) ru.getModification());
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        CloudMethodWorker cmw = (CloudMethodWorker) worker.getResource();
        ResourceManager.terminateResource(cmw, (CloudMethodResourceDescription) ru.getModification());
    }

    /*
     * ***************************************************************************************************************
     * SCHEDULING MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> workers = new LinkedList<>();
        workers.add(worker);
        return workers;
    }

    @Override
    public Implementation[] getImplementations() {
        Implementation[] impls = new Implementation[1];
        impls[0] = impl;
        return impls;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return (r == worker.getResource());
    }

    @Override
    public <T extends WorkerResourceDescription> LinkedList<Implementation> getCompatibleImplementations(ResourceScheduler<T> r) {
        LinkedList<Implementation> impls = new LinkedList<>();
        if (r == worker) {
            impls.add(impl);
        }
        return impls;
    }

    @Override
    public <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker, Score actionScore) {
        return null;
    }

    @SuppressWarnings("unchecked")
    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        schedule((ResourceScheduler<WorkerResourceDescription>) worker, impl);
    }

    @SuppressWarnings("unchecked")
    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {
        schedule((ResourceScheduler<WorkerResourceDescription>) targetWorker, impl);
    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {
        if (targetWorker != getEnforcedTargetResource()) {
            throw new UnassignedActionException();
        }
        // WARN: Parameter impl is ignored
        assignResource(targetWorker);
        assignImplementation(impl);
        targetWorker.scheduleAction(this);
    }

    @Override
    public String toString() {
        return "StopWorkerAction (Worker " + this.worker.getName() + ")";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }
}
