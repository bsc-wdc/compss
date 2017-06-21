package integratedtoolkit.scheduler.types.allocatableactions;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.ServiceImplementation;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource.Type;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.resources.updates.PendingReduction;
import integratedtoolkit.types.resources.updates.ResourceUpdate;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;

import java.util.LinkedList;

public class ReduceWorkerAction<T extends WorkerResourceDescription> extends AllocatableAction {

    private final ResourceScheduler<T> worker;
    private final TaskScheduler ts;
    private final Implementation impl;
    private final PendingReduction<T> ru;

    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */
    @SuppressWarnings("unchecked")
    public ReduceWorkerAction(
            SchedulingInformation schedulingInformation,
            ResourceScheduler<T> worker,
            TaskScheduler ts,
            ResourceUpdate<T> modification
    ) {
        super(schedulingInformation, ts.getOrchestrator());
        this.worker = worker;
        this.ts = ts;
        this.ru = (PendingReduction<T>) modification;
        if (worker.getResource().getType() == Type.WORKER) {
            impl = new MethodImplementation("", "", null, null, (MethodResourceDescription) modification.getModification());
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
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
        return false;
    }

    @Override
    protected void doAction() {
        (new Thread() {
            @Override
            public void run() {
                Thread.currentThread().setName(worker.getName() + " stopper");
                CloudMethodWorker w = (CloudMethodWorker) worker.getResource();
                PendingReduction<WorkerResourceDescription> crd = (PendingReduction<WorkerResourceDescription>) ru;
                ResourceManager.reduceResource(w, crd);
                w.endTask((MethodResourceDescription) getResourceConsumption());
                try {
                    ru.waitForCompletion();
                } catch (Exception e) {
                    LOGGER.error("ERROR: Exception raised on worker reduction", e);
                    ErrorManager.warn("Exception reducing worker. Check runtime.log for more details", e);
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
        ts.completedResourceUpdate(worker, ru);
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        LOGGER.error("Error waiting for tasks to end");
        ts.completedResourceUpdate(worker, ru);
    }

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
        Implementation[] impls = new Implementation[]{impl};
        return impls;
    }

    @Override
    public <T extends WorkerResourceDescription> boolean isCompatible(Worker<T> r) {
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

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        schedule((ResourceScheduler<WorkerResourceDescription>) worker, impl);
    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        schedule((ResourceScheduler< WorkerResourceDescription>) targetWorker, impl);
    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Implementation impl) throws BlockedActionException, UnassignedActionException {
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
        return "ReduceWorkerAction (Worker " + this.worker.getName() + ")";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

}
