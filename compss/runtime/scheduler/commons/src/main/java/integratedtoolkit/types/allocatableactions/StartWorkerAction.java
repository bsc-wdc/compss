package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.implementations.ServiceImplementation;
import integratedtoolkit.types.resources.Resource.Type;
import integratedtoolkit.types.resources.MethodWorker;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.util.ResourceScheduler;

import java.util.LinkedList;


public class StartWorkerAction<P extends Profile, T extends WorkerResourceDescription> extends AllocatableAction<P, T> {

    private final ResourceScheduler<P, T> worker;
    private final TaskScheduler<P, T> ts;
    private final Implementation<T> impl;


    @SuppressWarnings("unchecked")
    public StartWorkerAction(SchedulingInformation<P, T> schedulingInformation, ResourceScheduler<P, T> worker, TaskScheduler<P, T> ts) {
        super(schedulingInformation);
        this.worker = worker;
        this.ts = ts;
        if (worker.getResource().getType() == Type.WORKER) {
            MethodWorker mw = (MethodWorker) worker.getResource();
            impl = (Implementation<T>) new MethodImplementation("", "", null, null, mw.getDescription());
        } else {
            impl = (Implementation<T>) new ServiceImplementation(null, "", "", "", "");
        }
    }

    @Override
    protected boolean areEnoughResources() {
        Worker<T> w = selectedResource.getResource();
        return w.canRunNow(w.getDescription());
    }

    @Override
    protected void reserveResources() {
        Worker<T> w = selectedResource.getResource();
        w.runTask(w.getDescription());
    }

    @Override
    protected void releaseResources() {
        Worker<T> w = selectedResource.getResource();
        w.endTask(w.getDescription());
    }

    @Override
    protected void doAction() {
        (new Thread() {
            
            @Override
            public void run() {
                Thread.currentThread().setName(selectedResource.getResource().getName() + " starter");
                try {
                    selectedResource.getResource().start();
                    notifyCompleted();
                } catch (InitNodeException e) {
                    logger.error("Error starting resource", e);
                    ErrorManager.warn("Exception creating worker. Check runtime.log for more details", e);
                    notifyError();
                }
            }
        }).start();
    }

    @Override
    protected void doCompleted() {
        logger.info("Worker " + worker.getName() + " is ready to execute tasks.");
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        Worker<T> wNode = worker.getResource();

        // Remove from the pool
        ResourceManager.removeWorker(wNode);

        // Remove all resources assigned to the node
        ResourceDescription rd = wNode.getDescription();
        rd.reduce(rd);

        // Update the CE and Implementations that can run (none)
        worker.getResource().updatedFeatures();

        ts.updatedWorker(wNode);
    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers() {
        LinkedList<ResourceScheduler<?, ?>> workers = new LinkedList<>();
        workers.add(worker);
        return workers;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Implementation<T>[] getImplementations() {
        Implementation<T>[] impls = new Implementation[] { impl };
        return impls;
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return (r == worker.getResource());
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r) {
        LinkedList<Implementation<T>> impls = new LinkedList<>();
        if (r == worker) {
            impls.add(impl);
        }
        return impls;
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore) {
        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.selectedResource = worker;
        assignImplementation(impl);
        worker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.selectedResource = targetWorker;
        assignImplementation(impl);
        targetWorker.initialSchedule(this);
    }

    @Override
    public void schedule(ResourceScheduler<P, T> targetWorker, Implementation<T> impl) 
            throws BlockedActionException, UnassignedActionException {
        
        this.selectedResource = targetWorker;
        assignImplementation(impl);
        targetWorker.initialSchedule(this);
    }

    @Override
    public String toString() {
        return "StartWorkerAction ( Worker " + worker.getName() + ")";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

}
