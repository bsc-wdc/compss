package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
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

import java.util.LinkedList;


public class StartWorkerAction<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends AllocatableAction<P, T, I> {

    private final TaskScheduler<P, T, I> ts;
    private final ResourceScheduler<P, T, I> worker;
    private final I impl;


    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */
    @SuppressWarnings("unchecked")
    public StartWorkerAction(SchedulingInformation<P, T, I> schedulingInformation, ResourceScheduler<P, T, I> worker,
            TaskScheduler<P, T, I> ts) {
        super(schedulingInformation, ts.getOrchestrator());
        this.worker = worker;
        this.ts = ts;
        if (worker.getResource().getType() == Type.WORKER) {
            MethodWorker mw = (MethodWorker) worker.getResource();
            this.impl = (I) new MethodImplementation("", "", null, null, mw.getDescription());
        } else {
            this.impl = (I) new ServiceImplementation(null, "", "", "", "");
        }
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION AND LIFECYCLE MANAGEMENT
     * ***************************************************************************************************************
     */
    @Override
    public boolean areEnoughResources() {
        Worker<T, I> w = selectedResource.getResource();
        return w.canRunNow(w.getDescription());
    }

    @Override
    protected void reserveResources() {
        Worker<T, I> w = selectedResource.getResource();
        w.runTask(w.getDescription());
    }

    @Override
    protected void releaseResources() {
        Worker<T, I> w = selectedResource.getResource();
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
                    LOGGER.error("Error starting resource", e);
                    ErrorManager.warn("Exception creating worker. Check runtime.log for more details", e);
                    notifyError();
                }
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
        LOGGER.info("Worker " + worker.getName() + " is ready to execute tasks.");
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        Worker<T, I> wNode = this.worker.getResource();

        // Remove from the pool
        ResourceManager.removeWorker(wNode);

        // Remove all resources assigned to the node
        ResourceDescription rd = wNode.getDescription();
        rd.reduce(rd);

        // Update the CE and Implementations that can run (none)
        this.worker.getResource().updatedFeatures();

        this.ts.updatedWorker(wNode);
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
    public LinkedList<ResourceScheduler<P, T, I>> getCompatibleWorkers() {
        LinkedList<ResourceScheduler<P, T, I>> workers = new LinkedList<>();
        workers.add(this.worker);
        return workers;
    }

    @Override
    public Implementation<?>[] getImplementations() {
        Implementation<?>[] impls = new Implementation[] { impl };
        return impls;
    }

    @Override
    public boolean isCompatible(Worker<T, I> r) {
        return (r == this.worker.getResource());
    }

    @Override
    public LinkedList<I> getCompatibleImplementations(ResourceScheduler<P, T, I> r) {
        LinkedList<I> impls = new LinkedList<>();
        if (r == this.worker) {
            impls.add(this.impl);
        }
        return impls;
    }

    @Override
    public Score schedulingScore(ResourceScheduler<P, T, I> targetWorker, Score actionScore) {
        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        schedule(this.worker, this.impl);
    }

    @Override
    public void schedule(ResourceScheduler<P, T, I> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {
        
        // WARN: targetWorker is ignored
        schedule(this.worker, this.impl);
    }

    @Override
    public void schedule(ResourceScheduler<P, T, I> targetWorker, I impl) throws BlockedActionException, UnassignedActionException {
        // WARN: Parameter targetWorker and parameter impl are ignored
        this.selectedResource = this.worker;
        assignImplementation(this.impl);

        this.worker.scheduleAction(this);
    }

    @Override
    public String toString() {
        return "StartWorkerAction (Worker " + this.worker.getName() + ")";
    }

    @Override
    public int getPriority() {
        return Integer.MAX_VALUE;
    }

}
