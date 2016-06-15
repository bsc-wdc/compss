package integratedtoolkit.types.allocatableactions;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.util.ResourceScheduler;
import java.util.LinkedList;


public class StartWorkerAction<T extends WorkerResourceDescription> extends AllocatableAction<Profile, T> {

    private final ResourceScheduler<Profile, T> worker;
    private final TaskScheduler ts;

    public StartWorkerAction(SchedulingInformation schedulingInformation, ResourceScheduler<Profile, T> worker, TaskScheduler ts) {
        super(schedulingInformation);
        this.worker = worker;
        this.ts = ts;
    }

    @Override
    protected boolean areEnoughResources() {
        Worker w = selectedResource.getResource();
        return w.canRunNow(w.getDescription());
    }

    @Override
    protected void reserveResources() {
        Worker w = selectedResource.getResource();
        w.runTask(w.getDescription());
    }

    @Override
    protected void releaseResources() {
        Worker w = selectedResource.getResource();
        w.endTask(w.getDescription());
    }

    @Override
    protected void doAction() {
        (new Thread() {
            public void run() {
                Thread.currentThread().setName(selectedResource.getResource().getName() + " starter");
                try {
                    selectedResource.getResource().start();
                    notifyCompleted();
                } catch (Exception e) {
                    logger.error("Error starting resource", e);
                    ErrorManager.warn("Exception creating worker. Check runtime.log for more details", e);
                    notifyError();
                }
            }
        }).start();
    }

    @Override
    protected void doCompleted() {
        logger.error("Worker " + worker.getName() + " is ready to execute tasks.");
    }

    @Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        Worker wNode = worker.getResource();

        //Remove from the pool
        ResourceManager.removeWorker(wNode);

        //Remove all resources assigned to the node
        ResourceDescription rd = wNode.getDescription();
        rd.reduce(rd);

        //Update the CE and Implementations that can run (none)
        worker.getResource().updatedFeatures();

        ts.updatedWorker(wNode);
    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers() {
        LinkedList<ResourceScheduler<?, ?>> workers = new LinkedList<ResourceScheduler<?, ?>>();
        workers.add(worker);
        return workers;
    }

    @Override
    public Implementation<T>[] getImplementations() {
        return new Implementation[0];
    }

    @Override
    public boolean isCompatible(Worker<T> r) {
        return (r == worker.getResource());
    }

    @Override
    public LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<Profile, T> r) {
        LinkedList<Implementation<T>> impls = new LinkedList<Implementation<T>>();
        return impls;
    }

    @Override
    public Score schedulingScore(TaskScheduler ts) {
        return null;
    }

    @Override
    public Score schedulingScore(ResourceScheduler<Profile, T> targetWorker, Score actionScore) {
        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.selectedResource = worker;
        worker.initialSchedule(this, null);
    }

    @Override
    public void schedule(ResourceScheduler<Profile, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.selectedResource = targetWorker;
        targetWorker.initialSchedule(this, null);
    }

    public String toString() {
        return "StartWorkerAction for worker " + worker.getName();
    }
}
