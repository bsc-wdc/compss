package es.bsc.compss.scheduler.types.allocatableactions;

import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.ServiceImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource.Type;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;
import es.bsc.compss.types.resources.updates.PendingReduction;
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
    	removeResource();
    }

    private void removeResource() {
    	Worker<? extends WorkerResourceDescription> w = worker.getResource();
    	if (w instanceof CloudMethodWorker){
    		CloudMethodWorker cmw = (CloudMethodWorker)w ;
    		ResourceManager.terminateCloudResource(cmw, (CloudMethodResourceDescription) ru.getModification());
    	}else{
    		ResourceManager.removeWorker(w);
    	}
	}

	@Override
    protected void doError() throws FailedActionException {
        throw new FailedActionException();
    }

    @Override
    protected void doFailed() {
        removeResource();
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
    
    @Override
    public void tryToSchedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        this.schedule(actionScore);
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
