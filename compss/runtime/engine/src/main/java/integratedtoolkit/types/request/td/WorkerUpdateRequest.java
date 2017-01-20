package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * The AddCloudNodeRequest represents a request to add a new resource ready to execute to the resource pool
 */
public class WorkerUpdateRequest<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends TDRequest<P, T, I> {

    private final Worker<T, I> worker;


    /**
     * Constructs a AddCloudNodeRequest with all its parameters
     *
     * @param worker
     *            Worker that has been added
     *
     */
    public WorkerUpdateRequest(Worker<T, I> worker) {
        this.worker = worker;
    }

    public Worker<T, I> getWorker() {
        return worker;
    }

    @Override
    public void process(TaskScheduler<P, T, I> ts) throws ShutdownException {
        ts.updatedWorker(worker);
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.WORKER_UPDATE_REQUEST;
    }

}
