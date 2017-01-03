package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;


/**
 * The AddCloudNodeRequest represents a request to add a new resource ready to execute to the resource pool
 */
public class WorkerUpdateRequest<P extends Profile, T extends WorkerResourceDescription> extends TDRequest<P, T> {

    private final Worker<T> worker;


    /**
     * Constructs a AddCloudNodeRequest with all its parameters
     *
     * @param worker
     *            Worker that has been added
     *
     */
    public WorkerUpdateRequest(Worker<T> worker) {
        this.worker = worker;
    }

    public Worker<T> getWorker() {
        return worker;
    }

    @Override
    public void process(TaskScheduler<P, T> ts) {
        ts.updatedWorker(worker);
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.WORKER_UPDATE_REQUEST;
    }

}
