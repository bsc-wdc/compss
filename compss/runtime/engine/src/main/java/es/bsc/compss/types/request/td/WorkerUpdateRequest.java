package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.updates.ResourceUpdate;

/**
 * The AddCloudNodeRequest represents a request to add a new resource ready to
 * execute to the resource pool
 */
public class WorkerUpdateRequest<T extends WorkerResourceDescription> extends TDRequest {

    private final Worker<T> worker;
    private final ResourceUpdate<T> ru;

    /**
     * Constructs a AddCloudNodeRequest with all its parameters
     *
     * @param worker Worker that has been added
     * @param update
     *
     */
    public WorkerUpdateRequest(Worker<T> worker, ResourceUpdate<T> update) {
        this.worker = worker;
        this.ru = update;
    }

    public Worker<T> getWorker() {
        return worker;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        ts.updateWorker(worker, ru);
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.WORKER_UPDATE_REQUEST;
    }

}
