package integratedtoolkit.types.request.td;

import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;

import java.util.concurrent.Semaphore;


/**
 * The DeleteIntermediateFilesRequest represents a request to delete the intermediate files of the execution from all
 * the worker nodes of the resource pool.
 */
public class GetCurrentScheduleRequest<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends TDRequest<P, T, I> {

    /**
     * Current Schedule representation
     */
    private WorkloadStatus response;
    /**
     * Semaphore to synchronize until the representation is constructed
     */
    private Semaphore sem;


    /**
     * Constructs a GetCurrentScheduleRequest
     *
     * @param sem
     *            Semaphore to synchronize until the representation is constructed
     *
     */
    public GetCurrentScheduleRequest(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the current schedule representation
     *
     * @result current schedule representation
     *
     */
    public WorkloadStatus getResponse() {
        return response;
    }

    /**
     * Returns the semaphore to synchronize until the representation is constructed
     *
     * @result Semaphore to synchronize until the representation is constructed
     *
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Changes the semaphore to synchronize until the representation is constructed
     *
     * @param sem
     *            New semaphore to synchronize until the representation is constructed
     *
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void process(TaskScheduler<P, T, I> ts) throws ShutdownException {
        response = new WorkloadStatus(CoreManager.getCoreCount());
        ts.getWorkloadState(response);
        sem.release();
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.GET_CURRENT_SCHEDULE;
    }

}
