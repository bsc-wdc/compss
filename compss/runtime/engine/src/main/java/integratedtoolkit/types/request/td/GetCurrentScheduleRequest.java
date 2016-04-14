package integratedtoolkit.types.request.td;

import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.util.CoreManager;

import java.util.concurrent.Semaphore;


/**
 * The DeleteIntermediateFilesRequest represents a request to delete the
 * intermediate files of the execution from all the worker nodes of the resource
 * pool.
 */
public class GetCurrentScheduleRequest extends TDRequest {

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
     * @param sem Semaphore to synchronize until the representation is
     * constructed
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
     * Returns the semaphore to synchronize until the representation is
     * constructed
     *
     * @result Semaphore to synchronize until the representation is constructed
     *
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Changes the semaphore to synchronize until the representation is
     * constructed
     *
     * @param sem New semaphore to synchronize until the representation is
     * constructed
     *
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public TDRequestType getRequestType() {
        return TDRequestType.GET_STATE;
    }

    @Override
    public void process(TaskScheduler ts, JobManager jm) throws ShutdownException {
        response = new WorkloadStatus(CoreManager.getCoreCount());
        ts.getWorkloadState(response);
        sem.release();
    }
}
