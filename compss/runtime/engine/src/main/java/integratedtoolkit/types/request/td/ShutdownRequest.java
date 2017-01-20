package integratedtoolkit.types.request.td;

import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.JobDispatcher;
import integratedtoolkit.util.ResourceManager;

import java.util.concurrent.Semaphore;


/**
 * This class represents a notification to end the execution
 */
public class ShutdownRequest<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends TDRequest<P, T, I> {

    /**
     * Semaphore where to synchronize until the operation is done
     */
    private Semaphore semaphore;


    /**
     * Constructs a new ShutdownRequest
     *
     */
    public ShutdownRequest(Semaphore sem) {
        this.semaphore = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the object can be read
     *
     * @return the semaphore where to synchronize until the object can be read
     */
    public Semaphore getSemaphore() {
        return semaphore;
    }

    /**
     * Sets the semaphore where to synchronize until the requested object can be read
     *
     * @param sem
     *            the semaphore where to synchronize until the requested object can be read
     */
    public void setSemaphore(Semaphore sem) {
        this.semaphore = sem;
    }

    @Override
    public void process(TaskScheduler<P, T, I> ts) throws ShutdownException {
        logger.debug("Processing ShutdownRequest request...");

        // Shutdown Job Dispatcher
        JobDispatcher.shutdown();
        ;

        // Shutdown TaskScheduler
        ts.shutdown();

        // Print core state
        WorkloadStatus status = new WorkloadStatus(CoreManager.getCoreCount());
        ts.getWorkloadState(status);
        ResourceManager.stopNodes(status);
        // The semaphore is released after emitting the end event to prevent race conditions
        throw new ShutdownException(semaphore);
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.TD_SHUTDOWN;
    }

}
