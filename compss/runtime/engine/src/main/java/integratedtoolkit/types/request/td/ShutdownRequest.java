package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.util.JobDispatcher;
import integratedtoolkit.util.ResourceManager;

import java.util.concurrent.Semaphore;

/**
 * This class represents a notification to end the execution
 */
public class ShutdownRequest extends TDRequest {

    /**
     * Semaphore where to synchronize until the operation is done
     */
    private Semaphore semaphore;

    /**
     * Constructs a new ShutdownRequest
     *
     * @param sem
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
     * Sets the semaphore where to synchronize until the requested object can be
     * read
     *
     * @param sem the semaphore where to synchronize until the requested object
     * can be read
     */
    public void setSemaphore(Semaphore sem) {
        this.semaphore = sem;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        LOGGER.debug("Processing ShutdownRequest request...");

        // Shutdown Job Dispatcher
        JobDispatcher.shutdown();

        // Shutdown TaskScheduler
        ts.shutdown();

        // Print core state
        ResourceManager.stopNodes();

        // The semaphore is released after emitting the end event to prevent race conditions
        throw new ShutdownException(semaphore);
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.TD_SHUTDOWN;
    }

}
