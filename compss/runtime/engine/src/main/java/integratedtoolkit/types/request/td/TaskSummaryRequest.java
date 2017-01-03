package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.Logger;


/**
 * The MonitoringDataRequest class represents a request to obtain the current resources and cores that can be run
 */
public class TaskSummaryRequest<P extends Profile, T extends WorkerResourceDescription> extends TDRequest<P, T> {

    /**
     * Semaphore where to synchronize until the operation is done
     */
    private Semaphore sem;
    
    /**
     * Logger where to print information
     */
    private final Logger logger;


    /**
     * Constructs a new TaskStateRequest
     *
     * @param sem
     *            semaphore where to synchronize until the current state is described
     */
    public TaskSummaryRequest(Logger logger, Semaphore sem) {
        this.logger = logger;
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the current state is described
     *
     * @return the semaphore where to synchronize until the current state is described
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Sets the semaphore where to synchronize until the current state is described
     *
     * @param sem
     *            the semaphore where to synchronize until the current state is described
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void process(TaskScheduler<P, T> ts) {
        ts.getTaskSummary(logger);
        sem.release();
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.MONITORING_DATA;
    }

}
