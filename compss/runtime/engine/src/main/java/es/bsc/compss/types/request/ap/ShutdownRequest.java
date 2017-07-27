package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.request.exceptions.ShutdownException;

import java.util.concurrent.Semaphore;

public class ShutdownRequest extends APRequest {

    private Semaphore sem;

    public ShutdownRequest(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the object can be read
     *
     * @return the semaphore where to synchronize until the object can be read
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Sets the semaphore where to synchronize until the requested object can be
     * read
     *
     * @param sem the semaphore where to synchronize until the requested object
     * can be read
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) throws ShutdownException {
        // Close Graph
        ta.shutdown();
        // Clear delete Intermediate Files
        dip.shutdown();

        // The semaphore is released after emitting the end event to prevent race conditions
        throw new ShutdownException(sem);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.SHUTDOWN;
    }

}
