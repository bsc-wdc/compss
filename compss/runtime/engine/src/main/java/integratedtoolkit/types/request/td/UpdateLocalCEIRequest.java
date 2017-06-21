package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.util.CEIParser;
import integratedtoolkit.util.ResourceManager;

import java.util.concurrent.Semaphore;
import java.util.List;

public class UpdateLocalCEIRequest extends TDRequest {

    private final Class<?> ceiClass;
    private final Semaphore sem;

    public UpdateLocalCEIRequest(Class<?> ceiClass, Semaphore sem) {
        this.ceiClass = ceiClass;
        this.sem = sem;
    }

    /**
     * Returns the CoreElement Interface class
     *
     * @return
     */
    public Class<?> getCeiClass() {
        return this.ceiClass;
    }

    /**
     * Returns the semaphore where to synchronize until the operation is done
     *
     * @return Semaphore where to synchronize until the operation is done
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        LOGGER.debug("Treating request to update core elements");

        // Load new coreElements
        List<Integer> newCores = CEIParser.loadJava(this.ceiClass);
        if (DEBUG) {
            LOGGER.debug("New methods: " + newCores);
        }
        // Update Resources structures
        ResourceManager.coreElementUpdates(newCores);
        // Update Scheduler structures
        ts.coreElementsUpdated();

        // Release
        LOGGER.debug("Data structures resized and CE-resources links updated");
        sem.release();
    }

    @Override
    public TDRequestType getType() {
        return TDRequestType.UPDATE_CEI_LOCAL;
    }

}
