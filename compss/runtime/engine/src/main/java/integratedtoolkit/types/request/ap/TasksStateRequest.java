package integratedtoolkit.types.request.ap;

import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.DataInfoProvider;
import integratedtoolkit.components.impl.TaskAnalyser;
import integratedtoolkit.components.impl.TaskDispatcher;
import java.util.concurrent.Semaphore;


/**
 * The TasksStateRequests class represents a request to obtain the progress of all the applications that are running
 */
public class TasksStateRequest extends APRequest {

    /**
     * Semaphore where to synchronize until the operation is done
     */
    private Semaphore sem;
    /**
     * Applications progress description
     */
    private String response;


    /**
     * Constructs a new TaskStateRequest
     *
     * @param sem
     *            semaphore where to synchronize until the current state is described
     */
    public TasksStateRequest(Semaphore sem) {
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

    /**
     * Returns the progress description in an xml format string
     *
     * @return progress description in an xml format string
     */
    public String getResponse() {
        return response;
    }

    /**
     * Sets the current state description
     *
     * @param response
     *            current state description
     */
    public void setResponse(String response) {
        this.response = response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher<?, ?, ?> td) {
        response = ta.getTaskStateRequest();
        sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.TASKSTATE;
    }

}
