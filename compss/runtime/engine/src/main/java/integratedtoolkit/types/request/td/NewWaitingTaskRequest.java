package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.request.exceptions.ShutdownException;


/**
 * The NewWaitingTaskRequest represents a notification about a task that will be
 * dependency-free when some of the task already submitted to the TaskDispathcer
 * end.
 */
public class NewWaitingTaskRequest extends TDRequest {

    /**
     * blocked task method id
     */
    private int coreId;

    /**
     * Contructs a NewWaitingTaskRequest
     *
     * @param coreId core id of the blocked task
     */
    public NewWaitingTaskRequest(int coreId) {
        this.coreId = coreId;
    }

    /**
     * Returns the core Id of the blocked task
     *
     * @return core Id of the blocked task
     */
    public int getMethodId() {
        return coreId;
    }

    /**
     * Sets the core Id of the blocked task
     *
     * @param coreId core Id of the blocked task
     */
    public void setMethodId(int coreId) {
        this.coreId = coreId;
    }

    @Override
    public TDRequestType getRequestType() {
        return TDRequestType.NEW_WAITING_TASK;
    }

    @Override
    public void process(TaskScheduler ts, JobManager jm) throws ShutdownException {
        
    }
}
