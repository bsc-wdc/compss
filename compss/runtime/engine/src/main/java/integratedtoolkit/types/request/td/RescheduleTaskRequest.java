package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.request.exceptions.ShutdownException;
import integratedtoolkit.types.resources.Worker;


/**
 * The RefuseCloudWorkerRequest class represents the notification of an error
 * during a task execution that must be rescheduled in another resource.
 */
public class RescheduleTaskRequest extends TDRequest {

    /**
     * Task that must be rescheduled
     */
    private Task task;
    /**
     * Implementation that has been executed
     */
    private int implementationId;
    /**
     * Resource where the task has been submitted
     */
    private Worker<?> resource;

    /**
     * Constructs a new RescheduleTaskRequest for the task task
     *
     * @param task Task that must be rescheduled
     * @param implId Implementation that has been executed
     * @param resource Resource where the task has been submitted
     */
    public RescheduleTaskRequest(Task task, int implId, Worker<?> resource) {
        this.task = task;
        this.implementationId = implId;
        this.resource = resource;
    }

    /**
     * Returns the task that must be rescheduled
     *
     * @return Task that must be rescheduled
     */
    public Task getTask() {
        return task;
    }

    /**
     * Sets the task that must be rescheduled
     *
     * @param task Task that must be rescheduled
     */
    public void setTask(Task task) {
        this.task = task;
    }

    /**
     * Returns the id of implementation that has been executed
     *
     * @return Id of the implementation that has been executed
     */
    public int getImplementationId() {
        return implementationId;
    }

    /**
     * Sets the implementation that has been executed
     *
     * @param implId Id of the implementation that has been executed
     */
    public void setImplementationId(int implId) {
        this.implementationId = implId;
    }

    /**
     * Gets the resource where the task has been submitted
     *
     * @return name of the resource where the task has been submitted
     */
    public Worker<?> getResource() {
        return resource;
    }

    /**
     * Sets the resource where the task has been submitted
     *
     * @param resource Resource where the task has been submitted
     */
    public void setResource(Worker<?> resource) {
        this.resource = resource;
    }

    @Override
    public TDRequestType getRequestType() {
        return TDRequestType.RESCHEDULE_TASK;
    }

    @Override
    public void process(TaskScheduler ts, JobManager jm) throws ShutdownException {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
