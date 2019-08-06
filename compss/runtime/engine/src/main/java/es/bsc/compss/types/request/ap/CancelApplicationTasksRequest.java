package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.worker.COMPSsException;
import java.util.concurrent.Semaphore;


public class CancelApplicationTasksRequest extends APRequest {

    private Long appId;
    private Semaphore sem;


    /**
     * Creates a request to cancel all tasks of an application.
     * 
     * @param appId Application Id.
     */
    public CancelApplicationTasksRequest(Long appId, Semaphore sem) {
        this.appId = appId;
        this.sem = sem;
    }

    /**
     * Returns the waiting semaphore.
     * 
     * @return The waiting semaphore.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.CANCEL_ALL_TASKS;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
        throws ShutdownException, COMPSsException {
        ta.cancelApplicationTasks(this);
    }

    public Long getAppId() {
        return appId;
    }
}
