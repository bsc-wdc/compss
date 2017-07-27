package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;

import java.util.concurrent.Semaphore;

public class BarrierRequest extends APRequest {

    private Semaphore sem;
    private Long appId;

    public BarrierRequest(Long appId, Semaphore sem) {
        this.appId = appId;
        this.sem = sem;
    }

    public Long getAppId() {
        return this.appId;
    }

    public Semaphore getSemaphore() {
        return this.sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.barrier(this);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.WAIT_FOR_ALL_TASKS;
    }

}
