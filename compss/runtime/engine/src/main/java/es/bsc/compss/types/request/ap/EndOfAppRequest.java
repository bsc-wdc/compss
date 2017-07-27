package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import java.util.concurrent.Semaphore;

public class EndOfAppRequest extends APRequest {

    private Long appId;
    private Semaphore sem;

    public EndOfAppRequest(Long appId, Semaphore sem) {
        this.appId = appId;
        this.sem = sem;
    }

    public Long getAppId() {
        return appId;
    }

    public void setAppId(Long appId) {
        this.appId = appId;
    }

    public Semaphore getSemaphore() {
        return sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        ta.noMoreTasks(this);
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.END_OF_APP;
    }

}
