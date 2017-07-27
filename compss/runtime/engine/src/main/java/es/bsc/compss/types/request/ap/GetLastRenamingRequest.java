package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import java.util.concurrent.Semaphore;

public class GetLastRenamingRequest extends APRequest {

    private int code;
    private Semaphore sem;
    private String response;

    public GetLastRenamingRequest(int code, Semaphore sem) {
        this.code = code;
        this.sem = sem;
    }

    public int getCode() {
        return code;
    }

    public void setCode(int code) {
        this.code = code;
    }

    public Semaphore getSemaphore() {
        return sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    public String getResponse() {
        return response;
    }

    public void setResponse(String response) {
        this.response = response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        String renaming = dip.getLastRenaming(this.code);
        response = renaming;
        sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.GET_LAST_RENAMING;
    }

}
