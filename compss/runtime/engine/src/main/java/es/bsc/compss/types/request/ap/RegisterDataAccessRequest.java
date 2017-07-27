package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.AccessParams;
import java.util.concurrent.Semaphore;

import es.bsc.compss.types.data.DataAccessId;

public class RegisterDataAccessRequest extends APRequest {

    private AccessParams access;
    private Semaphore sem;
    private DataAccessId response;

    public RegisterDataAccessRequest(AccessParams access, Semaphore sem) {
        this.access = access;
        this.sem = sem;
    }

    public AccessParams getAccess() {
        return access;
    }

    public void setAccess(AccessParams access) {
        this.access = access;
    }

    public Semaphore getSemaphore() {
        return sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    public DataAccessId getResponse() {
        return response;
    }

    public void setResponse(DataAccessId response) {
        this.response = response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        DataAccessId daId = dip.registerDataAccess(this.access);
        this.response = daId;
        sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.REGISTER_DATA_ACCESS;
    }

}
