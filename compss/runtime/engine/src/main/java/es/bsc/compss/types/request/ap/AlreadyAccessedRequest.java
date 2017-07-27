package es.bsc.compss.types.request.ap;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.location.DataLocation;
import java.util.concurrent.Semaphore;

public class AlreadyAccessedRequest extends APRequest {

    private final DataLocation loc;

    private Semaphore sem;

    private boolean response;

    public AlreadyAccessedRequest(DataLocation loc, Semaphore sem) {
        this.loc = loc;
        this.sem = sem;
    }

    public DataLocation getLocation() {
        return loc;
    }

    public Semaphore getSemaphore() {
        return sem;
    }

    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    public boolean getResponse() {
        return response;
    }

    public void setResponse(boolean response) {
        this.response = response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        boolean aa = dip.alreadyAccessed(this.loc);
        this.response = aa;
        sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.ALREADY_ACCESSED;
    }

}
