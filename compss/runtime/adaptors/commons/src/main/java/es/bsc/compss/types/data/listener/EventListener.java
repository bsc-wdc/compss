package es.bsc.compss.types.data.listener;

import es.bsc.compss.types.data.operation.DataOperation;
import java.util.concurrent.atomic.AtomicInteger;


public abstract class EventListener {

    private static final AtomicInteger nextId = new AtomicInteger(0);
    private final int id = nextId.getAndIncrement();


    public Integer getId() {
        return id;
    }

    public abstract void notifyEnd(DataOperation fOp);

    public abstract void notifyFailure(DataOperation fOp, Exception e);

}
