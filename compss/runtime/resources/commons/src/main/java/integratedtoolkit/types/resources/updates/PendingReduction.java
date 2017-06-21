package integratedtoolkit.types.resources.updates;

import integratedtoolkit.types.resources.WorkerResourceDescription;
import java.util.concurrent.Semaphore;

public class PendingReduction<T extends WorkerResourceDescription> extends ResourceUpdate<T> {

    private final Semaphore sem;

    public PendingReduction(T reduction) {
        super(reduction);
        this.sem = new Semaphore(0);
    }

    @Override
    public Type getType() {
        return Type.REDUCE;
    }

    @Override
    public boolean checkCompleted() {
        return sem.tryAcquire();
    }

    @Override
    public void waitForCompletion() throws InterruptedException {
        sem.acquire();
    }

    public void notifyCompletion() {
        sem.release();
    }
}
