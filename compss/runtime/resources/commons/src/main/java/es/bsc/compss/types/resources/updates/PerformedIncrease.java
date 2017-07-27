package es.bsc.compss.types.resources.updates;

import es.bsc.compss.types.resources.WorkerResourceDescription;


public class PerformedIncrease<T extends WorkerResourceDescription> extends ResourceUpdate<T> {

    public PerformedIncrease(T increase) {
        super(increase);
    }

    @Override
    public Type getType() {
        return Type.INCREASE;
    }

    @Override
    public boolean checkCompleted() {
        return true;
    }

    @Override
    public void waitForCompletion() throws InterruptedException {
        // Do nothing. Already completed
    }

    public void notifyCompletion() {
        // Do nothing. Already completed.
    }
}
