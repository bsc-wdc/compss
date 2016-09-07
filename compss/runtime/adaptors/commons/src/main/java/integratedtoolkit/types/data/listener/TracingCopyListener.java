package integratedtoolkit.types.data.listener;

import integratedtoolkit.types.data.operation.DataOperation;

import java.util.concurrent.Semaphore;


public class TracingCopyListener extends EventListener {

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private final Semaphore sem;


    public TracingCopyListener(Semaphore sem) {
        this.sem = sem;
    }

    public void enable() {
        boolean finished;
        boolean failed;
        synchronized (this) {
            enabled = true;
            finished = operation == 0;
            failed = errors > 0;
        }

        if (finished) {
            if (failed) {
                doFailures();
            } else {
                doReady();
            }
        }
    }

    public synchronized void addOperation() {
        operation++;
    }

    @Override
    public void notifyEnd(DataOperation fOp) {
        boolean enabled;
        boolean finished;
        boolean failed;
        synchronized (this) {
            operation--;
            finished = operation == 0;
            failed = errors > 0;
            enabled = this.enabled;
        }
        if (finished && enabled) {
            if (failed) {
                doFailures();
            } else {
                doReady();
            }
        }
    }

    @Override
    public void notifyFailure(DataOperation fOp, Exception e) {
        boolean enabled;
        boolean finished;
        synchronized (this) {
            errors++;
            operation--;
            finished = operation == 0;
            enabled = this.enabled;
        }
        if (enabled && finished) {
            doFailures();
        }
    }

    private void doReady() {
        sem.release();
    }

    private void doFailures() {
        sem.release();
    }

}
