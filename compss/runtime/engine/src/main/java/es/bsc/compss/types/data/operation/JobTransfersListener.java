package es.bsc.compss.types.data.operation;

import es.bsc.compss.log.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.data.listener.EventListener;

public class JobTransfersListener extends EventListener {

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private static final Logger LOGGER = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private final ExecutionAction execution;

    public JobTransfersListener(ExecutionAction execution) {
        this.execution = execution;
    }

    public void enable() {
        boolean finished;
        boolean failed;
        synchronized (this) {
            enabled = true;
            finished = (operation == 0);
            failed = (errors > 0);
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
        if (DEBUG) {
            LOGGER.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is JOB_FILE, operation end state is FAILED", e);
        } else {
            LOGGER.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is JOB_FILE operation end state is FAILED");
        }

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
        execution.doSubmit(this.getId());
    }

    private void doFailures() {
        execution.failedTransfers(errors);
    }

}
