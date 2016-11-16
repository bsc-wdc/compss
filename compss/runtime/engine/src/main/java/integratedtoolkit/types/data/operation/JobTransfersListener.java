package integratedtoolkit.types.data.operation;

import integratedtoolkit.log.Loggers;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.types.Profile;
import integratedtoolkit.types.allocatableactions.ExecutionAction;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.resources.WorkerResourceDescription;


public class JobTransfersListener<P extends Profile, T extends WorkerResourceDescription> extends EventListener {

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private static final Logger logger = LogManager.getLogger(Loggers.FTM_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    private ExecutionAction<P, T> execution;


    public JobTransfersListener(ExecutionAction<P, T> execution) {
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
        if (debug) {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is JOB_FILE, operation end state is FAILED", e);
        } else {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
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
