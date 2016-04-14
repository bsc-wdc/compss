package integratedtoolkit.types.data.operation;

import integratedtoolkit.types.job.Job;
import integratedtoolkit.components.impl.JobManager;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Task;
import integratedtoolkit.types.resources.Worker;
import org.apache.log4j.Logger;

public class JobTransfersListener extends DataOperation.EventListener {

    private int operation = 0;
    private int errors = 0;
    private boolean enabled = false;

    private static final Logger logger = Logger.getLogger(Loggers.FTM_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    private Job<?> job;
    private Worker<?> res;
    private Task task;
    private JobManager JM;

    public JobTransfersListener(Job<?> job, Task task, Worker<?> res, JobManager jm) {
        this.job = job;
        this.res = res;
        this.JM = jm;
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
        e.printStackTrace();
        if (debug) {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is JOB_FILE"
                    + ", operation end state is FAILED",
                    e);
        } else {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is JOB_FILE"
                    + ", operation end state is FAILED");
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
        JM.submitJob(job, res);

    }

    private void doFailures() {
        JM.failedTransfers(job, task, errors, res);
    }
}
