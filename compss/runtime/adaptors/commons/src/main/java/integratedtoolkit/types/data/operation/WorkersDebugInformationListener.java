package integratedtoolkit.types.data.operation;

import integratedtoolkit.log.Loggers;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;


public class WorkersDebugInformationListener extends DataOperation.EventListener {
    protected static final Logger logger = Logger.getLogger(Loggers.JM_COMP);

    int operation = 0;
    int errors = 0;
    boolean enabled = false;

    final Semaphore sem;

    public WorkersDebugInformationListener(Semaphore sem) {
        logger.debug("Init with semaphore " + sem.toString());
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
        logger.debug("enabled: " + enabled + ", finished: "+ finished + ", failed: " + failed);

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
        logger.debug("adding OP (" + operation + ")");
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
