package integratedtoolkit.types.data.operation;

import integratedtoolkit.log.Loggers;
import java.util.concurrent.Semaphore;
import org.apache.log4j.Logger;


public class OneOpWithSemListener extends DataOperation.EventListener {

    private static final Logger logger = Logger.getLogger(Loggers.FTM_COMP);
    private static final boolean debug = logger.isDebugEnabled();

    private Semaphore sem;

    public OneOpWithSemListener(Semaphore sem) {
        this.sem = sem;
    }

    @Override
    public void notifyEnd(DataOperation fOp) {
        sem.release();
    }

    @Override
    public void notifyFailure(DataOperation fOp, Exception e) {
        if (debug) {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is OPEN_FILE"
                    + ", operation end state is FAILED",
                    e);
        } else {
            logger.error("THREAD " + Thread.currentThread().getName() + " File Operation failed on " + fOp.getName()
                    + ", file role is OPEN_FILE"
                    + ", operation end state is FAILED");
        }

        sem.release();

    }
}
