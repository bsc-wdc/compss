package es.bsc.compss.comm;

import es.bsc.compss.exceptions.FileDeletionException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.operation.Delete;
import es.bsc.compss.types.data.operation.copy.DeferredCopy;
import es.bsc.compss.types.data.operation.copy.ImmediateCopy;
import es.bsc.compss.util.RequestDispatcher;
import es.bsc.compss.util.RequestQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Dispatcher extends RequestDispatcher<DataOperation> {

    // Log and debug
    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
    public static final boolean debug = logger.isDebugEnabled();


    public Dispatcher(RequestQueue<DataOperation> queue) {
        super(queue);
    }

    public void processRequests() {
        DataOperation fOp;
        while (true) {
            fOp = queue.dequeue();
            if (fOp == null) {
                break;
            }
            // What kind of operation is requested?
            if (fOp instanceof ImmediateCopy) { // File transfer (copy)
                ImmediateCopy c = (ImmediateCopy) fOp;
                c.perform();
            } else if (fOp instanceof DeferredCopy) {
                // DO nothing

            } else { // fOp instanceof Delete
                Delete d = (Delete) fOp;
                performOperation(d);
            }
        }
    }

    public static void performOperation(Delete d) {
        logger.debug("THREAD " + Thread.currentThread().getName() + " Delete " + d.getFile());
        try {
            if (!d.getFile().delete()) {
                FileDeletionException fde = new FileDeletionException("Error performing delete file");
                d.end(DataOperation.OpEndState.OP_FAILED, fde);
                return;
            }
        } catch (SecurityException e) {
            d.end(DataOperation.OpEndState.OP_FAILED, e);
            return;
        }
        d.end(DataOperation.OpEndState.OP_OK);
    }

}
