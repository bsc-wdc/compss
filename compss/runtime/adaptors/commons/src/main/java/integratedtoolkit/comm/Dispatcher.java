package integratedtoolkit.comm;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.data.operation.Copy.DeferredCopy;
import integratedtoolkit.types.data.operation.Copy.ImmediateCopy;
import integratedtoolkit.types.data.operation.Delete;
import integratedtoolkit.util.RequestDispatcher;
import integratedtoolkit.util.RequestQueue;
import org.apache.log4j.Logger;

public class Dispatcher extends RequestDispatcher<DataOperation> {

    // Log and debug
    protected static final Logger logger = Logger.getLogger(Loggers.COMM);
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
            if (fOp instanceof ImmediateCopy) { 		// File transfer (copy)
                ImmediateCopy c = (ImmediateCopy) fOp;
                c.perform();
            } else if (fOp instanceof DeferredCopy) {
                //DO nothing

            } else { // fOp instanceof Delete
                Delete d = (Delete) fOp;
                performOperation(d);
            }
        }
    }

    public static void performOperation(Delete d) {
        logger.debug("THREAD " + Thread.currentThread().getName() + " Delete " + d.getFile());
        try {
            d.getFile().delete();
        } catch (Exception e) {
            d.end(DataOperation.OpEndState.OP_FAILED, e);
            return;
        }
        d.end(DataOperation.OpEndState.OP_OK);
    }

}
