package integratedtoolkit.types.data.operation.copy;

import integratedtoolkit.exceptions.CopyException;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.uri.MultiURI;


public abstract class ImmediateCopy extends Copy {

    public ImmediateCopy(LogicalData ld, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        super(ld, prefSrc, prefTgt, tgtData, reason, listener);
    }

    public void perform() {
        Resource targetHost = tgtLoc.getHosts().getFirst();
        logger.debug("THREAD " + Thread.currentThread().getName() + " - Copy file " + getName() + " to " + tgtLoc);

        synchronized (srcData) {
            if (tgtData != null) {
                MultiURI u;
                if ((u = srcData.alreadyAvailable(targetHost)) != null) {
                    setFinalTarget(u.getPath());
                    end(DataOperation.OpEndState.OP_OK);
                    logger.debug("THREAD " + Thread.currentThread().getName() + " - A copy of " + getName() + " is already present at "
                            + targetHost + " on path " + u.getPath());
                    return;
                }
                Copy copyInProgress = null;
                if ((copyInProgress = srcData.alreadyCopying(tgtLoc)) != null) {
                    String path = copyInProgress.tgtLoc.getURIInHost(targetHost).getPath();
                    setFinalTarget(path);
                    // The same operation is already in progress - no need to repeat it
                    end(DataOperation.OpEndState.OP_IN_PROGRESS);

                    // This group must be notified as well when the operation finishes
                    synchronized (copyInProgress.getEventListeners()) {
                        copyInProgress.addEventListeners(getEventListeners());
                    }
                    logger.debug("THREAD " + Thread.currentThread().getName() + " - A copy to " + path
                            + " is already in progress, skipping replication");
                    return;
                }
            }
            srcData.startCopy(this, tgtLoc);
        }

        try {
        	logger.debug("[InmediateCopy] Performing Inmediate specific Copy for "+getName());
            specificCopy();
        } catch (CopyException e) {
            end(DataOperation.OpEndState.OP_FAILED, e);
            return;
        } finally {
            DataLocation actualLocation;
            synchronized (srcData) {
                actualLocation = srcData.finishedCopy(this);
            }
            if (tgtData != null) {
                synchronized (tgtData) {
                    tgtData.addLocation(actualLocation);
                }
            }
        }

        String path = tgtLoc.getURIInHost(targetHost).getPath();
        setFinalTarget(path);
        synchronized (srcData) {
            end(DataOperation.OpEndState.OP_OK);
        }
        logger.debug("[InmediateCopy] Inmediate Copy for "+getName() +" performed.");
    }

    public abstract void specificCopy() throws CopyException;
    
}
