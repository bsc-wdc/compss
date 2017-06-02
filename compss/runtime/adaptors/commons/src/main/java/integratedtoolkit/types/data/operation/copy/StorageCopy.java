package integratedtoolkit.types.data.operation.copy;

import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.DataOperation;


public class StorageCopy extends DataOperation {

    private final LogicalData srcData;
    private final DataLocation srcLoc;
    private final LogicalData tgtData;
    private DataLocation tgtLoc;
    private Transferable reason;


    public StorageCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        super(srcData, listener);
        this.srcData = srcData;
        this.srcLoc = prefSrc;
        this.tgtData = tgtData;
        this.tgtLoc = prefTgt;
        this.reason = reason;
        if (DEBUG) {
            LOGGER.debug("Created replica " + this.getName() + " (id: " + this.getId() + ")");
        }
    }

    public LogicalData getSourceData() {
        return srcData;
    }

    public DataLocation getPreferredSource() {
        return srcLoc;
    }

    public DataLocation getTargetLoc() {
        return tgtLoc;
    }

    public LogicalData getTargetData() {
        return tgtData;
    }

    public void setFinalTarget(String targetAbsolutePath) {
        if (DEBUG) {
            LOGGER.debug(" Setting StorageCopy final target to : " + targetAbsolutePath);
        }
        reason.setDataTarget(targetAbsolutePath);
    }

    public boolean isRegistered() {
        return tgtData != null;
    }

}