package es.bsc.compss.types.data.operation.copy;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.DataOperation;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;


public abstract class Copy extends DataOperation {

    protected final LogicalData srcData;
    protected final DataLocation srcLoc;
    protected final LogicalData tgtData;
    protected DataLocation tgtLoc;
    protected final Transferable reason;


    public Copy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        super(srcData, listener);
        this.srcData = srcData;
        this.srcLoc = prefSrc;
        this.tgtData = tgtData;
        this.tgtLoc = prefTgt;
        this.reason = reason;
        if (DEBUG) {
            LOGGER.debug("Created copy " + this.getName() + " (id: " + this.getId() + ")");
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

    public boolean isRegistered() {
        return tgtData != null;
    }

    public void setProposedSource(Object source) {
        reason.setDataSource(source);
    }

    public void setFinalTarget(String targetAbsolutePath) {
        if (DEBUG) {
            LOGGER.debug(" Setting copy final target to : " + targetAbsolutePath);
        }
        reason.setDataTarget(targetAbsolutePath);
    }

    public String getFinalTarget() {
        return reason.getDataTarget();
    }

    public DataType getType() {
        return reason.getType();
    }

}
