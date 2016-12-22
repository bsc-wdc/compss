package integratedtoolkit.types.data.operation.copy;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;


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
        if (debug) {
            logger.debug("Created copy " + this.getName() + " (id: " + this.getId() + ")");
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
        if (debug) {
            logger.debug(" Setting copy final target to : " + targetAbsolutePath);
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
