package integratedtoolkit.types.data.operation.copy;

import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.DataAccessId.RAccessId;
import integratedtoolkit.types.data.DataAccessId.RWAccessId;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.parameter.DependencyParameter;


/**
 * Representation of a Storage Copy
 *
 */
public class StorageCopy extends DataOperation {

    private final LogicalData srcData;
    private final DataLocation srcLoc;
    private final LogicalData tgtData;
    private DataLocation tgtLoc;

    private Transferable reason;
    private final boolean preserveSourceData;


    /**
     * Create a new Storage Copy
     * 
     * @param srcData
     * @param prefSrc
     * @param prefTgt
     * @param tgtData
     * @param reason
     * @param listener
     */
    public StorageCopy(LogicalData srcData, DataLocation prefSrc, DataLocation prefTgt, LogicalData tgtData, Transferable reason,
            EventListener listener) {

        super(srcData, listener);

        this.srcData = srcData;
        this.srcLoc = prefSrc;
        this.tgtData = tgtData;
        this.tgtLoc = prefTgt;
        this.reason = reason;

        DependencyParameter dPar = (DependencyParameter) reason;
        DataAccessId dAccId = dPar.getDataAccessId();
        if (dAccId instanceof RAccessId) {
            // Parameter is a R, has sources
            this.preserveSourceData = ((RAccessId) dAccId).isPreserveSourceData();
        } else if (dAccId instanceof RWAccessId) {
            // Parameter is a RW, has sources
            this.preserveSourceData = ((RWAccessId) dAccId).isPreserveSourceData();
        } else {
            // Parameter is a W, it has no sources
            this.preserveSourceData = false;
        }

        if (DEBUG) {
            LOGGER.debug("Created StorageCopy " + this.getName() + " (id: " + this.getId() + ")");
        }
    }

    /**
     * Returns the source data
     * 
     * @return
     */
    public LogicalData getSourceData() {
        return this.srcData;
    }

    /**
     * Returns the preferred location of the source data
     * 
     * @return
     */
    public DataLocation getPreferredSource() {
        return this.srcLoc;
    }

    /**
     * Returns the target location
     * 
     * @return
     */
    public DataLocation getTargetLoc() {
        return this.tgtLoc;
    }

    /**
     * Returns the target data
     * 
     * @return
     */
    public LogicalData getTargetData() {
        return this.tgtData;
    }

    /**
     * Returns whether the source data must be preserved or not
     * 
     * @return
     */
    public boolean mustPreserveSourceData() {
        return this.preserveSourceData;
    }

    /**
     * Sets a new final target data
     * 
     * @param targetAbsolutePath
     */
    public void setFinalTarget(String targetAbsolutePath) {
        if (DEBUG) {
            LOGGER.debug(" Setting StorageCopy final target to : " + targetAbsolutePath);
        }
        this.reason.setDataTarget(targetAbsolutePath);
    }

    /**
     * Returns whether the target data is registered or not
     * 
     * @return
     */
    public boolean isRegistered() {
        return this.tgtData != null;
    }

}