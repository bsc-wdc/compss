package integratedtoolkit.types.resources;

import java.io.IOException;
import java.io.ObjectInput;
import java.io.ObjectOutput;

import integratedtoolkit.types.implementations.Implementation;

/**
 * Data Node representation
 *
 */
public class DataResourceDescription extends ResourceDescription {

    // Unassigned values
    // !!!!!!!!!! WARNING: Coherent with constraints class
    public static final String UNASSIGNED_STR = "[unassigned]";
    public static final int UNASSIGNED_INT = -1;
    public static final float UNASSIGNED_FLOAT = (float) -1.0;

    // Required DataNode information
    private final String host;
    private final String path;
    // Optional information: Storage
    protected float storageSize = UNASSIGNED_FLOAT;
    protected String storageType = UNASSIGNED_STR;

    /**
     * New empty data Resource Description
     *
     */
    public DataResourceDescription() {
        // Only for externalization
        super();
        this.host = "";
        this.path = "";
    }

    /**
     * New Data Resource description with host @host and path @path
     *
     * @param host
     * @param path
     */
    public DataResourceDescription(String host, String path) {
        super();
        this.host = host;
        this.path = path;
    }

    /**
     * Clone method for Data Resource Description
     *
     * @param clone
     */
    public DataResourceDescription(DataResourceDescription clone) {
        super(clone);

        this.host = clone.host;
        this.path = clone.path;
        this.storageSize = clone.storageSize;
        this.storageType = clone.storageType;
    }

    @Override
    public DataResourceDescription copy() {
        return new DataResourceDescription(this);
    }

    /**
     * Returns the host
     *
     * @return
     */
    public String getHost() {
        return this.host;
    }

    /**
     * Returns the path
     *
     * @return
     */
    public String getPath() {
        return this.path;
    }

    /**
     * Returns the storage size
     *
     * @return
     */
    public float getStorageSize() {
        return storageSize;
    }

    /**
     * Sets a new storage size
     *
     * @param storageSize
     */
    public void setStorageSize(float storageSize) {
        if (storageSize > (float) 0.0) {
            this.storageSize = storageSize;
        }
    }

    /**
     * Returns the storage type
     *
     * @return
     */
    public String getStorageType() {
        return storageType;
    }

    /**
     * Sets a new storage type
     *
     * @param storageType
     */
    public void setStorageType(String storageType) {
        if (storageType != null) {
            this.storageType = storageType;
        }
    }

    @Override
    public boolean canHost(Implementation impl) {
        // DataNodes can not run any implementation
        return false;
    }

    @Override
    public void increase(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
    }

    @Override
    public void reduce(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
    }

    @Override
    public boolean canHostDynamic(Implementation impl) {
        // DataNodes can not run any implementation
        return false;
    }

    @Override
    public ResourceDescription getDynamicCommons(ResourceDescription constraints) {
        // There is nothing common between two dataNodes
        return null;
    }

    @Override
    public void increaseDynamic(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
    }

    @Override
    public ResourceDescription reduceDynamic(ResourceDescription rd) {
        // A DataNode cannot be increased nor decreased, nothing to do
        return null;
    }

    public void readExternal(ObjectInput in) throws IOException, ClassNotFoundException {
        // Nothing to serialize since it is never used
    }

    public void writeExternal(ObjectOutput out) throws IOException {
        // Nothing to serialize since it is never used
    }

    @Override
    public boolean isDynamicUseless() {
        // A DataNode cannot be useless
        return false;
    }

    @Override
    public String toString() {
        return "[DATANODE " + "HOST=" + this.host + " " + "PATH=" + this.path + " " + "]";
    }

    @Override
    public String getDynamicDescription() {
        return "";
    }
}
