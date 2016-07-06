package integratedtoolkit.types.resources;

import integratedtoolkit.types.Implementation;

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

    public DataResourceDescription(String host, String path) {
        this.host = host;
        this.path = path;
    }

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

    public String getHost() {
        return this.host;
    }

    public String getPath() {
        return this.path;
    }

    public float getStorageSize() {
        return storageSize;
    }

    public void setStorageSize(float storageSize) {
        if (storageSize > (float) 0.0) {
            this.storageSize = storageSize;
        }
    }

    public String getStorageType() {
        return storageType;
    }

    public void setStorageType(String storageType) {
        if (storageType != null) {
            this.storageType = storageType;
        }
    }

    @Override
    public boolean canHost(Implementation<?> impl) {
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
    public boolean canHostDynamic(Implementation<?> impl) {
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

    @Override
    public boolean isDynamicUseless() {
        // A DataNode cannot be useless
        return false;
    }

    public String toString() {
        return "[DATANODE "
                + "HOST=" + this.host + " "
                + "PATH=" + this.path + " "
                + "]";
    }
}
