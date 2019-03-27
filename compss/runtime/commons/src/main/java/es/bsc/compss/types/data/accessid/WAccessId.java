package es.bsc.compss.types.data.accessid;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;


public class WAccessId extends DataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File version written
    private DataVersion writtenDataVersion;


    /**
     * Creates a new Write Access Id for serialization.
     */
    public WAccessId() {
        // For serialization
    }

    /**
     * Creates a new Write Access Id for data id {@code dataId} and version {@code wVersionId}.
     * 
     * @param dataId Data id.
     * @param wVersionId Write version id.
     */
    public WAccessId(int dataId, int wVersionId) {
        this.writtenDataVersion = new DataVersion(dataId, wVersionId);
    }

    /**
     * Creates a new WriteAccessId with the given data version.
     * 
     * @param wdv Write version.
     */
    public WAccessId(DataVersion wdv) {
        this.writtenDataVersion = wdv;
    }

    @Override
    public int getDataId() {
        return this.writtenDataVersion.getDataInstanceId().getDataId();
    }

    @Override
    public Direction getDirection() {
        return Direction.W;
    }

    /**
     * Returns the written data instance.
     * 
     * @return The written data instance.
     */
    public DataInstanceId getWrittenDataInstance() {
        return this.writtenDataVersion.getDataInstanceId();
    }

    /**
     * Returns the write version id.
     * 
     * @return The write version id.
     */
    public int getWVersionId() {
        return this.writtenDataVersion.getDataInstanceId().getVersionId();
    }

    @Override
    public String toString() {
        return "Written data: " + this.writtenDataVersion.getDataInstanceId();
    }

}
