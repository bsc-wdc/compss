package es.bsc.compss.types.data.accessid;

import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.DataVersion;


public class RWAccessId extends DataAccessId {

    /**
     * Serializable objects Version UID are 1L in all Runtime.
     */
    private static final long serialVersionUID = 1L;

    // File version read
    private DataVersion readDataVersion;
    // File version written
    private DataVersion writtenDataVersion;


    /**
     * Creates a new ReadWrite Access Id for serialization.
     */
    public RWAccessId() {
        // For serialization
    }

    /**
     * Creates a new ReadWrite Access Id with read version {@code rdv} and write version {@code wdv}.
     * 
     * @param rdv Read version.
     * @param wdv Write version.
     */
    public RWAccessId(DataVersion rdv, DataVersion wdv) {
        this.readDataVersion = rdv;
        this.writtenDataVersion = wdv;
    }

    @Override
    public int getDataId() {
        return this.readDataVersion.getDataInstanceId().getDataId();
    }

    @Override
    public Direction getDirection() {
        return Direction.RW;
    }

    /**
     * Returns the read data instance.
     * 
     * @return The read data instance.
     */
    public DataInstanceId getReadDataInstance() {
        return this.readDataVersion.getDataInstanceId();
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
     * Returns the read version id.
     * 
     * @return The read version id.
     */
    public int getRVersionId() {
        return this.readDataVersion.getDataInstanceId().getVersionId();
    }

    /**
     * Returns the write version id.
     * 
     * @return The write version id.
     */
    public int getWVersionId() {
        return this.writtenDataVersion.getDataInstanceId().getVersionId();
    }

    /**
     * Returns whether the source data must be preserved or not.
     * 
     * @return {@code true} if the source data must be preserved, {@code false} otherwise.
     */
    public boolean isPreserveSourceData() {
        return readDataVersion.hasMoreReaders();
    }

    @Override
    public String toString() {
        return "Read data: " + this.readDataVersion.getDataInstanceId() + ", Written data: "
                + this.writtenDataVersion.getDataInstanceId() + (isPreserveSourceData() ? ", Preserved" : ", Erased");
    }

}
