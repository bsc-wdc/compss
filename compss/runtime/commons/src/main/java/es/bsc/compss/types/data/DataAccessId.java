package es.bsc.compss.types.data;

import java.io.Serializable;


public abstract class DataAccessId implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;


    public static enum Direction {
        R, // Read
        RW, // Read and write
        W // Write
    }


    public abstract int getDataId();

    public abstract Direction getDirection();


    /**
     * Read access
     *
     */
    public static class RAccessId extends DataAccessId {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        // File version read
        //private DataInstanceId readDataInstance;
        private DataVersion readDataVersion;
        // Source data preservation flag
        private boolean preserveSourceData = true;


        public RAccessId() {
            // For serialization
        }

        public RAccessId(int dataId, int rVersionId) {
            this.readDataVersion = new DataVersion(dataId, rVersionId);
        }

        public RAccessId(DataVersion rdv) {
            this.readDataVersion = rdv;
        }

        @Override
        public Direction getDirection() {
            return Direction.R;
        }

        @Override
        public int getDataId() {
            return readDataVersion.getDataInstanceId().getDataId();
        }

        public int getRVersionId() {
            return readDataVersion.getDataInstanceId().getVersionId();
        }

        public DataInstanceId getReadDataInstance() {
            return readDataVersion.getDataInstanceId();
        }

        public boolean isPreserveSourceData() {
            return preserveSourceData;
        }

        @Override
        public String toString() {
            return "Read data: " + readDataVersion.getDataInstanceId() + (preserveSourceData ? ", Preserved" : ", Erased");
        }

    }

    /**
     * Write access
     *
     */
    public static class WAccessId extends DataAccessId {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        // File version written
        private DataVersion writtenDataVersion;


        public WAccessId() {
            // For serialization
        }

        public WAccessId(int dataId, int wVersionId) {
            this.writtenDataVersion = new DataVersion(dataId, wVersionId);
        }

        public WAccessId(DataVersion wdi) {
            this.writtenDataVersion = wdi;
        }

        @Override
        public Direction getDirection() {
            return Direction.W;
        }

        @Override
        public int getDataId() {
            return writtenDataVersion.getDataInstanceId().getDataId();
        }

        public int getWVersionId() {
            return writtenDataVersion.getDataInstanceId().getVersionId();
        }

        public DataInstanceId getWrittenDataInstance() {
            return writtenDataVersion.getDataInstanceId();
        }

        @Override
        public String toString() {
            return "Written data: " + writtenDataVersion.getDataInstanceId();
        }

    }

    /**
     * Read-Write access
     *
     */
    public static class RWAccessId extends DataAccessId {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        // File version read
        private DataVersion readDataVersion;
        // File version written
        private DataVersion writtenDataVersion;
        // Source data preservation flag
        //private boolean preserveSourceData = false;


        public RWAccessId() {
            // For serialization
        }

        public RWAccessId(DataVersion rdv, DataVersion wdv) {
            this.readDataVersion = rdv;
            this.writtenDataVersion = wdv;
        }

        @Override
        public Direction getDirection() {
            return Direction.RW;
        }

        @Override
        public int getDataId() {
            return readDataVersion.getDataInstanceId().getDataId();
        }

        public int getRVersionId() {
            return readDataVersion.getDataInstanceId().getVersionId();
        }

        public int getWVersionId() {
            return writtenDataVersion.getDataInstanceId().getVersionId();
        }

        public DataInstanceId getReadDataInstance() {
            return readDataVersion.getDataInstanceId();
        }

        public DataInstanceId getWrittenDataInstance() {
            return writtenDataVersion.getDataInstanceId();
        }

        public boolean isPreserveSourceData() {
        	return readDataVersion.isOnlyReader();
        }

        @Override
        public String toString() {
            return "Read data: " + readDataVersion.getDataInstanceId() + ", Written data: " + writtenDataVersion.getDataInstanceId()
                    + (isPreserveSourceData() ? ", Preserved" : ", Erased");
        }

    }

}
