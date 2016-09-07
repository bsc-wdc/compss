package integratedtoolkit.types.data;

import java.io.Serializable;


public abstract class DataAccessId implements Serializable {

    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;


    public static enum Direction {
        R,
        RW,
        W
    }


    public abstract int getDataId();

    public abstract Direction getDirection();


    // Read access
    public static class RAccessId extends DataAccessId {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        // File version read
        private DataInstanceId readDataInstance;
        // Source data preservation flag
        private boolean preserveSourceData = true;


        public RAccessId() {
        }

        public RAccessId(int dataId, int rVersionId) {
            this.readDataInstance = new DataInstanceId(dataId, rVersionId);
        }

        public RAccessId(DataInstanceId rdi) {
            this.readDataInstance = rdi;
        }

        @Override
        public Direction getDirection() {
            return Direction.R;
        }

        @Override
        public int getDataId() {
            return readDataInstance.getDataId();
        }

        public int getRVersionId() {
            return readDataInstance.getVersionId();
        }

        public DataInstanceId getReadDataInstance() {
            return readDataInstance;
        }

        public boolean isPreserveSourceData() {
            return preserveSourceData;
        }

        @Override
        public String toString() {
            return "Read data: " + readDataInstance + (preserveSourceData ? ", Preserved" : ", Erased");
        }

    }

    // Write access
    public static class WAccessId extends DataAccessId {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        // File version written
        private DataInstanceId writtenDataInstance;


        public WAccessId() {
        }

        public WAccessId(int dataId, int wVersionId) {
            this.writtenDataInstance = new DataInstanceId(dataId, wVersionId);
        }

        public WAccessId(DataInstanceId wdi) {
            this.writtenDataInstance = wdi;
        }

        @Override
        public Direction getDirection() {
            return Direction.W;
        }

        @Override
        public int getDataId() {
            return writtenDataInstance.getDataId();
        }

        public int getWVersionId() {
            return writtenDataInstance.getVersionId();
        }

        public DataInstanceId getWrittenDataInstance() {
            return writtenDataInstance;
        }

        @Override
        public String toString() {
            return "Written data: " + writtenDataInstance;
        }

    }

    // Read-Write access
    public static class RWAccessId extends DataAccessId {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        // File version read
        private DataInstanceId readDataInstance;
        // File version written
        private DataInstanceId writtenDataInstance;
        // Source data preservation flag
        private boolean preserveSourceData = false;


        public RWAccessId() {
        }

        public RWAccessId(DataInstanceId rdi, DataInstanceId wdi, boolean preserveSourceData) {
            this.readDataInstance = rdi;
            this.writtenDataInstance = wdi;
            this.preserveSourceData = preserveSourceData;
        }

        @Override
        public Direction getDirection() {
            return Direction.RW;
        }

        @Override
        public int getDataId() {
            return readDataInstance.getDataId();
        }

        public int getRVersionId() {
            return readDataInstance.getVersionId();
        }

        public int getWVersionId() {
            return writtenDataInstance.getVersionId();
        }

        public DataInstanceId getReadDataInstance() {
            return readDataInstance;
        }

        public DataInstanceId getWrittenDataInstance() {
            return writtenDataInstance;
        }

        public boolean isPreserveSourceData() {
            return preserveSourceData;
        }

        @Override
        public String toString() {
            return "Read data: " + readDataInstance + ", Written data: " + writtenDataInstance
                    + (preserveSourceData ? ", Preserved" : ", Erased");
        }

    }

}
