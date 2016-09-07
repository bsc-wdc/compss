package integratedtoolkit.types.data;

import java.io.Serializable;

import integratedtoolkit.types.data.location.DataLocation;


//Parameters of access to a file
public class AccessParams implements Serializable {

    public static enum AccessMode {
        R, // Read
        W, // Write
        RW // ReadWrite
    }


    /**
     * Serializable objects Version UID are 1L in all Runtime
     */
    private static final long serialVersionUID = 1L;

    private final AccessMode mode;


    public AccessParams(AccessMode mode) {
        this.mode = mode;
    }

    public AccessMode getMode() {
        return mode;
    }


    // File access
    public static class FileAccessParams extends AccessParams {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        private DataLocation loc;


        public FileAccessParams(AccessMode mode, DataLocation loc) {
            super(mode);
            this.loc = loc;
        }

        public DataLocation getLocation() {
            return loc;
        }

    }

    // Object access
    public static class ObjectAccessParams extends AccessParams {

        /**
         * Serializable objects Version UID are 1L in all Runtime
         */
        private static final long serialVersionUID = 1L;

        private int hashCode;
        private Object value;


        public ObjectAccessParams(AccessMode mode, Object value, int hashCode) {
            super(mode);
            this.value = value;
            this.hashCode = hashCode;
        }

        public Object getValue() {
            return value;
        }

        public int getCode() {
            return hashCode;
        }
    }

}
