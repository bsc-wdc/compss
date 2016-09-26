package integratedtoolkit.types.annotations;

/**
 * Constants for the Annotations Interface
 * 
 */
public class Constants {

    /**
     * Unassigned value for Strings
     * 
     */
    public static final String UNASSIGNED_STR = "[unassigned]";

    /**
     * Unassigned value for ints
     * 
     */
    public static final int UNASSIGNED_INT = -1;

    /**
     * Unassigned value for floats
     * 
     */
    public static final float UNASSIGNED_FLOAT = (float) -1.0;
    
    /**
     * Reference to all COMPSs nodes
     * 
     */
    public static final String ALL_NODES = "all";
    
    /**
     * Single node execution
     * 
     */
    public static final int SINGLE_NODE = 1;

    /**
     * Available hostnames for MPI tasks
     * 
     */
    public static final String COMPSS_HOSTNAMES = "COMPSS_HOSTNAMES";

    /**
     * Number of available workers for MPI tasks
     */
    public static final String COMPSS_NUM_NODES = "COMPSS_NUM_NODES";

    /**
     * Number of available cores for Ompss tasks
     */
    public static final String COMPSS_NUM_THREADS = "COMPSS_NUM_THREADS";


    // Private constructor to avoid instantiation
    private Constants() {
        throw new UnsupportedOperationException();
    }

}
