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
    public static final String UNASSIGNED = "[unassigned]";

    /**
     * Priority value
     * 
     */
    public static final boolean PRIORITY = true;

    /**
     * Target modification boolean
     * 
     */
    public static final boolean IS_MODIFIER = true;

    /**
     * Single node value
     * 
     */
    public static final int SINGLE_NODE = 1;

    /**
     * Replicated task execution
     * 
     */
    public static final boolean REPLICATED_TASK = true;

    /**
     * Distributed task execution
     * 
     */
    public static final boolean DISTRIBUTED_TASK = true;

    /**
     * Processor types
     * 
     */
    public static final String CPU_TYPE = "CPU";
    public static final String GPU_TYPE = "GPU";
    public static final String FPGA_TYPE = "FPGA";
    public static final String OTHER_TYPE = "OTHER";

    /**
     * Unassigned value for processor type
     */
    public static final String UNASSIGNED_PROCESSOR_TYPE = CPU_TYPE;

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
     * Number of available cores for OmpSs tasks
     */
    public static final String COMPSS_NUM_THREADS = "COMPSS_NUM_THREADS";


    // Private constructor to avoid instantiation
    private Constants() {
        throw new UnsupportedOperationException();
    }

}
