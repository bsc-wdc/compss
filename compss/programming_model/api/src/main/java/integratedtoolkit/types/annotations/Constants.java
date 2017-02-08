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
     * Values for task priority
     * 
     */
    public static final String IS_PRIORITARY_TASK = "true";
    
    public static final String IS_NOT_PRIORITARY_TASK = "false";


    /**
     * Values for target modification
     * 
     */
    public static final String IS_MODIFIER = "true";
    
    public static final String IS_NOT_MODIFIER = "false";


    /**
     * Replicated task execution
     * 
     */
    public static final String IS_REPLICATED_TASK = "true";
    
    public static final String IS_NOT_REPLICATED_TASK = "false";

    /**
     * Distributed task execution
     * 
     */
    public static final String IS_DISTRIBUTED_TASK = "true";
    
    public static final String IS_NOT_DISTRIBUTED_TASK = "false";
    
    /**
     * Single node value
     * 
     */
    public static final int SINGLE_NODE = 1;

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

    /**
     * Empty prefix for parameters
     */
    public static final String PREFIX_EMTPY = "null";

    // Private constructor to avoid instantiation
    private Constants() {
        throw new UnsupportedOperationException();
    }

}
