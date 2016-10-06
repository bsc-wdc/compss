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

	public static final String CPU_TYPE = "CPU";
	public static final String GPU_TYPE = "GPU";
	public static final String FPGA_TYPE = "FPGA";
	public static final String OTHER_TYPE = "OTHER";
	
    public static final String UNASSIGNED_PROCESSOR_TYPE = CPU_TYPE;
    public static final Processor[] UNASSIGNED_PROCESSORS= new Processor[]{};
    
    /**
     * Replicated task execution
     * 
     */
    public static final boolean REPLICATED_TASK = true;

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
