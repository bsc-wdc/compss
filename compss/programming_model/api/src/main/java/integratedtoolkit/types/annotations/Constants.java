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
    public static final String UNASSIGNED_STR 	= "[unassigned]";
    
    /**
     * Unassigned value for ints
     * 
     */
    public static final int UNASSIGNED_INT 		= -1;
    
    /**
     * Unassigned value for floats
     * 
     */
    public static final float UNASSIGNED_FLOAT 	= (float) -1.0;
    
    
    //Private constructor to avoid instantiation
    private Constants() {
    	throw new UnsupportedOperationException();
    }
    
}
