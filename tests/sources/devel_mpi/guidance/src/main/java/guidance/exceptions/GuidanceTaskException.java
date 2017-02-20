package guidance.exceptions;

/**
 * Class to represent exceptions while checking environment variables
 *
 */
public class GuidanceTaskException extends Exception {

    /**
     * Exception serial version ID
     */
    private static final long serialVersionUID = 1L;


    /**
     * Default Environment exception
     * 
     */
    public GuidanceTaskException() {
        super();
    }

    /**
     * Environment exception with error message
     * 
     * @param msg
     */
    public GuidanceTaskException(String msg) {
        super(msg);
    }

    /**
     * Environment exception with nested exception
     * 
     * @param e
     */
    public GuidanceTaskException(Exception e) {
        super(e);
    }

    /**
     * Environment exception with error message and nested exception
     * 
     * @param msg
     * @param e
     */
    public GuidanceTaskException(String msg, Exception e) {
        super(msg, e);
    }

}
