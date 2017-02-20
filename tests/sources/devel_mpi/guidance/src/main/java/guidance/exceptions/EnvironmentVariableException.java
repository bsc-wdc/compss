package guidance.exceptions;

/**
 * Class to represent exceptions while checking environment variables
 *
 */
public class EnvironmentVariableException extends Exception {

    /**
     * Exception serial version ID
     */
    private static final long serialVersionUID = 1L;


    /**
     * Default Environment exception
     * 
     */
    public EnvironmentVariableException() {
        super();
    }

    /**
     * Environment exception with error message
     * 
     * @param msg
     */
    public EnvironmentVariableException(String msg) {
        super(msg);
    }

    /**
     * Environment exception with nested exception
     * 
     * @param e
     */
    public EnvironmentVariableException(Exception e) {
        super(e);
    }

    /**
     * Environment exception with error message and nested exception
     * 
     * @param msg
     * @param e
     */
    public EnvironmentVariableException(String msg, Exception e) {
        super(msg, e);
    }

}
