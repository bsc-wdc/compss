package integratedtoolkit.exceptions;

/**
 * Exception for Copy Exceptions
 *
 */
public class CopyException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Copy Exception
     */
    public CopyException() {
        super();
    }

    /**
     * New nested @e Copy Exception
     * 
     * @param e
     */
    public CopyException(Exception e) {
        super(e);
    }

    /**
     * New Copy Exception with message @msg
     * 
     * @param msg
     */
    public CopyException(String msg) {
        super(msg);
    }

}
