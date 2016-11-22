package integratedtoolkit.exceptions;

/**
 * Exception to creation/deletion announce errors
 * 
 */
public class AnnounceException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Announce Exception
     * 
     */
    public AnnounceException() {
        super();
    }

    /**
     * New nested @e Announce Exception
     * 
     * @param e
     */
    public AnnounceException(Exception e) {
        super(e);
    }

    /**
     * New Announce Exception with message @msg
     * 
     * @param msg
     */
    public AnnounceException(String msg) {
        super(msg);
    }

}
