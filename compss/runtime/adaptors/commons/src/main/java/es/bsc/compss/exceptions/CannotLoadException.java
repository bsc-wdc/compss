package es.bsc.compss.exceptions;

/**
 * Exception to handle nodes that have not been started yet
 * 
 */
public class CannotLoadException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Unstarted Node Exception
     * 
     */
    public CannotLoadException() {
        super();
    }

    /**
     * New nested @e Unstarted Node Exception
     * 
     * @param e
     */
    public CannotLoadException(Exception e) {
        super(e);
    }

    /**
     * New Unstarted Node Exception with message @msg
     * 
     * @param msg
     */
    public CannotLoadException(String msg) {
        super(msg);
    }

}
