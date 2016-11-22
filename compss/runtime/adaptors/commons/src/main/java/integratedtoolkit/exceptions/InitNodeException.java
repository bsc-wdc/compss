package integratedtoolkit.exceptions;

/**
 * Exception to handle errors on node start
 * 
 */
public class InitNodeException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Init Node Exception
     * 
     */
    public InitNodeException() {
        super();
    }

    /**
     * New nested @e Init Node Exception
     * 
     * @param e
     */
    public InitNodeException(Exception e) {
        super(e);
    }

    /**
     * New Init Node Exception with message @msg
     * 
     * @param msg
     */
    public InitNodeException(String msg) {
        super(msg);
    }

}
