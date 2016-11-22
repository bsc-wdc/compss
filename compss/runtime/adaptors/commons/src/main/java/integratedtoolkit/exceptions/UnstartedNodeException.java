package integratedtoolkit.exceptions;

/**
 * Exception to handle nodes that have not been started yet
 * 
 */
public class UnstartedNodeException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Unstarted Node Exception
     * 
     */
    public UnstartedNodeException() {
        super();
    }

    /**
     * New nested @e Unstarted Node Exception
     * 
     * @param e
     */
    public UnstartedNodeException(Exception e) {
        super(e);
    }

    /**
     * New Unstarted Node Exception with message @msg
     * 
     * @param msg
     */
    public UnstartedNodeException(String msg) {
        super(msg);
    }

}
