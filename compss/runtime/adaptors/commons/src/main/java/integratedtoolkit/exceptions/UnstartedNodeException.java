package integratedtoolkit.exceptions;

public class UnstartedNodeException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;

    public UnstartedNodeException() {
        super();
    }

    public UnstartedNodeException(Exception e) {
        super(e);
    }

    public UnstartedNodeException(String msg) {
        super(msg);
    }
}
