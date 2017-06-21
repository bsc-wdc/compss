package integratedtoolkit.scheduler.exceptions;

public class BlockedActionException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;

    public BlockedActionException() {
        super();
    }

    public BlockedActionException(Exception e) {
        super(e);
    }

    public BlockedActionException(String msg) {
        super(msg);
    }

}
