package es.bsc.compss.scheduler.exceptions;

public class FailedActionException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;

    public FailedActionException() {
        super();
    }

    public FailedActionException(Exception e) {
        super(e);
    }

    public FailedActionException(String msg) {
        super(msg);
    }

}
