package integratedtoolkit.scheduler.exceptions;

public class UnassignedActionException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public UnassignedActionException() {
        super();
    }

    public UnassignedActionException(Exception e) {
        super(e);
    }

    public UnassignedActionException(String msg) {
        super(msg);
    }

}