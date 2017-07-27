package es.bsc.compss.scheduler.exceptions;

public class InvalidSchedulingException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;

    public InvalidSchedulingException() {
        super();
    }

    public InvalidSchedulingException(Exception e) {
        super(e);
    }

    public InvalidSchedulingException(String msg) {
        super(msg);
    }

}
