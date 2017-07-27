package es.bsc.compss.scheduler.exceptions;

public class ActionNotWaitingException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;

    public ActionNotWaitingException() {
        super();
    }

    public ActionNotWaitingException(Exception e) {
        super(e);
    }

    public ActionNotWaitingException(String msg) {
        super(msg);
    }

}
