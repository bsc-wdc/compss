package es.bsc.compss.scheduler.exceptions;

public class ActionNotFoundException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;

    public ActionNotFoundException() {
        super();
    }

    public ActionNotFoundException(Exception e) {
        super(e);
    }

    public ActionNotFoundException(String msg) {
        super(msg);
    }

}
