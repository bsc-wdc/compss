package es.bsc.compss.exceptions;

public class NoResourceAvailableException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public NoResourceAvailableException() {
        super();
    }

    public NoResourceAvailableException(Exception e) {
        super(e);
    }

    public NoResourceAvailableException(String msg) {
        super(msg);
    }

}
