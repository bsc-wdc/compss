package es.bsc.compss.loader.exceptions;

/**
 * Exception for null or not found method/class names
 * 
 */
public class NameNotFoundException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Name Not Found Exception
     * 
     */
    public NameNotFoundException() {
        super();
    }

    /**
     * New nested @e Name Not Found Exception
     * 
     * @param e
     */
    public NameNotFoundException(Exception e) {
        super(e);
    }

    /**
     * New Announce Name Not Found with message @msg
     * 
     * @param msg
     */
    public NameNotFoundException(String msg) {
        super(msg);
    }

}
