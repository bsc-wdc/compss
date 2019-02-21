package streams.components.exceptions;

public class AnnounceException extends Exception {

    /**
     * Exceptions Version UID are 3L in all Tests
     */
    private static final long serialVersionUID = 3L;


    /**
     * Creates a new exception for publication
     * 
     */
    public AnnounceException() {
        super();
    }

    /**
     * Creates a new exception for publication with a given message msg
     * 
     * @param msg
     */
    public AnnounceException(String msg) {
        super();
    }

    /**
     * Creates a new exception for publication with a nested exception e
     * 
     * @param e
     */
    public AnnounceException(Exception e) {
        super(e);
    }

    /**
     * Creates a new exception for publication with a given message msg and a nested exception e
     * 
     * @param msg
     * @param e
     */
    public AnnounceException(String msg, Exception e) {
        super();
    }

}
