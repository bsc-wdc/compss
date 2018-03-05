package streams.components.exceptions;

public class SubscribeException extends Exception {

    /**
     * Exceptions Version UID are 3L in all Tests
     */
    private static final long serialVersionUID = 3L;


    /**
     * Creates a new exception for subscription
     * 
     */
    public SubscribeException() {
        super();
    }

    /**
     * Creates a new exception for subscription with a given message msg
     * 
     * @param msg
     */
    public SubscribeException(String msg) {
        super();
    }

    /**
     * Creates a new exception for subscription with a nested exception e
     * 
     * @param e
     */
    public SubscribeException(Exception e) {
        super(e);
    }

    /**
     * Creates a new exception for subscription with a given message msg and a nested exception e
     * 
     * @param msg
     * @param e
     */
    public SubscribeException(String msg, Exception e) {
        super();
    }

}
