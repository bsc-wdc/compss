package streams.exceptions;

public class ConsumerException extends Exception {

    /**
     * Exceptions Version UID are 3L in all Tests
     */
    private static final long serialVersionUID = 3L;


    public ConsumerException() {
        super();
    }

    public ConsumerException(Exception e) {
        super(e);
    }

    public ConsumerException(String msg) {
        super(msg);
    }

    public ConsumerException(String msg, Exception e) {
        super(msg);
    }

}
