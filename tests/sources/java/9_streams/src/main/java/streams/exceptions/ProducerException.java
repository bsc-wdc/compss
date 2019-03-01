package streams.exceptions;

public class ProducerException extends Exception {

    /**
     * Exceptions Version UID are 3L in all Tests
     */
    private static final long serialVersionUID = 3L;


    public ProducerException() {
        super();
    }

    public ProducerException(Exception e) {
        super(e);
    }

    public ProducerException(String msg) {
        super(msg);
    }

}
