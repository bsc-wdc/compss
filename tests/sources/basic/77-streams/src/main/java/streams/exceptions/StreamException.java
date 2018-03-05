package streams.exceptions;

public class StreamException extends Exception {

    /**
     * Exceptions Version UID are 3L in all Tests
     */
    private static final long serialVersionUID = 3L;


    public StreamException() {
        super();
    }

    public StreamException(Exception e) {
        super(e);
    }

    public StreamException(String msg) {
        super(msg);
    }

}
