package integratedtoolkit.nio.worker.exceptions;

public class InvalidMapException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public InvalidMapException(String message) {
        super(message);
    }

    public InvalidMapException(Exception e) {
        super(e);
    }

    public InvalidMapException(String msg, Exception e) {
        super(msg, e);
    }

}
