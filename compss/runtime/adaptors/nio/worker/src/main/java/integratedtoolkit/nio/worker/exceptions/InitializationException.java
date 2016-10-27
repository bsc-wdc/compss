package integratedtoolkit.nio.worker.exceptions;

public class InitializationException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public InitializationException(String message) {
        super(message);
    }

    public InitializationException(Exception e) {
        super(e);
    }

    public InitializationException(String msg, Exception e) {
        super(msg, e);
    }

}
