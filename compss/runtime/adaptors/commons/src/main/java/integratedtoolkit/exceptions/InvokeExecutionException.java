package integratedtoolkit.exceptions;

public class InvokeExecutionException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public InvokeExecutionException(String message) {
        super(message);
    }

    public InvokeExecutionException(String message, Exception e) {
        super(message, e);
    }
}
