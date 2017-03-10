package nmmb.exceptions;

public class MainExecutionException extends Exception {

    /**
     * Exception Version UID
     */
    private static final long serialVersionUID = 2L;


    /**
     * Constructs a new MainExecutionException from a given message
     * 
     * @param message
     */
    public MainExecutionException(String message) {
        super(message);
    }

    /**
     * Constructs a new MainExecutionException with a nested exception @e
     * 
     * @param e
     */
    public MainExecutionException(Exception e) {
        super(e);
    }

    /**
     * Constructs a new MainExecutionException with a given message and with a nested exception @e
     * 
     * @param message
     * @param e
     */
    public MainExecutionException(String message, Exception e) {
        super(message, e);
    }

}
