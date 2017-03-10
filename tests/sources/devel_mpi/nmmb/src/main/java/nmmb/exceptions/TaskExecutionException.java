package nmmb.exceptions;

public class TaskExecutionException extends Exception {

    /**
     * Exception Version UID
     */
    private static final long serialVersionUID = 2L;


    /**
     * Constructs a new TaskExecutionException from a given message
     * 
     * @param message
     */
    public TaskExecutionException(String message) {
        super(message);
    }

    /**
     * Constructs a new TaskExecutionException with a nested exception @e
     * 
     * @param e
     */
    public TaskExecutionException(Exception e) {
        super(e);
    }

    /**
     * Constructs a new TaskExecutionException with a given message and with a nested exception @e
     * 
     * @param message
     * @param e
     */
    public TaskExecutionException(String message, Exception e) {
        super(message, e);
    }

}
