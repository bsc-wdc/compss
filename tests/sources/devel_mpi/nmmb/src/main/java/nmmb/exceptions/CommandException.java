package nmmb.exceptions;

public class CommandException extends Exception {

    /**
     * Exception Version UID
     */
    private static final long serialVersionUID = 2L;


    /**
     * Constructs a new CommandException from a given message
     * 
     * @param message
     */
    public CommandException(String message) {
        super(message);
    }

    /**
     * Constructs a new CommandException with a nested exception @e
     * 
     * @param e
     */
    public CommandException(Exception e) {
        super(e);
    }

    /**
     * Constructs a new CommandException with a given message and with a nested exception @e
     * 
     * @param message
     * @param e
     */
    public CommandException(String message, Exception e) {
        super(message, e);
    }

}
