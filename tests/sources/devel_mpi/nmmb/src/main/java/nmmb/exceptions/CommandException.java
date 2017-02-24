package nmmb.exceptions;

public class CommandException extends Exception {

    /**
     * Exception Version UID
     */
    private static final long serialVersionUID = 2L;


    public CommandException(String message) {
        super(message);
    }

    public CommandException(Exception e) {
        super(e);
    }

    public CommandException(String message, Exception e) {
        super(message, e);
    }
}
