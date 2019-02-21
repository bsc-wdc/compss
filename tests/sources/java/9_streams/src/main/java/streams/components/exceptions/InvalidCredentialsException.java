package streams.components.exceptions;

public class InvalidCredentialsException extends Exception {

    /**
     * Exceptions Version UID are 3L in all Tests
     */
    private static final long serialVersionUID = 3L;

    private static final String ERROR_MSG = "ERROR: Invalid credentials exception";


    /**
     * Creates a new exception for invalid credentials
     * 
     */
    public InvalidCredentialsException() {
        super(ERROR_MSG);
    }

    /**
     * Creates a new exception for invalid credentials with a nested exception e
     * 
     * @param e
     */
    public InvalidCredentialsException(Exception e) {
        super(ERROR_MSG, e);
    }

}