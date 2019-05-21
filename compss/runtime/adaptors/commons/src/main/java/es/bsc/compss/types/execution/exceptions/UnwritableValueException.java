package es.bsc.compss.types.execution.exceptions;

public class UnwritableValueException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new UnwritableValueException with a nested exception {@code e}.
     * 
     * @param e Nested exception.
     */
    public UnwritableValueException(Exception e) {
        super("Cannot write value due to nested exception", e);
    }

}
