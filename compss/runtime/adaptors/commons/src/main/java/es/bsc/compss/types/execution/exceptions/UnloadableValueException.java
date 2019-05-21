package es.bsc.compss.types.execution.exceptions;

public class UnloadableValueException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new UnloadableValueException with a nested exception {@code e}.
     * 
     * @param e Nested exception.
     */
    public UnloadableValueException(Exception e) {
        super("Cannot load value due to nested exception", e);
    }

}
