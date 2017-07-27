package es.bsc.compss.types.exceptions;

/**
 * Exception for non instantiable classes
 *
 */
public class NonInstantiableException extends RuntimeException {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new Non Instantiable Exception for a given class @className
     * 
     * @param className
     */
    public NonInstantiableException(String className) {
        super("Class " + className + " can not be instantiated.");
    }

}
