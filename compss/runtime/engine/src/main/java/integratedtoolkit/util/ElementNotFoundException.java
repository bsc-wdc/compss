package integratedtoolkit.util;

/**
 * The ElementNotFoundException is an Exception that will arise when some element that someone was looking for in a set
 * is not inside it.
 */
public class ElementNotFoundException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * Constructs a new ElementNotFoundException with the default messatge
     */
    public ElementNotFoundException() {
        super("Cannot find the requested element");
    }

    /**
     * Constructs a new ElementNotFoundException with that message
     * 
     * @param message
     *            Message that will return the exception
     */
    public ElementNotFoundException(String message) {
        super(message);
    }

}
