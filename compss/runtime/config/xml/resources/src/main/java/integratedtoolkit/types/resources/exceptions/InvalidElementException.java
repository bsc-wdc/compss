package integratedtoolkit.types.resources.exceptions;

public class InvalidElementException extends ResourcesFileValidationException {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public InvalidElementException(String elementType, String elementId, String reason) {
        super("Invalid element " + elementType + " with id = " + elementId + ". Reason: " + reason);
    }

}
