package es.bsc.compss.types.resources.exceptions;

public class ResourcesFileValidationException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public ResourcesFileValidationException(Exception e) {
        super(e);
    }

    public ResourcesFileValidationException(String msg) {
        super(msg);
    }

    public ResourcesFileValidationException(String property, String parent) {
        super("Invalid property " + property + " with parent element " + parent);
    }

}
