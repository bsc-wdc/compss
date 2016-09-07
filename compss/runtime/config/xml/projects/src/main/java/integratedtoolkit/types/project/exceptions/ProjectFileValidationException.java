package integratedtoolkit.types.project.exceptions;

public class ProjectFileValidationException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public ProjectFileValidationException(Exception e) {
        super(e);
    }

    public ProjectFileValidationException(String msg) {
        super(msg);
    }

    public ProjectFileValidationException(String property, String parent) {
        super("Invalid property " + property + " with parent element " + parent);
    }

}
