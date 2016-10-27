package integratedtoolkit.exceptions;

public class FileDeletionException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public FileDeletionException() {
        super();
    }

    public FileDeletionException(Exception e) {
        super(e);
    }

    public FileDeletionException(String msg) {
        super(msg);
    }

}
