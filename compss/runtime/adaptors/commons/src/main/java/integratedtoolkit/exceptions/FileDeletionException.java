package integratedtoolkit.exceptions;

/**
 * Exception for File Deletion Exceptions
 *
 */
public class FileDeletionException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty File Deletion Exception
     */
    public FileDeletionException() {
        super();
    }

    /**
     * New nested @e File Deletion Exception
     * 
     * @param e
     */
    public FileDeletionException(Exception e) {
        super(e);
    }

    /**
     * New File Deletion Exception with message @msg
     * 
     * @param msg
     */
    public FileDeletionException(String msg) {
        super(msg);
    }

}
