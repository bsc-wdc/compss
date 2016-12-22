package blast.exceptions;

public class BlastException extends Exception {

    /**
     * External Exception ID
     */
    private static final long serialVersionUID = 3L;


    public BlastException(Exception e) {
        super(e);
    }

    public BlastException(String msg) {
        super(msg);
    }

    public BlastException(String msg, Exception e) {
        super(msg, e);
    }

}
