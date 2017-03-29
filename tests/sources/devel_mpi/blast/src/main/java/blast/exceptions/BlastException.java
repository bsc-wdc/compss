package blast.exceptions;

/**
 * Blast exception to encapsulate exceptions raised by the BLAST application
 * 
 */
public class BlastException extends Exception {

    /**
     * External Exception ID
     */
    private static final long serialVersionUID = 3L;


    /**
     * Constructs a new BlastException with a nested exception @e
     * 
     * @param e
     */
    public BlastException(Exception e) {
        super(e);
    }

    /**
     * Constructs a new BlastException with an error message @msg
     * 
     * @param msg
     */
    public BlastException(String msg) {
        super(msg);
    }

    /**
     * Constructs a new BlastException with a nested exception @e and error message @msg
     * 
     * @param msg
     * @param e
     */
    public BlastException(String msg, Exception e) {
        super(msg, e);
    }

}
