package integratedtoolkit.gat.master.exceptions;

import integratedtoolkit.exceptions.CopyException;


/**
 * Exception for GAT Copies
 *
 */
public class GATCopyException extends CopyException {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new nested Exception @e
     * 
     * @param e
     */
    public GATCopyException(Exception e) {
        super(e);
    }

    /**
     * Creates a new Exception with a given message @msg
     * 
     * @param msg
     */
    public GATCopyException(String msg) {
        super(msg);
    }

}
