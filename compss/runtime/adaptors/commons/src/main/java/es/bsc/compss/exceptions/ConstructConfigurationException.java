package es.bsc.compss.exceptions;

/**
 * Representation of an exception while constructing an Adaptor Configuration
 *
 */
public class ConstructConfigurationException extends Exception {

    /**
     * Exceptions Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    /**
     * New empty Construct Configuration Exception
     */
    public ConstructConfigurationException() {
        super();
    }

    /**
     * New Construct Configuration Exception with a nested exception @e
     * 
     * @param e
     */
    public ConstructConfigurationException(Exception e) {
        super(e);
    }

    /**
     * New Construct Configuration Exception with a message @msg
     * 
     * @param msg
     */
    public ConstructConfigurationException(String msg) {
        super(msg);
    }

}
