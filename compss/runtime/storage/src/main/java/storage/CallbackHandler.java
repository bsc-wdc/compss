package storage;

/**
 * Abstract representation to handle callback events
 * 
 */
public abstract class CallbackHandler {

    /**
     * Empty callback handler instantiation
     * 
     */
    public CallbackHandler() {
        // Nothing to do
    }

    /**
     * Adds an event listener to the callback event @e
     * 
     * @param e
     */
    protected void eventListener(CallbackEvent e) {
        // Nothing to do
    }

}
