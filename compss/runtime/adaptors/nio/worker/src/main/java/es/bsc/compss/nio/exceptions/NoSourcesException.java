package es.bsc.compss.nio.exceptions;

public class NoSourcesException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime.
     */
    private static final long serialVersionUID = 2L;


    /**
     * Creates a new NoSourcesException with a fixed message.
     */
    public NoSourcesException() {
        super("No sources form where to load the value");
    }

}
