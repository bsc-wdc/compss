package es.bsc.compss.nio.exceptions;

public class SerializedObjectException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public SerializedObjectException(String dataName) {
        super(dataName);
    }

}
