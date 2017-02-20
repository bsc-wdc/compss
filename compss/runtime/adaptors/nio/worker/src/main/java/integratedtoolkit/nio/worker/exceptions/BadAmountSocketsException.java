package integratedtoolkit.nio.worker.exceptions;


public class BadAmountSocketsException extends Exception{

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public BadAmountSocketsException(String message) {
        super(message);
    }

    public BadAmountSocketsException(Exception e) {
        super(e);
    }

    public BadAmountSocketsException(String msg, Exception e) {
        super(msg, e);
    }
    
}
