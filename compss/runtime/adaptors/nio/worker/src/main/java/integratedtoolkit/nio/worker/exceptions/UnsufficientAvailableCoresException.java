package integratedtoolkit.nio.worker.exceptions;

public class UnsufficientAvailableCoresException extends UnsufficientAvailableComputingUnitsException {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public UnsufficientAvailableCoresException(String message) {
        super(message);
    }

    public UnsufficientAvailableCoresException(Exception e) {
        super(e);
    }

    public UnsufficientAvailableCoresException(String msg, Exception e) {
        super(msg, e);
    }

}
