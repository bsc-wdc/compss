package es.bsc.compss.nio.worker.exceptions;

public class UnsufficientAvailableGPUsException extends UnsufficientAvailableComputingUnitsException {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public UnsufficientAvailableGPUsException(String message) {
        super(message);
    }

    public UnsufficientAvailableGPUsException(Exception e) {
        super(e);
    }

    public UnsufficientAvailableGPUsException(String msg, Exception e) {
        super(msg, e);
    }

}
