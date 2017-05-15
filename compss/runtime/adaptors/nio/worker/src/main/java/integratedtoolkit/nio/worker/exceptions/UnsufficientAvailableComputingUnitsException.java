package integratedtoolkit.nio.worker.exceptions;

public class UnsufficientAvailableComputingUnitsException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public UnsufficientAvailableComputingUnitsException(String message) {
        super(message);
    }

    public UnsufficientAvailableComputingUnitsException(Exception e) {
        super(e);
    }

    public UnsufficientAvailableComputingUnitsException(String msg, Exception e) {
        super(msg, e);
    }

}
