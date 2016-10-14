package integratedtoolkit.nio.exceptions;

public class JobExecutionException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;


    public JobExecutionException(String message) {
        super(message);
    }
    
    public JobExecutionException(Exception e) {
        super(e);
    }

    public JobExecutionException(String message, Exception e) {
        super(message, e);
    }
}
