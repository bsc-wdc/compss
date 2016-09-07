package integratedtoolkit.types.request.exceptions;

import java.util.concurrent.Semaphore;


public class ShutdownException extends Exception {

    /**
     * Exception Version UID are 2L in all Runtime
     */
    private static final long serialVersionUID = 2L;
    private final Semaphore sem;


    public ShutdownException(Semaphore sem) {
        super();
        this.sem = sem;
    }

    public Semaphore getSemaphore() {
        return this.sem;
    }

}
