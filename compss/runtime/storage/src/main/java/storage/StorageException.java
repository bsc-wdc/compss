package storage;

import java.io.Serializable;


/**
 * Exception representation for errors when calling the Storage ITF
 * 
 *
 */
public class StorageException extends Exception implements Serializable {

    private static final long serialVersionUID = 1L;


    /**
     * New empty storage exception
     * 
     */
    public StorageException() {

    }

    /**
     * New storage exception with message @message
     * 
     * @param message
     */
    public StorageException(String message) {
        super(message);
    }

    /**
     * New storage exception for nested exception @cause
     * 
     * @param cause
     */
    public StorageException(Exception cause) {
        super(cause);
    }

    /**
     * New storage exception with message @message and nested exception @cause
     * 
     * @param message
     * @param cause
     */
    public StorageException(String message, Exception cause) {
        super(message, cause);
    }

}
