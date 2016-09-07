package storage;

import java.io.Serializable;


public class StorageException extends Exception implements Serializable {

    private static final long serialVersionUID = 1L;


    public StorageException() {

    }

    public StorageException(String message, Exception cause) {
        super(message, cause);
    }

    public StorageException(String message) {
        super(message);
    }

    public StorageException(Exception cause) {
        super(cause);
    }

}
