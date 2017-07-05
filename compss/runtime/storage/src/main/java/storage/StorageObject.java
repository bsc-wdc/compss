package storage;

/**
 * Abstract implementation of an Storage Object
 *
 */
public class StorageObject implements StubItf {

    /**
     * Constructor
     * 
     */
    public StorageObject() {

    }

    /**
     * Constructor by alias
     * 
     * @param alias
     */
    public StorageObject(String alias) {

    }

    /**
     * Returns the persistent object ID
     * 
     * @return
     */
    @Override
    public String getID() {
        throw new UnsupportedOperationException();
    }

    /**
     * Persist the object
     * 
     * @param id
     */
    @Override
    public void makePersistent(String id) {
        throw new UnsupportedOperationException();
    }

    /**
     * Deletes the persistent object occurrences
     */
    @Override
    public void deletePersistent() {
        throw new UnsupportedOperationException();
    }

}
