package storage;

/**
 * Abstract interface of Stub objects
 *
 */
public interface StubItf {

    /**
     * Returns the persistent object ID
     * 
     * @return
     */
    public abstract String getID();

    /**
     * Persist the object
     * 
     * @param id
     */
    public abstract void makePersistent(String id);

    /**
     * Deletes the persistent object occurrences
     */
    public abstract void deletePersistent();

}
