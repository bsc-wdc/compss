package storage;

import java.io.IOException;

public interface StubItf {

    /**
     * Returns the Id of the persistent object
     *
     * @return
     */
    public abstract String getID();

    /**
     * Makes persistent the current object
     *
     * @param id
     */
    //TODO: Add StorageException in the header of ALL interfaces (it does not make sense to catch it)
    public abstract void makePersistent(String id) throws IOException, StorageException;

    /**
     * Removes persistent object
     */
    public abstract void deletePersistent();

}
