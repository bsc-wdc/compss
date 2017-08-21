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
    public abstract void makePersistent(String id) throws IOException, StorageException;

    /**
     * Removes persistent object
     */
    public abstract void deletePersistent();

}
