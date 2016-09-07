package storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class StorageObject implements StubItf {

    // Logger: According to Loggers.STORAGE
    private static final Logger logger = LogManager.getLogger("integratedtoolkit.Storage");

    private String id = null;


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
        return this.id;
    }

    /**
     * Persist the object
     * 
     * @param id
     */
    @Override
    public void makePersistent(String id) {
        try {
            StorageItf.makePersistent(this, id);
            this.id = id;
        } catch (StorageException e) {
            logger.error("Exception serializing object", e);
        }
    }

    /**
     * Deletes the persistent object occurrences
     */
    @Override
    public void deletePersistent() {
        StorageItf.removeById(this.id);
        this.id = null;
    }

    /**
     * Sets the ID (only used by this implementation)
     */
    protected void setID(String id) {
        this.id = id;
    }

}
