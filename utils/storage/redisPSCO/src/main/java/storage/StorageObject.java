package storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;


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

    }

    /**
     * Deletes the persistent object occurrences
     */
    @Override
    public void deletePersistent() {
        this.id = null;
    }

    /**
     * Sets the ID (only used by this implementation)
     */
    protected void setID(String id) {
        this.id = id;
    }

}
