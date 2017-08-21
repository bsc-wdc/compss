package storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.util.UUID;


public class StorageObject implements StubItf {

    // Logger: According to Loggers.STORAGE
    private static final Logger logger = LogManager.getLogger("compss.Storage");

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
    public void makePersistent(String id) throws IOException, StorageException {
        System.out.println("persistent obj...");
        // The object is already persisted
        if(this.id != null) return;
        // There was no given identifier, lets compute a random one
        if(id == null) {
            id = UUID.randomUUID().toString();
            logger.debug("Object to persist had no preferred ID, assigning random id:" + id);
        }
        this.id = id;
        // Call the storage API
        logger.debug("Persisting the object...");
        StorageItf.makePersistent(this, id);
        logger.debug("Object persisted!");
    }

    /**
     * Persist the object.
     * The identifier will be a pseudo-randomly generated UUID
     */
    public void makePersistent() throws IOException, StorageException {
        this.makePersistent(null);
    }
    /**
     * Deletes the persistent object occurrences
     */
    @Override
    public void deletePersistent() {
        // The object is not persisted, do nothing
        if(this.id == null) return;
        // Call the storage API
        StorageItf.removeById(this.id);
        // Set the id to null
        this.id = null;
    }

    /**
     * Sets the ID
     */
    //TODO: Is this the intended behaviour?
    protected void setID(String id) throws IOException, StorageException {
        // If the object was already identified then it means that it was also persisted
        // lets re-insert it to maintain consistency
        if(this.id != null) {
            // Remove the object from the storage system (old id)
            StorageItf.removeById(id);
            // Set the new id an re-add it to the storage
            // This may cause Redis to store the object on a different node than the previous one
            StorageItf.makePersistent(this, id);
        }
        // Assign the new id to the object in a safe, consistent way
        this.id = id;
    }
}
