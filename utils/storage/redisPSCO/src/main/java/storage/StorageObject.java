package storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

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
    public void makePersistent(String id) {
        // The object is already persisted
        if(this.id != null) return;
        // There was no given identifier, lets compute a random one
        if(id == null) {
            id = UUID.randomUUID().toString();
        }
        this.id = id;
        // Call the storage API
        try {
            StorageItf.makePersistent(this, id);
        } catch (StorageException e) {
            e.printStackTrace();
        }
    }

    /**
     * Persist the object.
     * The identifier will be a pseudo-randomly generated UUID
     */
    public void makePersistent() {
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
        // Set the id as null
        this.id = null;
    }

    /**
     * Sets the ID
     */
    protected void setID(String id) {
        // Remove the object from the storage system (old id)
        StorageItf.removeById(id);
        // Set the new id an re-add it to the storage
        // This may cause Redis to store the object on a different node than the previous one
        try {
            StorageItf.makePersistent(this, id);
        } catch (StorageException e) {
            e.printStackTrace();
        }
        this.id = id;
    }

}
