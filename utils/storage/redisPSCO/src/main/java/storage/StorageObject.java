package storage;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

import java.io.IOException;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.UUID;


public class StorageObject implements StubItf {

    // Logger: According to Loggers.STORAGE
    private static final Logger logger = LogManager.getLogger("es.bsc.compss.Storage");

    private String host;

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
        // The object is already persisted
        if(this.id != null) return;
        // There was no given identifier, lets compute a random one
        setHost();
        if(id == null) {
            id = UUID.randomUUID().toString();
        }
        this.id = id;
        // Call the storage API
        StorageItf.makePersistent(this, id);
    }

    private void setHost() {
        String hostname = null;
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            hostname = localHost.getCanonicalHostName();
        } catch (UnknownHostException e) {
            e.printStackTrace();
            System.exit(1);
        }
        this.host = hostname;
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
        this.id = id;
    }

    /**
     * Updates the object in the database.
     * That is, removes the current version and then adds the in-memory one with
     * the same identifier.
     */
    public void updatePersistent() {
        String pId = this.getID();
        this.deletePersistent();
        try {
            this.makePersistent(pId);
        } catch (IOException | StorageException e) {
            e.printStackTrace();
        }
    }

    public String getHost() {
        return host;
    }

    public void setHost(String host) {
        this.host = host;
    }
}
