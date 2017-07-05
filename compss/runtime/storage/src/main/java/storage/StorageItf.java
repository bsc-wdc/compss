package storage;

import java.util.List;


/**
 * Representation of the Storage ITF
 *
 */
public final class StorageItf {

    private static final String STORAGE_NOT_FOUND_MESSAGE = "You are trying to start a run with "
            + "persistent object storage but any back-end client is loaded in the classpath.";


    /**
     * Constructor
     * 
     */
    public StorageItf() {

    }

    /**
     * Initializes the persistent storage
     * 
     * @param storageConf
     * @throws StorageException
     */
    public static void init(String storageConf) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);

    }

    /**
     * Stops the persistent storage
     * 
     * @throws StorageException
     */
    public static void finish() throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Returns all the valid locations of a given id
     * 
     * @param id
     * @return
     * @throws StorageException
     */
    public static List<String> getLocations(String pscoId) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Creates a new replica of PSCO id @id in host @hostname
     * 
     * @param id
     * @param hostName
     * @throws StorageException
     */
    public static void newReplica(String id, String hostName) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Create a new version of the PSCO id @id in the host @hostname. The flag preserveSource indicates whether the PSCO
     * with ID @id must be preserved or can be removed. The method returns the id of the new version
     * 
     * @param id
     * @param preserveSource
     * @param hostName
     * @return
     * @throws StorageException
     */
    public static String newVersion(String id, boolean preserveSource, String hostName) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Returns the object with id @id This function retrieves the object from any location
     * 
     * @param id
     * @return
     * @throws StorageException
     */
    public static Object getByID(String id) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Executes the task into persistent storage
     * 
     * @param id
     * @param descriptor
     * @param values
     * @param hostName
     * @param callback
     * @return
     * @throws StorageException
     */
    public static String executeTask(String id, String descriptor, Object[] values, String hostName, CallbackHandler callback)
            throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

    /**
     * Retrieves the result of persistent storage execution
     * 
     * @param event
     * @return
     */
    public static Object getResult(CallbackEvent event) throws StorageException {
        // Nothing to do
        return null;
    }

    /**
     * Consolidates all intermediate versions to the final id
     * 
     * @param idFinal
     * @throws StorageException
     */
    public static void consolidateVersion(String idFinal) throws StorageException {
        throw new StorageException(STORAGE_NOT_FOUND_MESSAGE);
    }

}
