package storage;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;

// TODO: averiguar donde hay que levantar redis
// TODO: averiguar como obtener los nodos disponibles
public final class StorageItf {

    // Logger According to Loggers.STORAGE
    private static final Logger LOGGER = LogManager.getLogger("integratedtoolkit.Storage");

    // Error Messages
    private static final String ERROR_HOSTNAME = "ERROR_HOSTNAME";

    // Directories
    private static final String BASE_WORKING_DIR = File.separator + "tmp" + File.separator + "PSCO" + File.separator;

    private static final String MASTER_HOSTNAME;
    private static final String MASTER_WORKING_DIR;

    private static final String ID_EXTENSION = ".ID";
    private static final String PSCO_EXTENSION = ".PSCO";

    // Worker Hostnames
    private static final LinkedList<String> HOSTNAMES = new LinkedList<>();

    // Redis constants
    // Redis recommends to have at least three master nodes for clusters
    private static final int REDIS_MIN_MASTERS = 3;

    // redis.confg configuration template
    private static final String REDIS_CONF_TEMPLATE = String.join(
            System.getProperty("line.separator"),
            "bind 0.0.0.0",
            "port ##REDIS_PORT##",
            "cluster-enabled yes",
            "cluster-config-file nodes.conf",
            "cluster-node-timeout ##NODE_TIMEOUT##",
            "appendonly yes"
    );

    // Redis variables
    // Client connection
    private static Jedis redisConnection;


    static {
        String hostname = null;
        try {
            InetAddress localHost = InetAddress.getLocalHost();
            hostname = localHost.getCanonicalHostName();
        } catch (UnknownHostException e) {
            System.err.println(ERROR_HOSTNAME);
            e.printStackTrace();
            System.exit(1);
        }
        MASTER_HOSTNAME = hostname;
        MASTER_WORKING_DIR = BASE_WORKING_DIR + File.separator + MASTER_HOSTNAME + File.separator;
    }


    /**
     * Reads a config.properties file and returns a Properties object
     * @param path String with path to the cfg file
     * @return Properties object
     * @throws IOException If file not found or wrong formatting
     */
    private static Properties getPropertiesFromFile(String path) throws IOException {
        FileInputStream fis = new FileInputStream(path);
        Properties ret = new Properties();
        ret.load(fis);
        fis.close();
        return ret;
    }

    /**
     * Constructor
     */
    public StorageItf() {
        // Nothing to do since everything is static
    }

    /**
     * Initializes the persistent storage Configuration file must contain all the worker hostnames, one by line
     *
     * @param storageConf Path to the storage configuration File
     * @throws StorageException
     */
    public static void init(String storageConf) throws StorageException, IOException {

    }

    /**
     * Stops the persistent storage
     * 
     * @throws StorageException
     */
    public static void finish() throws StorageException {

    }

    /**
     * Returns all the valid locations of a given id
     * 
     * @param id
     * @return
     * @throws StorageException
     */
    public static List<String> getLocations(String id) throws StorageException {
        return null;
    }

    /**
     * Creates a new replica of PSCO id @id in host @hostname
     * 
     * @param id
     * @param hostName
     * @throws StorageException
     */
    public static void newReplica(String id, String hostName) throws StorageException {

    }

    /**
     * Create a new version of the PSCO id @id in the host @hostname Returns the id of the new version
     * 
     * @param id
     * @param hostName
     * @return
     * @throws StorageException
     */
    public static String newVersion(String id, boolean preserveSource, String hostName) throws StorageException {
        return null;
    }

    /**
     * Returns the object with id @id This function retrieves the object from any location
     * 
     * @param id
     * @return
     * @throws StorageException
     */
    public static Object getByID(String id) throws StorageException {
        return null;
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
        return null;
    }

    /**
     * Retrieves the result of persistent storage execution
     * 
     * @param event
     * @return
     */
    public static Object getResult(CallbackEvent event) throws StorageException {
        return null;
    }

    /**
     * Consolidates all intermediate versions to the final id
     * 
     * @param idFinal
     * @throws StorageException
     */
    public static void consolidateVersion(String idFinal) throws StorageException {
        LOGGER.info("Consolidating version for " + idFinal);

        // Nothing to do in this dummy implementation
    }

    /*
     * ****************************************************************************************************************
     * SPECIFIC IMPLEMENTATION METHODS
     *****************************************************************************************************************/
    /**
     * Stores the object @o in the persistent storage with id @id
     * 
     * @param o
     * @param id
     * @throws StorageException
     */
    public static void makePersistent(Object o, String id) throws StorageException {

    }

    /**
     * Removes all the occurrences of a given @id
     * 
     * @param id
     */
    public static void removeById(String id) {

    }

    public static void main(String[] args) {
        try {
            init("utils/storage/redisPSCO/src/main/resources/config.properties");
        } catch(StorageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
