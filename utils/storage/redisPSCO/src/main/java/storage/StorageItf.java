package storage;

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.Jedis;
import storage.utils.Serializer;

public final class StorageItf {

    // Logger According to Loggers.STORAGE
    private static final Logger LOGGER = LogManager.getLogger("compss.Storage");

    // Error Messages
    private static final String ERROR_HOSTNAME = "ERROR_HOSTNAME";

    // Directories
    private static final String BASE_WORKING_DIR = File.separator + "tmp" + File.separator + "PSCO" + File.separator;

    private static final String MASTER_HOSTNAME;
    private static final String MASTER_WORKING_DIR;

    private static final String ID_EXTENSION = ".ID";
    private static final String PSCO_EXTENSION = ".PSCO";

    // Redis variables
    // Client connection
    private static Jedis redisConnection;
    private static Properties storageConfiguration;

    private static String[] hosts;


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
     * Initializes the persistent storage
     * Configuration file must contain all the worker hostnames, one by line
     *
     * @param storageConf Path to the storage configuration File
     * @throws StorageException
     */
    public static void init(String storageConf) throws StorageException, IOException {
        //TODO: Make it work properly, at the moment we will assume that we must establish a connection with
        //TODO: the localhost port 6379
        redisConnection = new Jedis("127.0.0.1", 6379);
        hosts = new String[]{"127.0.0.1:6379"};
    }

    /**
     * Stops the persistent storage
     * 
     * @throws StorageException
     */
    public static void finish() throws StorageException {
        redisConnection.close();
    }

    /**
     * Returns all the valid locations of a given id
     * WARNING: Given that Redis has no immediate mechanisms to retrieve this information, we will return
     * all the nodes instead, because a connection to any of them will grant us that we can retrieve it
     * @param id
     * @return
     * @throws StorageException
     */
    public static List<String> getLocations(String id) throws StorageException {
        return Arrays.asList(hosts);
    }

    /**
     * Creates a new replica of PSCO id @id in host @hostname
     * 
     * @param id
     * @param hostName
     * @throws StorageException
     */
    public static void newReplica(String id, String hostName) throws StorageException {
        throw new StorageException("Redis does not support this feature");
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
    public static Object getByID(String id) throws StorageException, IOException, ClassNotFoundException {
        LOGGER.debug("Retrieving serialized object...");
        byte[] serializedObject = redisConnection.get(id.getBytes());
        LOGGER.debug("Serialized object has been retrieved!");
        Object ret = Serializer.deserialize(serializedObject);
        LOGGER.debug("Object has been deserialized!");
        return ret;
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
    public static void makePersistent(Object o, String id) throws StorageException, IOException {
        LOGGER.debug("Serializing the object...");
        byte[]  serializedObject = Serializer.serialize(o);
        LOGGER.debug("Object serialized! Adding to Redis...");
        redisConnection.set(id.getBytes(), serializedObject);
        LOGGER.debug("Object has been added to Redis");
    }

    /**
     * Removes all the occurrences of a given @id
     * 
     * @param id
     */
    public static void removeById(String id) {
        LOGGER.debug("Deleting object with id " + id);
        redisConnection.del(id.getBytes());
        LOGGER.debug("Object deleted!");
    }




    // ONLY FOR TESTING PURPOSES
    static class MyStorageObject extends StorageObject {
        private String innerString;

        public MyStorageObject(String myString) {
            innerString = myString;
        }

        public String getInnerString() {
            return innerString;
        }

        public void setInnerString(String innerString) {
            this.innerString = innerString;
        }
    }

    /**
     * ONLY FOR TESTING PURPOSES
     * @param args
     */
    public static void main(String[] args) throws ClassNotFoundException {
        try {
            init("I DONT CARE");
            String myObject = "This is an object";
            StorageItf.makePersistent(myObject, "prueba");
            Object retrieved = StorageItf.getByID("prueba");
            System.out.println((String)retrieved);
            StorageItf.removeById("prueba");
        } catch(StorageException e) {
            e.printStackTrace();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

}
