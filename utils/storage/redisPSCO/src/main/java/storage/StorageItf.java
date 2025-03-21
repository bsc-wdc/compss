/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package storage;

import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.io.Serializable;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeSet;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import redis.clients.jedis.HostAndPort;
import redis.clients.jedis.Jedis;
import redis.clients.jedis.JedisCluster;
import redis.clients.jedis.JedisPool;
import redis.clients.jedis.JedisPoolConfig;
import redis.clients.util.JedisClusterCRC16;

import storage.utils.Serializer;


public final class StorageItf {

    // Logger According to Loggers.STORAGE
    private static final Logger LOGGER = LogManager.getLogger("es.bsc.compss.Storage");

    // Redis variables

    // This port is the official Redis Port
    // See https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers
    // The storage API will assume that, given a hostname, there is a Redis Server listening there
    private static final int REDIS_PORT = 6379;
    private static final int REDIS_MAX_CLIENTS = 1 << 19;
    // Number of Redis hash slots. This is fixed, and official. See redis.io tutorials
    private static final int REDIS_MAX_HASH_SLOTS = 16_384;
    // Client connections
    // Given that the client classes that are needed to establish a connection with a Redis backend are
    // different for standalone and cluster cases, we are going to first try to establish a connection with
    // the cluster client, and, if it fails, with the standalone client
    // Given that JedisCluster and Jedis are classes that share no common ancestor, this is the cleanest way I can
    // come up with.
    private static JedisCluster redisClusterConnection;
    private static JedisPool redisConnection;
    private static boolean clusterMode;

    private static final List<String> HOSTS = new ArrayList<>();
    private static final Map<String, String> PREVIOUS_VERSION = new HashMap<>();
    // Given a hash slot, return a list with the hosts that contain at least one instance that includes
    // this slot in its slot interval
    @SuppressWarnings("unchecked")
    private static final List<String>[] HOSTS_BY_SLOT = new ArrayList[REDIS_MAX_HASH_SLOTS];

    static {
        // Cluster mode
        clusterMode = true;
    }


    /**
     * Constructor.
     */
    public StorageItf() {
        // Nothing to do since everything is static.
    }

    /**
     * Initializes the persistent storage Configuration file must contain all the worker hostnames, one by line.
     *
     * @param storageConf Path to the storage configuration File.
     * @throws StorageException When an internal error occurs.
     * @throws IOException When the storage configuration file cannot be opened.
     */
    public static void init(String storageConf) throws StorageException, IOException {
        LOGGER.info("[LOG] Configuration received: " + storageConf);
        try (BufferedReader br = new BufferedReader(new FileReader(storageConf))) {
            String line;
            while ((line = br.readLine()) != null) {
                HOSTS.add(line.trim());
                LOGGER.info("Adding " + line.trim() + " to list of known hosts...");
            }
        } catch (FileNotFoundException e) {
            throw new StorageException("Could not find configuration file", e);
        } catch (IOException e) {
            throw new StorageException("Could not open configuration file", e);
        }
        assert (!HOSTS.isEmpty());

        clusterMode = HOSTS.size() > 1;
        if (clusterMode) {
            LOGGER.info("More than one host detected, enabling Client Cluster Mode");
            // TODO: Ask Jedis guys why JedisCluster needs a HostAndPort and why Jedis needs a String and an Integer
            redisClusterConnection = new JedisCluster(new HostAndPort(HOSTS.get(0), REDIS_PORT));
            // Precompute host hashmap
            preComputeHostHashMap();
        } else {
            LOGGER.info("Only one host detect, using standalone client...");
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(REDIS_MAX_CLIENTS);
            redisConnection = new JedisPool(poolConfig, HOSTS.get(0), REDIS_PORT);
        }
    }

    private static void preComputeHostHashMap() {
        String someHost = (String) redisClusterConnection.getClusterNodes().keySet().toArray()[0];
        String clusterInfo = redisClusterConnection.getClusterNodes().get(someHost).getResource().clusterNodes();
        String[] clusterLines = clusterInfo.split("\n");
        ArrayList<Host> clusterHosts = new ArrayList<>();
        for (String line : clusterLines) {
            Host h = new Host(line);
            clusterHosts.add(h);
        }
        for (int i = 0; i < REDIS_MAX_HASH_SLOTS; ++i) {
            ArrayList<String> validHosts = new ArrayList<>();
            for (Host h : clusterHosts) {
                if (h.l <= i && i <= h.r) {
                    validHosts.add(h.host);
                }
            }
            HOSTS_BY_SLOT[i] = new ArrayList<>(new TreeSet<>(validHosts));
        }
    }

    /**
     * Stops the persistent storage StorageItf.
     *
     * @throws StorageException When the persistent storage cannot be stopped.
     */
    public static void finish() throws StorageException {
        if (clusterMode) {
            try {
                redisClusterConnection.close();
            } catch (IOException e) {
                LOGGER.error(e);
            }
        } else {
            redisConnection.close();
        }
    }

    /**
     * Returns all the valid locations of a given id.
     *
     * @param id Object identifier.
     * @return List of valid locations for given resource.
     * @throws StorageException When the persistent storage raises an internal exception.
     */
    public static List<String> getLocations(String id) throws StorageException {
        if (id != null && clusterMode) {
            int slot = JedisClusterCRC16.getSlot(id);
            return HOSTS_BY_SLOT[slot];
        } else {
            return HOSTS;
        }
    }

    /**
     * Creates a new replica of PSCO id {@code id} in host {@code hostname}.
     *
     * @param id Data Id.
     * @param hostName Hostname.
     * @throws StorageException When the persistent storage raises an internal exception.
     */
    public static void newReplica(String id, String hostName) throws StorageException {
        throw new StorageException("Redis does not support this feature.");
    }

    private static void putInRedis(byte[] serializedObject, String id) throws StorageException {
        String result = clusterMode ? redisClusterConnection.set(id.getBytes(), serializedObject)
            : redisConnection.getResource().set(id.getBytes(), serializedObject);
        if (!result.equals("OK")) {
            throw new StorageException("Redis returned an error while trying to store object with id " + id);
        }
    }

    private static void putListInRedis(List<byte[]> serializedObjects, String id) throws StorageException {
        long position = 0;
        for (byte[] element : serializedObjects) {
            position = clusterMode ? redisClusterConnection.rpush(id.getBytes(), element)
                : redisConnection.getResource().rpush(id.getBytes(), element);
        }
        if (position != serializedObjects.size()) {
            throw new StorageException("Redis failed while storing object with id " + id);
        }
    }

    /**
     * Create a new version of the PSCO id {@code id} in the host {@code hostname}. Returns the id of the new version.
     *
     * @param id Data id.
     * @param hostName Hostname.
     * @return The Id of the new version.
     * @throws StorageException When the persistent storage raises an internal exception.
     */
    public static String newVersion(String id, boolean preserveSource, String hostName)
        throws StorageException, IOException, ClassNotFoundException {
        String newId = UUID.randomUUID().toString();
        String valueType = clusterMode ? redisClusterConnection.type(id.getBytes())
            : redisConnection.getResource().type(id.getBytes());
        if (valueType.equals("list")) {
            List<byte[]> obj = getBytesByIDFromList(id);
            PREVIOUS_VERSION.put(newId, id);
            putListInRedis(obj, newId);
        } else {
            byte[] obj = getBytesByID(id);
            PREVIOUS_VERSION.put(newId, id);
            putInRedis(obj, newId);
        }
        if (!preserveSource) {
            consolidateVersion(newId);
        }
        return newId;
    }

    /**
     * Returns the object with id {@code id}. This function retrieves the object from any location.
     *
     * @param id Data Id.
     * @return The object with the given id.
     * @throws StorageException When the persistent storage raises an internal exception.
     */
    public static Object getByID(String id) throws StorageException, IOException, ClassNotFoundException {
        byte[] serializedObject =
            clusterMode ? redisClusterConnection.get(id.getBytes()) : redisConnection.getResource().get(id.getBytes());
        if (serializedObject == null) {
            throw new StorageException("Object with id " + id + " is not in Redis!");
        }
        Object ret = Serializer.deserialize(serializedObject);
        ((StorageObject) ret).setID(id);
        return ret;
    }

    private static byte[] getBytesByID(String id) throws StorageException {
        byte[] ret = null;
        ret =
            clusterMode ? redisClusterConnection.get(id.getBytes()) : redisConnection.getResource().get(id.getBytes());
        if (ret == null) {
            throw new StorageException("Object with id " + id + " is not in Redis!");
        }
        return ret;
    }

    private static List<byte[]> getBytesByIDFromList(String id) throws StorageException {
        List<byte[]> rets = new ArrayList<>();
        long valueLength = clusterMode ? redisClusterConnection.llen(id.getBytes())
            : redisConnection.getResource().llen(id.getBytes());
        if (valueLength == 0) {
            throw new StorageException("Object with id " + id + " is not in Redis!");
        }
        rets = clusterMode ? redisClusterConnection.lrange(id.getBytes(), 0, valueLength)
            : redisConnection.getResource().lrange(id.getBytes(), 0, valueLength);
        return rets;
    }

    /**
     * Executes the task into persistent storage.
     *
     * @param id Task id.
     * @param descriptor Task description.
     * @param values Task parameter values.
     * @param hostName Hostname where to execute the task.
     * @param callback Callback handler.
     * @return id of the executor.
     * @throws StorageException When an internal error occurs.
     */
    public static String executeTask(String id, String descriptor, Object[] values, String hostName,
        CallbackHandler callback) throws StorageException {
        throw new StorageException("Redis does not support this feature.");
    }

    /**
     * Retrieves the result of persistent storage execution.
     *
     * @param event Event to retrieve the result from.
     * @return Result of the persistent storage execution.
     */
    public static Object getResult(CallbackEvent event) throws StorageException {
        throw new StorageException("Redis does not support this feature.");
    }

    /**
     * Consolidates all intermediate versions to the final id.
     *
     * @param idFinal Final Id.
     * @throws StorageException When an internal error occurs.
     */
    public static void consolidateVersion(String idFinal) throws StorageException {
        LOGGER.info("Consolidating version for " + idFinal);
        // Skip final version
        idFinal = PREVIOUS_VERSION.get(idFinal);
        while (idFinal != null) {
            LOGGER.info("Removing version " + idFinal);
            removeById(idFinal);
            String oldId = idFinal;
            idFinal = PREVIOUS_VERSION.get(idFinal);
            PREVIOUS_VERSION.remove(oldId);
        }
    }

    /*
     * ****************************************************************************************************************
     * SPECIFIC IMPLEMENTATION METHODS
     *****************************************************************************************************************/
    /**
     * Stores the object {@code o} in the persistent storage with id {@code id}.
     *
     * @param o Object to store.
     * @param id Object Id.
     * @throws StorageException When an internal error occurs.
     */
    public static void makePersistent(Object o, String id) throws StorageException {
        byte[] serializedObject;
        try {
            serializedObject = Serializer.serialize(o);
        } catch (IOException ioe) {
            throw new StorageException(ioe);
        }

        String result;
        if (clusterMode) {
            result = redisClusterConnection.set(id.getBytes(), serializedObject);
        } else {
            result = redisConnection.getResource().set(id.getBytes(), serializedObject);
        }
        if (!result.equals("OK")) {
            throw new StorageException("Redis returned an error while trying to store object with id " + id);
        }
    }

    /**
     * Removes all the occurrences of a given data with id {@code id}.
     *
     * @param id Data Id to remove.
     */
    public static void removeById(String id) {
        if (clusterMode) {
            redisClusterConnection.del(id.getBytes());
        } else {
            redisConnection.getResource().del(id.getBytes());
        }
    }

    /*
     * ****************************************************************************************************************
     * PRIVATE CLASSES
     *****************************************************************************************************************/


    /**
     * Temporary representation of a host.
     */
    private static class Host {

        // Host (name)
        private String host;
        // Hash slot endpoints
        private int l;
        private int r;


        /**
         * Creates a new host parsing the info line.
         *
         * @param clusterInfoLine Host information string line.
         */
        public Host(String clusterInfoLine) {
            String[] tokens = clusterInfoLine.split(" ");
            this.host = tokens[1].split("@")[0].split(":")[0];
            InetAddress addr = null;
            try {
                addr = InetAddress.getByName(this.host);
            } catch (UnknownHostException e) {
                e.printStackTrace();
            }
            this.host = addr.getHostName();
            String[] interval = tokens[tokens.length - 1].split("-");
            this.l = Integer.parseInt(interval[0]);
            this.r = Integer.parseInt(interval[1]);
        }

    }

    /*
     * ****************************************************************************************************************
     * TESTING CLASSES AND METHODS
     *****************************************************************************************************************/

    /**
     * Only for testing purposes.
     */
    private static class MyStorageObject extends StorageObject implements Serializable {

        /**
         * Custom Serialization version UUID.
         */
        private static final long serialVersionUID = 5L;

        private final String innerString;


        public MyStorageObject(String myString) {
            this.innerString = myString;
        }

        public String getInnerString() {
            return this.innerString;
        }
    }


    /**
     * Main function for internal testing purposes.
     *
     * @param args Application arguments.
     * @throws ClassNotFoundException When main class is not found.
     */
    public static void main(String[] args) throws ClassNotFoundException, StorageException, IOException {
        init("$HOME/framework/utils/storage/redisPSCO/scripts/sample_hosts");

        if (clusterMode) {
            // let's do getByID stuff
            MyStorageObject myObject = new MyStorageObject("This is an object");
            myObject.makePersistent();
            Map<String, JedisPool> m = redisClusterConnection.getClusterNodes();
            for (String s : m.keySet()) {
                JedisPool jp = m.get(s);
                Jedis j = jp.getResource();
                System.out.println(j.info());
                // System.out.println(j.clusterInfo());
                // System.out.println(j.clusterNodes());
                break;
            }
        } else {
            // let's do standalone stuff
            MyStorageObject myObject = new MyStorageObject("This is an object");
            StorageItf.makePersistent(myObject, "prueba");
            Object retrieved = StorageItf.getByID("prueba");
            System.out.println(((MyStorageObject) retrieved).getInnerString());
            myObject.updatePersistent();
            StorageItf.removeById("prueba");
        }
    }

}
