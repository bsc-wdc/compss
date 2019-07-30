/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

import java.io.*;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.*;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;
import redis.clients.util.JedisClusterCRC16;
import storage.utils.Serializer;


public final class StorageItf {

    // Logger According to Loggers.STORAGE
    private static final Logger LOGGER = LogManager.getLogger("es.bsc.compss.Storage");

    // Error Messages
    private static final String ERROR_HOSTNAME = "ERROR_HOSTNAME";

    private static final String MASTER_HOSTNAME;

    // Redis variables

    // This port is the official Redis Port
    // See https://en.wikipedia.org/wiki/List_of_TCP_and_UDP_port_numbers
    // The storage API will assume that, given a hostname, there is a Redis Server listening there
    private static final int REDIS_PORT = 6379;
    private static final int REDIS_MAX_CLIENTS = 1 << 19;
    // Number of Redis hash slots. This is fixed, and official. See redis.io tutorials
    private static final int REDIS_MAX_HASH_SLOTS = 16384;
    // Client connections
    // Given that the client classes that are needed to establish a connection with a Redis backend are
    // different for standalone and cluster cases, we are going to first try to establish a connection with
    // the cluster client, and, if it fails, with the standalone client
    // Given that JedisCluster and Jedis are classes that share no common ancestor, this is the cleanest way I can
    // come up with.
    private static JedisCluster redisClusterConnection;
    private static JedisPool redisConnection;
    private static boolean clusterMode = true;

    private static List<String> hosts = new ArrayList<>();

    private static HashMap<String, String> previousVersion = new HashMap<>();

    // Given a hash slot, return a list with the hosts that contain at least one instance that includes
    // this slot in its slot interval
    private static ArrayList<String>[] hostsBySlot = new ArrayList[REDIS_MAX_HASH_SLOTS];

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
        LOGGER.info("[LOG] Configuration received: " + storageConf);
        try (BufferedReader br = new BufferedReader(new FileReader(storageConf))) {
            String line;
            while ((line = br.readLine()) != null) {
                hosts.add(line.trim());
                LOGGER.info("Adding " + line.trim() + " to list of known hosts...");
            }
        } catch (FileNotFoundException e) {
            throw new StorageException("Could not find configuration file", e);
        } catch (IOException e) {
            throw new StorageException("Could not open configuration file", e);
        }
        assert (!hosts.isEmpty());
        clusterMode = hosts.size() > 1;
        if (clusterMode) {
            LOGGER.info("More than one host detected, enabling Client Cluster Mode");
            // TODO: Ask Jedis guys why JedisCluster needs a HostAndPort and why Jedis needs a String and an Integer
            redisClusterConnection = new JedisCluster(new HostAndPort(hosts.get(0), REDIS_PORT));
            // Precompute host hashmap
            preComputeHostHashMap();
        } else {
            LOGGER.info("Only one host detect, using standalone client...");
            JedisPoolConfig poolConfig = new JedisPoolConfig();
            poolConfig.setMaxTotal(REDIS_MAX_CLIENTS);
            redisConnection = new JedisPool(poolConfig, hosts.get(0), REDIS_PORT);
        }
    }


    // Temporary representation of a host
    static private class Host {

        // Host (name)
        String host;
        // Hash slot endpoints
        int l, r;


        Host(String clusterInfoLine) {
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

        void printHostInfo() {
            System.out.printf("Host %s covers slots [%d, %d]\n", host, l, r);
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
            hostsBySlot[i] = new ArrayList<>(new TreeSet<>(validHosts));
        }
    }

    /**
     * Stops the persistent storage StorageItf
     * 
     * @throws StorageException
     */
    public static void finish() throws StorageException {
        if (clusterMode) {
            try {
                redisClusterConnection.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        } else {
            redisConnection.close();
        }
    }

    /**
     * Returns all the valid locations of a given id
     * 
     * @param id Object identifier
     * @return List of valid locations for given resource
     * @throws StorageException
     */
    public static List<String> getLocations(String id) throws StorageException {
        if (id != null && clusterMode) {
            int slot = JedisClusterCRC16.getSlot(id);
            return hostsBySlot[slot];
        } else {
            return hosts;
        }
    }

    /**
     * Creates a new replica of PSCO id @id in host @hostname
     * 
     * @param id
     * @param hostName
     * @throws StorageException
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

    /**
     * Create a new version of the PSCO id @id in the host @hostname Returns the id of the new version
     * 
     * @param id
     * @param hostName
     * @return
     * @throws StorageException
     */
    public static String newVersion(String id, boolean preserveSource, String hostName)
        throws StorageException, IOException, ClassNotFoundException {
        byte[] obj = getBytesByID(id);
        String newId = UUID.randomUUID().toString();
        previousVersion.put(newId, id);
        putInRedis(obj, newId);
        if (!preserveSource) {
            consolidateVersion(newId);
        }
        return newId;
    }

    /**
     * Returns the object with id @id This function retrieves the object from any location
     * 
     * @param id
     * @return
     * @throws StorageException
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
        byte[] ret =
            clusterMode ? redisClusterConnection.get(id.getBytes()) : redisConnection.getResource().get(id.getBytes());
        if (ret == null) {
            throw new StorageException("Object with id " + id + " is not in Redis!");
        }
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
    public static String executeTask(String id, String descriptor, Object[] values, String hostName,
        CallbackHandler callback) throws StorageException {
        throw new StorageException("Redis does not support this feature.");
    }

    /**
     * Retrieves the result of persistent storage execution
     * 
     * @param event
     * @return
     */
    public static Object getResult(CallbackEvent event) throws StorageException {
        throw new StorageException("Redis does not support this feature.");
    }

    /**
     * Consolidates all intermediate versions to the final id
     * 
     * @param idFinal
     * @throws StorageException
     */
    public static void consolidateVersion(String idFinal) throws StorageException {
        LOGGER.info("Consolidating version for " + idFinal);
        // Skip final version
        idFinal = previousVersion.get(idFinal);
        while (idFinal != null) {
            LOGGER.info("Removing version " + idFinal);
            removeById(idFinal);
            String oldId = idFinal;
            idFinal = previousVersion.get(idFinal);
            previousVersion.remove(oldId);
        }
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
        byte[] serializedObject = Serializer.serialize(o);
        String result = clusterMode ? redisClusterConnection.set(id.getBytes(), serializedObject)
            : redisConnection.getResource().set(id.getBytes(), serializedObject);
        if (!result.equals("OK")) {
            throw new StorageException("Redis returned an error while trying to store object with id " + id);
        }
    }

    /**
     * Removes all the occurrences of a given @id
     * 
     * @param id
     */
    public static void removeById(String id) {
        if (clusterMode) {
            redisClusterConnection.del(id.getBytes());
        } else {
            redisConnection.getResource().del(id.getBytes());
        }
    }


    // ONLY FOR TESTING PURPOSES
    static class MyStorageObject extends StorageObject implements Serializable {

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
     * 
     * @param args
     */
    public static void main(String[] args) throws ClassNotFoundException {
        try {
            init("/home/sergiorg/git/framework/utils/storage/redisPSCO/scripts/sample_hosts");
            if (clusterMode) {
                // let's do getByID stuff
                MyStorageObject myObject = new MyStorageObject("This is an object");
                myObject.makePersistent();
                Map<String, JedisPool> m = redisClusterConnection.getClusterNodes();
                for (String s : m.keySet()) {
                    JedisPool jp = m.get(s);
                    Jedis j = jp.getResource();
                    // System.out.println(j.info());
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
        } catch (StorageException | IOException e) {
            e.printStackTrace();
        }
    }

}
