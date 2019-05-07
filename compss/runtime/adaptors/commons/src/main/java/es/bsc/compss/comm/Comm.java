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
package es.bsc.compss.comm;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.StreamBackend;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.exceptions.NonInstantiableException;
import es.bsc.compss.types.resources.MasterResource;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.configuration.Configuration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.Classpath;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;
import es.bsc.distrostreamlib.client.DistroStreamClient;
import es.bsc.distrostreamlib.exceptions.DistroStreamClientInitException;
import es.bsc.distrostreamlib.requests.StopRequest;
import es.bsc.distrostreamlib.server.DistroStreamServer;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.Set;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;


/**
 * Representation of the Communication interface of the Runtime.
 */
public class Comm {

    // Log and debug
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Streaming
    private static final StreamBackend STREAMING_BACKEND;
    private static final int BASE_STREAMING_PORT = 49_049;
    private static final int STREAMING_PORT_RAND_RANGE = 100;
    private static final int STREAMING_PORT;

    // Storage
    private static final String STORAGE_CONF;

    // Adaptors
    private static final String ADAPTORS_REL_PATH = File.separator + "Runtime" + File.separator + "adaptors";
    private static final Map<String, CommAdaptor> ADAPTORS;

    // Logical data
    private static final Map<String, LogicalData> DATA;

    // Master information
    private static MasterResource appHost;

    static {
        String streamBackendProperty = System.getProperty(COMPSsConstants.STREAMING_BACKEND);
        String streamBackendPropertyFixed = (streamBackendProperty == null || streamBackendProperty.isEmpty()
                || streamBackendProperty.equals("null")) ? "NONE" : streamBackendProperty.toUpperCase();
        STREAMING_BACKEND = StreamBackend.valueOf(streamBackendPropertyFixed);
        STREAMING_PORT = BASE_STREAMING_PORT + new Random().nextInt(STREAMING_PORT_RAND_RANGE);

        String storageCfgProperty = System.getProperty(COMPSsConstants.STORAGE_CONF);
        STORAGE_CONF = (storageCfgProperty == null || storageCfgProperty.isEmpty() || storageCfgProperty.equals("null"))
                ? null
                : storageCfgProperty;

        ADAPTORS = new ConcurrentHashMap<>();

        DATA = Collections.synchronizedMap(new TreeMap<String, LogicalData>());
    }


    /**
     * Private constructor to avoid instantiation.
     */
    private Comm() {
        throw new NonInstantiableException("Comm");
    }

    /**
     * Communications initializer.
     *
     * @param master Master resource
     */
    public static void init(MasterResource master) {
        // Store master resource
        appHost = master;

        // Load communication adaptors
        loadAdaptorsJars();

        // Start tracing system
        if (System.getProperty(COMPSsConstants.TRACING) != null
                && Integer.parseInt(System.getProperty(COMPSsConstants.TRACING)) != 0) {
            int tracingLevel = Integer.parseInt(System.getProperty(COMPSsConstants.TRACING));
            LOGGER.debug("Tracing is activated [" + tracingLevel + ']');
            Tracer.init(tracingLevel);
            if (Tracer.extraeEnabled()) {
                Tracer.emitEvent(Tracer.Event.STATIC_IT.getId(), Tracer.Event.STATIC_IT.getType());
            }
        }

        // Start streaming library
        if (STREAMING_BACKEND.equals(StreamBackend.NONE)) {
            LOGGER.warn("No streaming backend passed");
        } else {
            LOGGER.info("Initializing DS Library for type " + STREAMING_BACKEND.name());
            // Server
            LOGGER.debug("Initializing Streaming Server");
            DistroStreamServer.initAndStart(master.getName(), STREAMING_PORT);
            // Client
            LOGGER.debug("Initializing Streaming Client");
            try {
                DistroStreamClient.initAndStart(master.getName(), STREAMING_PORT);
            } catch (DistroStreamClientInitException dscie) {
                ErrorManager.fatal("Error initializing DS client", dscie);
            }
            // Internal libraries if needed
            if (STREAMING_BACKEND.equals(StreamBackend.OBJECTS) || STREAMING_BACKEND.equals(StreamBackend.ALL)) {
                LOGGER.debug("Initializing Kafka Backend");
                // TODO: Start Kafka backend
            }
        }

        // Start storage interface
        if (STORAGE_CONF == null) {
            LOGGER.warn("No storage configuration file passed");
        } else {
            LOGGER.debug("Initializing Storage with: " + STORAGE_CONF);
            try {
                StorageItf.init(STORAGE_CONF);
            } catch (StorageException e) {
                ErrorManager.fatal("Error loading storage configuration file: " + STORAGE_CONF, e);
            }
        }
    }

    /**
     * Initializes the internal adaptor and constructs a comm configuration.
     *
     * @param adaptorName Adaptor name.
     * @param project_properties Properties from the project.xml file.
     * @param resources_properties Properties from the resources.xml file.
     * @return An adaptor configuration.
     * @throws ConstructConfigurationException When adaptor class cannot be instantiated.
     */
    public static Configuration constructConfiguration(String adaptorName, Object projectProperties,
            Object resourcesProperties) throws ConstructConfigurationException {

        // Check if adaptor has already been used
        CommAdaptor adaptor = ADAPTORS.get(adaptorName);
        if (adaptor == null) {
            // Create a new adaptor instance
            try {
                Constructor<?> constrAdaptor = Class.forName(adaptorName).getConstructor();
                adaptor = (CommAdaptor) constrAdaptor.newInstance();
            } catch (NoSuchMethodException | SecurityException | ClassNotFoundException | InstantiationException
                    | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {

                throw new ConstructConfigurationException(e);
            }

            // Initialize adaptor
            adaptor.init();

            // Add adaptor to used adaptors
            ADAPTORS.put(adaptorName, adaptor);
        }

        if (DEBUG) {
            LOGGER.debug("Adaptor Name: " + adaptorName);
        }

        // Construct properties
        return adaptor.constructConfiguration(projectProperties, resourcesProperties);
    }

    /**
     * Returns the resource assigned as master node.
     *
     * @return The resource assigned as master node.
     */
    public static MasterResource getAppHost() {
        return appHost;
    }

    /**
     * Returns the assigned streaming port.
     * 
     * @return The assigned streaming port.
     */
    public static int getStreamingPort() {
        return STREAMING_PORT;
    }

    /**
     * Returns the streaming backend.
     * 
     * @return The streaming backend.
     */
    public static StreamBackend getStreamingBackend() {
        return STREAMING_BACKEND;
    }

    /**
     * Initializes a worker with name {@code name} and configuration {@code config}.
     *
     * @param config Adaptor configuration.
     * @return A COMPSsWorker object representing the worker.
     */
    public static COMPSsWorker initWorker(Configuration config) {
        String adaptorName = config.getAdaptorName();
        CommAdaptor adaptor = ADAPTORS.get(adaptorName);
        return adaptor.initWorker(config);
    }

    /**
     * Stops the communication layer. Clean FTM, Job, {GATJob, NIOJob} and WSJob.
     */
    public static void stop() {
        appHost.deleteIntermediate();
        for (CommAdaptor adaptor : ADAPTORS.values()) {
            adaptor.stop();
        }

        // Stop streaming
        if (!STREAMING_BACKEND.equals(StreamBackend.NONE)) {
            LOGGER.info("Stopping Streaming...");

            LOGGER.debug("Stopping Streaming Client...");
            StopRequest stopRequest = new StopRequest();
            DistroStreamClient.request(stopRequest);
            stopRequest.waitProcessed();
            int errorCode = stopRequest.getErrorCode();
            if (errorCode != 0) {
                LOGGER.error("Error stopping Streaming Client");
                LOGGER.error("Error Code: " + errorCode);
                LOGGER.error("Error Message: " + stopRequest.getErrorMessage());
            }

            LOGGER.debug("Stopping Streaming Server...");
            DistroStreamServer.setStop();
        }

        // Stop Storage interface
        if (STORAGE_CONF != null) {
            try {
                LOGGER.info("Stopping Storage...");
                StorageItf.finish();
            } catch (StorageException se) {
                LOGGER.error("Error releasing storage library: " + se.getMessage(), se);
            }
        }

        // Stop tracing system
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
            Tracer.fini();
        }else if (Tracer.scorepEnabled() || Tracer.mapEnabled()) {
            Tracer.fini();
        }
    }

    /**
     * Registers a new data with id {@code dataId}.
     *
     * @param dataId Data Id.
     * @return The LogicalData representing the dataId after its registration.
     */
    public static synchronized LogicalData registerData(String dataId) {
        LOGGER.debug("Register new data " + dataId);

        LogicalData logicalData = new LogicalData(dataId);
        DATA.put(dataId, logicalData);

        return logicalData;
    }

    /**
     * Registers a new location {@code location} for the data with id {@code dataId}.
     *
     * @param dataId Data Id. Must exist previously.
     * @param location New location.
     * @return The updated LogicalData for the given dataId with the new location.
     */
    public static synchronized LogicalData registerLocation(String dataId, DataLocation location) {
        LOGGER.debug("Registering new Location for data " + dataId + ":");
        LOGGER.debug("  * Location: " + location);

        LogicalData logicalData = DATA.get(dataId);
        logicalData.addLocation(location);

        return logicalData;
    }

    /**
     * Registers a new value {@code value} for the data with id {@code dataId}.
     *
     * @param dataId Data Id. Must exist previously.
     * @param value New data value.
     * @return The updated LogicalData for the given dataId with the new value.
     */
    public static synchronized LogicalData registerValue(String dataId, Object value) {
        LOGGER.debug("Register value " + value + " for data " + dataId);

        String targetPath = Protocol.OBJECT_URI.getSchema() + dataId;
        DataLocation location = null;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            location = DataLocation.createLocation(appHost, uri);
        } catch (IOException e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
        }

        LogicalData logicalData = DATA.get(dataId);
        logicalData.addLocation(location);
        logicalData.setValue(value);

        // Register PSCO Location if needed it's PSCO and it's persisted
        if (value instanceof StubItf) {
            String id = ((StubItf) value).getID();
            if (id != null) {
                Comm.registerPSCO(dataId, id);
            }
        }

        return logicalData;
    }

    /**
     * Registers a new collection with the given dataId {@code dataId}.
     * 
     * @param dataId Identifier of the collection.
     * @param parameters Parameters of the collection.
     * @return LogicalData representing the collection with its parameters.
     */
    public static synchronized LogicalData registerCollection(String dataId, List<?> parameters) {
        return registerValue(dataId, parameters);
    }

    /**
     * Registers a new External PSCO id {@code id} for the data with id {@code dataId}.
     *
     * @param dataId Data Id. Must exist previously.
     * @param id PSCO Id.
     * @return The LogicalData representing the given data Id with the associated PSCO Id.
     */
    public static synchronized LogicalData registerExternalPSCO(String dataId, String id) {
        LogicalData ld = registerPSCO(dataId, id);
        ld.setValue(id);

        return ld;
    }

    /**
     * Registers a new Binding Object {@code bo} for the data with id {@code dataId}.
     *
     * @param dataId Data Id. Must exist previously.
     * @param bo Binding Object.
     * @return The LogicalData representing the given data Id with the associated Binding Object.
     */
    public static synchronized LogicalData registerBindingObject(String dataId, BindingObject bo) {
        String targetPath = Protocol.BINDING_URI.getSchema() + bo.toString();
        DataLocation location = null;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            location = DataLocation.createLocation(appHost, uri);
        } catch (IOException ioe) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, ioe);
        }

        LogicalData logicalData = DATA.get(dataId);
        logicalData.addLocation(location);
        logicalData.setValue(dataId + "#" + bo.getType() + "#" + bo.getElements());
        return logicalData;
    }

    /**
     * Registers a new PSCO id {@code id} for the data with id {@code dataId}.
     *
     * @param dataId Data Id. Must previously exist.
     * @param id PSCO Id.
     * @return The LogicalData after registering the PSCO Id into the given data Id.
     */
    public static synchronized LogicalData registerPSCO(String dataId, String id) {
        String targetPath = Protocol.PERSISTENT_URI.getSchema() + id;
        DataLocation location = null;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            location = DataLocation.createLocation(appHost, uri);
        } catch (IOException ioe) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, ioe);
        }

        LogicalData logicalData = DATA.get(dataId);
        logicalData.addLocation(location);

        return logicalData;
    }

    /**
     * Clears the value of the data id {@code dataId}.
     *
     * @param dataId Data Id.
     * @return The previously registered value.
     */
    public static synchronized Object clearValue(String dataId) {
        LOGGER.debug("Clear value of data " + dataId);
        LogicalData logicalData = DATA.get(dataId);

        return logicalData.removeValue();
    }

    /**
     * Checks if a given dataId {@code renaming} exists.
     *
     * @param renaming Data Id to check.
     * @return {@code true} if the {@code renaming} exists, {@code false} otherwise.
     */
    public static synchronized boolean existsData(String renaming) {
        return (DATA.get(renaming) != null);
    }

    /**
     * Returns the data with id {@code dataId}.
     *
     * @param dataId Data Id.
     * @return The associated LogicalData.
     */
    public static synchronized LogicalData getData(String dataId) {
        LogicalData retVal = DATA.get(dataId);
        if (retVal == null) {
            LOGGER.warn("Get data " + dataId + " is null.");
        }
        return retVal;
    }

    /**
     * Dumps the stored data (only for testing).
     *
     * @return String dump of all the currently registered data.
     */
    public static synchronized String dataDump() {
        StringBuilder sb = new StringBuilder("DATA DUMP\n");
        for (Entry<String, LogicalData> lde : DATA.entrySet()) {
            sb.append("\t *").append(lde.getKey()).append(":\n");
            LogicalData ld = lde.getValue();
            for (MultiURI u : ld.getURIs()) {
                sb.append("\t\t + ").append(u.toString()).append("\n");
                for (String adaptor : ADAPTORS.keySet()) {

                    Object internal = null;
                    try {
                        internal = u.getInternalURI(adaptor);
                        if (internal != null) {
                            sb.append("\t\t\t - ").append(internal.toString()).append("\n");
                        }
                    } catch (UnstartedNodeException une) {
                        // Node was not started. Cannot print internal object.
                    }
                }
            }
        }
        return sb.toString();
    }

    /**
     * Returns all the data stored in a host {@code host}.
     *
     * @param host Resource where to look for data.
     * @return Set of LogicalData stored in the given host.
     */
    public static Set<LogicalData> getAllData(Resource host) {
        // logger.debug("Get all data from host: " + host.getName());
        return host.getAllDataFromHost();
    }

    /**
     * Removes the data with id {@code renaming}.
     *
     * @param renaming Data Id.
     */
    public static synchronized void removeData(String renaming) {
        LOGGER.debug("Removing data " + renaming);

        LogicalData ld = DATA.remove(renaming);
        ld.isObsolete();
        for (DataLocation dl : ld.getLocations()) {
            MultiURI uri = dl.getURIInHost(appHost);
            if (uri != null) {
                File f = new File(uri.getPath());
                if (f.exists()) {
                    LOGGER.info("Deleting file " + f.getAbsolutePath());
                    if (!f.delete()) {
                        LOGGER.error("Cannot delete file " + f.getAbsolutePath());
                    }
                }
            }
        }
        ld.removeValue();

    }

    /**
     * Returns the active adaptors.
     *
     * @return A map containing the active adaptors.
     */
    public static Map<String, CommAdaptor> getAdaptors() {
        return ADAPTORS;
    }

    /**
     * Stops all the submitted jobs.
     */
    public static void stopSubmittedjobs() {
        for (CommAdaptor adaptor : ADAPTORS.values()) {
            adaptor.stopSubmittedJobs();
        }
    }

    /**
     * Loads the adaptors jars into the classpath.
     */
    private static void loadAdaptorsJars() {
        LOGGER.info("Loading Adaptors...");
        String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);

        if (compssHome == null || compssHome.isEmpty()) {
            LOGGER.warn("WARN: COMPSS_HOME not defined, no adaptors loaded.");
            return;
        }

        try {
            Classpath.loadPath(compssHome + ADAPTORS_REL_PATH, LOGGER);
        } catch (FileNotFoundException ex) {
            LOGGER.warn("WARN_MSG = [Adaptors folder not defined, no adaptors loaded.]");
        }
    }

}
