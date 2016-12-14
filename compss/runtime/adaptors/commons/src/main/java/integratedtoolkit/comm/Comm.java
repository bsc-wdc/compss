package integratedtoolkit.comm;

import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.exceptions.NonInstantiableException;
import integratedtoolkit.ITConstants;
import integratedtoolkit.exceptions.ConstructConfigurationException;
import integratedtoolkit.exceptions.UnstartedNodeException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;

import storage.StorageException;
import storage.StorageItf;
import storage.StubItf;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.resources.MasterResource;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.configuration.Configuration;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.Classpath;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.Tracer;

import java.io.File;
import java.io.FileNotFoundException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.TreeMap;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Representation of the Communication interface of the Runtime
 * 
 */
public class Comm {

    private static final String STORAGE_CONF = System.getProperty(ITConstants.IT_STORAGE_CONF);
    private static final String ADAPTORS_REL_PATH = File.separator + "Runtime" + File.separator + "adaptors";

    private static final HashMap<String, CommAdaptor> adaptors = new HashMap<>();

    // Log and debug
    protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
    private static final boolean debug = logger.isDebugEnabled();

    // Logical data
    private static Map<String, LogicalData> data = Collections.synchronizedMap(new TreeMap<String, LogicalData>());

    // Master information
    private static MasterResource appHost;
    
    
    /**
     * Private constructor to avoid instantiation
     */
    private Comm() {
        throw new NonInstantiableException("Comm");
    }

    /**
     * Communications initializer
     */
    public static synchronized void init() {
        appHost = new MasterResource();
        try {
            if (STORAGE_CONF == null || STORAGE_CONF.equals("") || STORAGE_CONF.equals("null")) {
                logger.warn("No storage configuration file passed");
            } else {
                logger.debug("Initializing Storage with: " + STORAGE_CONF);
                StorageItf.init(STORAGE_CONF);
            }
        } catch (StorageException e) {
            logger.fatal("Error loading storage configuration file: " + STORAGE_CONF, e);
            System.exit(1);
        }

        loadAdaptorsJars();
        /*
         * Initializes the Tracer activation value to enable querying Tracer.isActivated()
         */
        if (System.getProperty(ITConstants.IT_TRACING) != null && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0) {
            logger.debug("Tracing is activated");
            int tracing_level = Integer.parseInt(System.getProperty(ITConstants.IT_TRACING));
            Tracer.init(tracing_level);
            Tracer.emitEvent(Tracer.Event.STATIC_IT.getId(), Tracer.Event.STATIC_IT.getType());
        }

    }

    /**
     * Initializes the internal adaptor and constructs a comm configuration
     * 
     * @param adaptorName
     * @param project_properties
     * @param resources_properties
     * @return
     * @throws ConstructConfigurationException
     */
    public static synchronized Configuration constructConfiguration(String adaptorName, Object project_properties,
            Object resources_properties) throws ConstructConfigurationException {

        // Check if adaptor has already been used
        CommAdaptor adaptor = adaptors.get(adaptorName);
        if (adaptor == null) {
            // Create a new adaptor instance
            try {
                Constructor<?> constrAdaptor = Class.forName(adaptorName).getConstructor();
                adaptor = (CommAdaptor) constrAdaptor.newInstance();
            } catch (NoSuchMethodException | SecurityException | ClassNotFoundException | 
                    InstantiationException | IllegalAccessException | IllegalArgumentException | InvocationTargetException e) {
                
                throw new ConstructConfigurationException(e);
            }
            
            // Initialize adaptor
            adaptor.init();
            
            // Add adaptor to used adaptors
            adaptors.put(adaptorName, adaptor);
        }

        if (debug) {
            logger.debug("Adaptor Name: " + adaptorName);
        }

        // Construct properties
        return adaptor.constructConfiguration(project_properties, resources_properties);
    }
    
    /**
     * Returns the resource assigned as master node
     * 
     * @return
     */
    public static MasterResource getAppHost() {
        return appHost;
    }

    /**
     * Initializes a worker with name @name and configuration @config
     * 
     * @param name
     * @param config
     * @return
     */
    public static synchronized COMPSsWorker initWorker(String name, Configuration config) {
        String adaptorName = config.getAdaptorName();
        CommAdaptor adaptor = adaptors.get(adaptorName);
        return adaptor.initWorker(name, config);
    }

    /**
     * Stops the communication layer. 
     * Clean FTM, Job, {GATJob, NIOJob} and WSJob
     */
    public static synchronized void stop() {
        appHost.deleteIntermediate();
        for (CommAdaptor adaptor : adaptors.values()) {
            adaptor.stop();
        }

        // Stop Storage interface
        if (STORAGE_CONF != null && !STORAGE_CONF.equals("") && !STORAGE_CONF.equals("null")) {
            try {
                logger.debug("Stopping Storage...");
                StorageItf.finish();
            } catch (StorageException e) {
                logger.error("Error releasing storage library: " + e.getMessage());
            }
        }
        // Stop tracing system
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
            Tracer.fini();
        }
    }

    /**
     * Registers a new data with id @dataId
     * 
     * @param dataId
     * @return
     */
    public static synchronized LogicalData registerData(String dataId) {
        logger.debug("Register new data " + dataId);
        
        LogicalData logicalData = new LogicalData(dataId);
        data.put(dataId, logicalData);
        
        return logicalData;
    }

    /**
     * Registers a new location @location for the data with id @dataId
     * dataId must exist
     * 
     * @param dataId
     * @param location
     * @return
     */
    public static synchronized LogicalData registerLocation(String dataId, DataLocation location) {
        logger.debug("Registering new Location for data " + dataId + ":");
        logger.debug("  * Location: " + location);
        
        LogicalData logicalData = data.get(dataId);
        logicalData.addLocation(location);
        
        return logicalData;
    }

    /**
     * Registers a new value @value for the data with id @dataId
     * dataId must exist
     * 
     * @param dataId
     * @param value
     * @return
     */
    public static synchronized LogicalData registerValue(String dataId, Object value) {
        logger.debug("Register value " + value + " for data " + dataId);

        String targetPath = Protocol.OBJECT_URI.getSchema() + dataId;
        DataLocation location = null;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            location = DataLocation.createLocation(appHost, uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
        }

        LogicalData logicalData = data.get(dataId);
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
     * Registers a new PSCO id @id for the data with id @dataId
     * dataId must exist
     * 
     * @param dataId
     * @param id
     * @return
     */
    public static synchronized LogicalData registerPSCO(String dataId, String id) {
        String targetPath = Protocol.PERSISTENT_URI.getSchema() + id;
        DataLocation location = null;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            location = DataLocation.createLocation(appHost, uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
        }

        LogicalData logicalData = data.get(dataId);
        logicalData.addLocation(location);

        return logicalData;
    }

    /**
     * Clears the value of the data id @dataId
     * 
     * @param dataId
     * @return
     */
    public static synchronized Object clearValue(String dataId) {
        logger.debug("Clear value of data " + dataId);
        LogicalData logicalData = data.get(dataId);

        return logicalData.removeValue();
    }

    /**
     * Checks if a given dataId @renaming exists
     * 
     * @param renaming
     * @return
     */
    public static synchronized boolean existsData(String renaming) {
        return (data.get(renaming) != null);
    }

    /**
     * Returns the data with id @dataId
     * 
     * @param dataId
     * @return
     */
    public static synchronized LogicalData getData(String dataId) {
        if (logger.isDebugEnabled()) {
            logger.debug("Get data " + data.get(dataId));
        }

        return data.get(dataId);
    }

    /**
     * Dumps the stored data (only for testing)
     * 
     * @return
     */
    public static synchronized String dataDump() {
        StringBuilder sb = new StringBuilder("DATA DUMP\n");
        for (Map.Entry<String, LogicalData> lde : data.entrySet()) {
            sb.append("\t *").append(lde.getKey()).append(":\n");
            LogicalData ld = lde.getValue();
            for (MultiURI u : ld.getURIs()) {
                sb.append("\t\t + ").append(u.toString()).append("\n");
                for (String adaptor : adaptors.keySet()) {

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
     * Returns all the data stored in a host @host
     * 
     * @param host
     * @return
     */
    public static synchronized HashSet<LogicalData> getAllData(Resource host) {
        // logger.debug("Get all data from host: " + host.getName());
        return host.getAllDataFromHost();
    }

    /**
     * Removes the data with id @renaming
     * 
     * @param renaming
     */
    public static synchronized void removeData(String renaming) {
        logger.debug("Remove data " + renaming);

        LogicalData ld = data.remove(renaming);
        ld.isObsolete();
        for(DataLocation dl : ld.getLocations()){
        	MultiURI uri = dl.getURIInHost(appHost);
        	if (uri != null){
        		File f = new File (uri.getPath());
        		if (f.exists()){
        			logger.info("Deleting file " + f.getAbsolutePath());
        			if (!f.delete()) {
        			    logger.error("Cannot delete file " + f.getAbsolutePath());
        			}
        		}
        	}
        }
       
        
    }

    /**
     * Return the active adaptors
     * 
     * @return
     */
    public static synchronized HashMap<String, CommAdaptor> getAdaptors() {
        return adaptors;
    }

    /**
     * Stops all the submitted jobs
     * 
     */
    public static synchronized void stopSubmittedjobs() {
        for (CommAdaptor adaptor : adaptors.values()) {
            adaptor.stopSubmittedJobs();
        }
    }

    private static void loadAdaptorsJars() {
        logger.info("Loading Adaptors...");
        String itHome = System.getenv(ITConstants.IT_HOME);

        if (itHome == null || itHome.isEmpty()) {
            logger.warn("WARN: IT_HOME not defined, no adaptors loaded.");
            return;
        }

        try {
            Classpath.loadPath(itHome + ADAPTORS_REL_PATH, logger);
        } catch (FileNotFoundException ex) {
            logger.warn("WARN_MSG = [Adaptors folder not defined, no adaptors loaded.]");
        }
    }

}
