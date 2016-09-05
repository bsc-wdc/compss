package integratedtoolkit.comm;

import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.ITConstants;
import integratedtoolkit.exceptions.UnstartedNodeException;

import java.lang.reflect.Constructor;

import storage.StorageException;
import storage.StorageItf;
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


public class Comm {

	private static final String STORAGE_CONF = System.getProperty(ITConstants.IT_STORAGE_CONF);
	private static final String ADAPTORS_REL_PATH = File.separator + "Runtime" + File.separator + "adaptors";

	private static final HashMap<String, CommAdaptor> adaptors = new HashMap<String, CommAdaptor>();

	// Log and debug
	protected static final Logger logger = LogManager.getLogger(Loggers.COMM);
	private static final boolean debug = logger.isDebugEnabled();

	// Logical data
	private static Map<String, LogicalData> data = Collections.synchronizedMap(new TreeMap<String, LogicalData>());

	// Master information
	public static MasterResource appHost;


	// Communications initializer
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

		if (Tracer.isActivated()){
			Tracer.emitEvent(Tracer.Event.STATIC_IT.getId(), Tracer.Event.STATIC_IT.getType());
		}

	}

	public static synchronized Configuration constructConfiguration(String adaptorName, Object project_properties,
			Object resources_properties) throws Exception {

		// Init adaptor
		CommAdaptor adaptor = adaptors.get(adaptorName);
		if (adaptor == null) {
			Constructor<?> constrAdaptor = Class.forName(adaptorName).getConstructor();
			adaptor = (CommAdaptor) constrAdaptor.newInstance();
			adaptor.init();
			adaptors.put(adaptorName, adaptor);
		}

		if (debug) {
			logger.debug("Adaptor Name: " + adaptorName);
		}

		// Construct properties
		return adaptor.constructConfiguration(project_properties, resources_properties);
	}

	public static synchronized COMPSsWorker initWorker(String name, Configuration config) {
		String adaptorName = config.getAdaptorName();
		CommAdaptor adaptor = adaptors.get(adaptorName);
		return adaptor.initWorker(name, config);
	}

	// Clean FTM, Job, {GATJob, NIOJob} and WSJob
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

		if (Tracer.isActivated()){
			Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
			Tracer.fini();
		}
	}

	public static synchronized LogicalData registerData(String dataId) {
		logger.debug("Register new data " + dataId);
		LogicalData logicalData = new LogicalData(dataId);
		data.put(dataId, logicalData);
		return logicalData;
	}

	public static synchronized LogicalData registerLocation(String dataId, DataLocation location) {
		logger.debug("Registering new Location for data " + dataId + ":");
		logger.debug("  * Location: " + location);
		LogicalData logicalData = data.get(dataId);
		logicalData.addLocation(location);
		return logicalData;
	}

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

		return logicalData;
	}

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

	public static synchronized Object clearValue(String dataId) {
		logger.debug("Clear value of data " + dataId);
		LogicalData logicalData = data.get(dataId);

		return logicalData.removeValue();
	}

	public static synchronized boolean existsData(String renaming) {
		return (data.get(renaming) != null);
	}

	public static synchronized LogicalData getData(String dataId) {
		if (logger.isDebugEnabled()) {
			logger.debug("Get data " + data.get(dataId));
		}

		return data.get(dataId);
	}

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

	public static synchronized HashSet<LogicalData> getAllData(Resource host) {
		// logger.debug("Get all data from host: " + host.getName());
		return host.getAllDataFromHost();
	}

	public static synchronized void removeData(String renaming) {
		logger.debug("Remove data " + renaming);

		LogicalData ld = data.remove(renaming);
		ld.isObsolete();
	}

	public static synchronized HashMap<String, CommAdaptor> getAdaptors() {
		return adaptors;
	}

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
