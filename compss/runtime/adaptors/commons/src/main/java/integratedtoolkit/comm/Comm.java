package integratedtoolkit.comm;

import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.ITConstants;
import java.lang.reflect.Constructor;

import org.apache.log4j.Logger;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.types.parameter.SCOParameter;
import integratedtoolkit.types.resources.MasterResource;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.SharedDiskManager;

import integratedtoolkit.util.Tracer;

import java.io.File;

import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.net.URL;
import java.net.URLClassLoader;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.List;

import java.util.Map;
import java.util.TreeMap;
import storage.StorageException;
import storage.StorageItf;

public class Comm {

    // Tracing
    protected static final boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;
    protected static final int tracing_level = Integer.parseInt(System.getProperty(ITConstants.IT_TRACING));

    private static final String defaultAdaptor = System.getProperty(ITConstants.COMM_ADAPTOR);
    private static final HashMap< String, CommAdaptor> adaptors = new HashMap< String, CommAdaptor>();

    // Log and debug
    protected static final Logger logger = Logger.getLogger(Loggers.COMM);
    private static final boolean debug = logger.isDebugEnabled();

    // Logical data
    private static Map<String, LogicalData> data = Collections.synchronizedMap(new TreeMap<String, LogicalData>());

    private static Map<Integer, PSCOId> pscoids = Collections.synchronizedMap(new TreeMap<Integer, PSCOId>());
    // Master information
    public static MasterResource appHost;

    // Communications initializer
    public static synchronized void init() {
        appHost = new MasterResource();
        try {
            URLClassLoader sysloader = (URLClassLoader) ClassLoader.getSystemClassLoader();
            Class<?> sysclass = URLClassLoader.class;
            String itHome = System.getenv("IT_HOME");
            Method method = sysclass.getDeclaredMethod("addURL", new Class[]{URL.class});
            method.setAccessible(true);
            File directory = new File(itHome + File.separator + "adaptors");
            File[] fList = directory.listFiles();
            for (File f : fList) {
                File adaptorMasterDir = new File(f.getAbsolutePath() + File.separator + "master");
                File[] jarList = adaptorMasterDir.listFiles();
                for (File jar : jarList) {
                    try {
                        method.invoke(sysloader, new Object[]{(new File(jar.getAbsolutePath())).toURI().toURL()});
                    } catch (Exception e) {
                        logger.error("COULD NOT LOAD ADAPTOR JAR " + jar.getAbsolutePath(), e);
                    }
                }
            }
        } catch (Exception e) {
            logger.error("CAN NOT LOAD ANY ADAPTOR ", e);
        }

        if (tracing) {
            Tracer.init(tracing_level);
            Tracer.masterEventStart(Tracer.Event.STATIC_IT.getId());
        }
    }

    public static synchronized void addSharedDiskToMaster(String diskName, String mountPoint) {
        SharedDiskManager.addSharedToMachine(diskName, mountPoint, appHost);
    }

    public static synchronized COMPSsWorker initWorker(String name, HashMap<String, String> properties, TreeMap<String, AdaptorDescription> adaptorsDesc) throws Exception {
        String adaptorName = defaultAdaptor;
        // WS are treated separately
        if (adaptorsDesc.keySet().contains(AdaptorDescription.WSAdaptor)) {
            adaptorName = AdaptorDescription.WSAdaptor;
        }

        // Init adaptdor
        CommAdaptor adaptor = adaptors.get(adaptorName);
        if (adaptor == null) {
            try {
                Constructor<?> constrAdaptor = Class.forName(adaptorName).getConstructor();
                adaptor = (CommAdaptor) constrAdaptor.newInstance();
            } catch (ClassNotFoundException e) {
                logger.error("Can not find adaptor class " + adaptorName + ".", e);
            } catch (NoSuchMethodException e) {
                logger.error("Class " + adaptorName + " has no valid constructor.", e);
            } catch (InstantiationException e) {
                logger.error("Can not instantiate adaptor " + adaptorName + ".", e);
            } catch (IllegalAccessException e) {
                logger.error("Illegal access on adaptor " + adaptorName + " creation", e);
            } catch (IllegalArgumentException e) {
                logger.error("Illegal argument on adaptor " + adaptorName + " creation", e);
            } catch (InvocationTargetException e) {
                logger.error("Wrong target for " + adaptorName + " invocation", e);
            } catch (Exception e) {
                logger.error(e);
            }
            adaptor.init();
            adaptors.put(adaptorName, adaptor);
        }

        if (debug) {
            logger.debug("Adaptor Name: " + adaptorName);
            logger.debug("Adaptors Keys: " + adaptorsDesc.keySet());
        }

        // Check adaptor properties if needed
        COMPSsWorker worker = null;
        AdaptorDescription ad = adaptorsDesc.get(adaptorName);
        if (ad != null && !adaptorName.equals(AdaptorDescription.WSAdaptor)) {
            logger.debug("Checking adaptor properties...");
            ad.checkAdaptorProperties();
        }

        // Init worker
        logger.debug("Init worker adaptor");
        worker = adaptor.initWorker(name, properties, adaptorsDesc);

        return worker;
    }

    // Clean FTM, Job, {GATJob, NIOJob} and WSJob
    public static synchronized void stop() {
        for (CommAdaptor adaptor : adaptors.values()) {
            adaptor.stop();
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
        logger.debug("Register value " + value + "for data " + dataId);
        DataLocation location = DataLocation.getLocation(appHost, dataId);

        LogicalData logicalData = data.get(dataId);
        logicalData.addLocationAndValue(location, value);

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
        //logger.debug("Get data " + dataId + " with value " + data.get(dataId));

        return data.get(dataId);
    }

    public static synchronized String dataDump() {
        StringBuilder sb = new StringBuilder("DATA DUMP\n");
        for (Map.Entry<String, LogicalData> lde : data.entrySet()) {
            sb.append("\t *").append(lde.getKey()).append(":\n");
            LogicalData ld = lde.getValue();
            for (URI u : ld.getURIs()) {
                sb.append("\t\t + ").append(u.toString()).append("\n");
                for (String adaptor : adaptors.keySet()) {
                    Object internal = u.getInternalURI(adaptor);
                    if (internal != null) {
                        sb.append("\t\t\t - ").append(internal.toString()).append("\n");
                    }
                }
            }
        }
        return sb.toString();
    }

    public static synchronized HashSet<LogicalData> getAllData(Resource host) {
        //logger.debug("Get all data from host: " + host.getName());

        return LogicalData.getAllDataFromHost(host);
    }

    public static synchronized void removeData(String renaming) {
        logger.debug("Remove data " + renaming);

        LogicalData ld = data.remove(renaming);
        ld.isObsolete();
    }

    public static synchronized HashMap< String, CommAdaptor> getAdaptors() {
        return adaptors;
    }

    public static synchronized void stopSubmittedjobs() {
        for (CommAdaptor adaptor : adaptors.values()) {
            adaptor.stopSubmittedJobs();
        }
    }

    public static synchronized void registerPSCOId(int id, PSCOId pscoid) {
        pscoids.put(id, pscoid);
    }

    public static synchronized PSCOId getPSCOId(int id) {
        return pscoids.get(id);
    }

    public static synchronized PSCOId removePSCOId(int id) {
        return pscoids.remove(id);
    }

    public static synchronized List<String> getPSCOLocations(SCOParameter p) {
        List<String> backends = new LinkedList<String>();

        Integer id = p.getCode();
        if (!(p.getValue() instanceof PSCOId)) {
            // Check if the SCOParamter has a PSCOId not updated yet.
            PSCOId pscoId = Comm.removePSCOId(id);
            p.setValue(pscoId);
        }

        if ((p.getValue() instanceof PSCOId)) {
            String pscoId = ((PSCOId) (p.getValue())).getId();
            try {
                if (tracing) {
                    Tracer.masterEventStart(Tracer.Event.STORAGE_GETLOCATIONS.getId());
                }
                backends = StorageItf.getLocations(pscoId);
                if (tracing) {
                    Tracer.masterEventFinish();
                }
            } catch (StorageException e) {
                backends = new LinkedList<String>();
                ErrorManager.warn(e.getMessage());
                if (tracing) {
                    Tracer.masterEventFinish();
                }
            }
            ((PSCOId) p.getValue()).setBackends(backends);
        }

        return backends;
    }
}
