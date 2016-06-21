package integratedtoolkit.api.impl;

import java.io.File;
import java.io.InputStream;
import java.util.Properties;

import integratedtoolkit.ITConstants;
import integratedtoolkit.api.COMPSsRuntime;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.components.impl.AccessProcessor;
import integratedtoolkit.components.impl.TaskDispatcher;
import integratedtoolkit.components.monitor.impl.GraphGenerator;
import integratedtoolkit.components.monitor.impl.RuntimeMonitor;
import integratedtoolkit.loader.LoaderAPI;
import integratedtoolkit.loader.LoaderUtils;
import integratedtoolkit.loader.total.ObjectRegistry;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.MethodImplementation;
import integratedtoolkit.types.data.AccessParams.AccessMode;
import integratedtoolkit.types.data.AccessParams.FileAccessParams;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.parameter.BasicTypeParameter;
import integratedtoolkit.types.parameter.FileParameter;
import integratedtoolkit.types.parameter.ObjectParameter;
import integratedtoolkit.types.parameter.PSCOId;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.parameter.SCOParameter;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.RuntimeConfigManager;
import integratedtoolkit.util.Tracer;

import org.apache.log4j.Logger;
import org.apache.log4j.PropertyConfigurator;

import storage.StubItf;


public class COMPSsRuntimeImpl implements COMPSsRuntime, LoaderAPI {

    // Exception constants definition
    protected static final String WARN_IT_FILE_NOT_READ = "WARNING: IT Properties file could not be read";
    protected static final String WARN_LOG4J_FILE_NOT_READ = "WARNING: Log4j Properties file could not be read.\nNo logging in files will be available";
    protected static final String WARN_FILE_EMPTY_DEFAULT = "WARNING: IT Properties file is null. Setting default values";
    protected static final String WARN_VERSION_PROPERTIES = "WARNING: COMPSs Runtime VERSION-BUILD properties file could not be read";
    protected static final String ERROR_FILE_NAME = "ERROR: Cannot parse file name";
    protected static final String ERROR_OBJECT_SERIALIZE = "ERROR: Cannot serialize object to file";
    protected static final String ERROR_OBJECT_DESERIALIZE = "ERROR: Cannot deserialize object from file";
    protected static final String WARN_WRONG_DIRECTION = "ERROR: Invalid parameter direction: ";

    // Constants
    protected static final String FILE_URI = "file:";
    protected static final String SHARED_URI = "shared:";

    // COMPSs Version and buildnumber attributes
    protected static String COMPSs_VERSION = null;
    protected static String COMPSs_BUILDNUMBER = null;

    // Components
    protected static AccessProcessor ap;
    protected static TaskDispatcher td;

    // Application attributes and directories
    public static String appName;

    public static boolean initialized = false;

    // Object registry
    protected static ObjectRegistry oReg;

    //Monitor
    protected static GraphGenerator graphMonitor;
    protected static RuntimeMonitor runtimeMonitor;

    // Logger
    protected static Logger logger = null;

    // Tracing
    protected static boolean tracing = System.getProperty(ITConstants.IT_TRACING) != null
            && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0;

    static {
        // Load Runtime configuration parameters
        String properties_loc = System.getProperty(ITConstants.IT_CONFIG_LOCATION);
        if (properties_loc == null) {
            InputStream stream = findPropertiesConfigFile();
            if (stream != null) {
                try {
                    setPropertiesFromRuntime(new RuntimeConfigManager(stream));
                } catch (Exception e) {
                    System.err.println(WARN_IT_FILE_NOT_READ);
                    e.printStackTrace();
                }
            } else {
                setDefaultProperties();
            }
        } else {
            try {
                setPropertiesFromRuntime(new RuntimeConfigManager(properties_loc));
            } catch (Exception e) {
                System.err.println(WARN_IT_FILE_NOT_READ);
                e.printStackTrace();
            }
        }

        Comm.init();
        /*
         * Configures log4j for the JVM where the application and the IT API belong
         */
        logger = Logger.getLogger(Loggers.API);
        String log4j = System.getProperty(ITConstants.LOG4J);
        if (log4j!= null){
        	PropertyConfigurator.configure(System.getProperty(ITConstants.LOG4J));
        }else{
        	System.err.println(WARN_LOG4J_FILE_NOT_READ);
        }
    }

    //Code Added to support configuration files
    private static void setPropertiesFromRuntime(RuntimeConfigManager manager) {
        try {
            if (manager != null) {
                if (manager.getDeploymentId() != null && System.getProperty(ITConstants.IT_DEPLOYMENT_ID) == null) {
                    System.setProperty(ITConstants.IT_DEPLOYMENT_ID, manager.getDeploymentId());
                }
                if (manager.getMasterPort() != null && System.getProperty(ITConstants.IT_MASTER_PORT) == null) {
                    System.setProperty(ITConstants.IT_MASTER_PORT, manager.getMasterPort());
                }
                if (manager.getAppName() != null && System.getProperty(ITConstants.IT_APP_NAME) == null) {
                    System.setProperty(ITConstants.IT_APP_NAME, manager.getAppName());
                }
                if (manager.getCOMPSsBaseLogDir() != null && System.getProperty(ITConstants.IT_BASE_LOG_DIR) == null) {
                    System.setProperty(ITConstants.IT_BASE_LOG_DIR, manager.getCOMPSsBaseLogDir());
                }
                if (manager.getSpecificLogDir() != null && System.getProperty(ITConstants.IT_SPECIFIC_LOG_DIR) == null) {
                    System.setProperty(ITConstants.IT_SPECIFIC_LOG_DIR, manager.getSpecificLogDir());
                }
                if (manager.getLog4jConfiguration() != null && System.getProperty(ITConstants.LOG4J) == null) {
                    System.setProperty(ITConstants.LOG4J, manager.getLog4jConfiguration());
                }
                if (manager.getResourcesFile() != null && System.getProperty(ITConstants.IT_RES_FILE) == null) {
                    System.setProperty(ITConstants.IT_RES_FILE, manager.getResourcesFile());
                }
                if (manager.getResourcesSchema() != null && System.getProperty(ITConstants.IT_RES_SCHEMA) == null) {
                    System.setProperty(ITConstants.IT_RES_SCHEMA, manager.getResourcesSchema());
                }
                if (manager.getProjectFile() != null && System.getProperty(ITConstants.IT_PROJ_FILE) == null) {
                    System.setProperty(ITConstants.IT_PROJ_FILE, manager.getProjectFile());
                }
                if (manager.getProjectSchema() != null && System.getProperty(ITConstants.IT_PROJ_SCHEMA) == null) {
                    System.setProperty(ITConstants.IT_PROJ_SCHEMA, manager.getProjectSchema());
                }

                if (manager.getScheduler() != null && System.getProperty(ITConstants.IT_SCHEDULER) == null) {
                    System.setProperty(ITConstants.IT_SCHEDULER, manager.getScheduler());
                }
                if (manager.getMonitorInterval() > 0 && System.getProperty(ITConstants.IT_MONITOR) == null) {
                    System.setProperty(ITConstants.IT_MONITOR, Long.toString(manager.getMonitorInterval()));
                }
                if (manager.getGATAdaptor() != null && System.getProperty(ITConstants.GAT_ADAPTOR_PATH) == null) {
                    System.setProperty(ITConstants.GAT_ADAPTOR_PATH, manager.getGATAdaptor());
                }
                if (manager.getGATBrokerAdaptor() != null && System.getProperty(ITConstants.GAT_BROKER_ADAPTOR) == null) {
                    System.setProperty(ITConstants.GAT_BROKER_ADAPTOR, manager.getGATBrokerAdaptor());
                }
                if (manager.getGATFileAdaptor() != null && System.getProperty(ITConstants.GAT_FILE_ADAPTOR) == null) {
                    System.setProperty(ITConstants.GAT_FILE_ADAPTOR, manager.getGATFileAdaptor());
                }
                if (manager.getWorkerCP() != null && System.getProperty(ITConstants.IT_WORKER_CP) == null) {
                    System.setProperty(ITConstants.IT_WORKER_CP, manager.getWorkerCP());
                }
                if (manager.getServiceName() != null && System.getProperty(ITConstants.IT_SERVICE_NAME) == null) {
                    System.setProperty(ITConstants.IT_SERVICE_NAME, manager.getServiceName());
                }
                if (System.getProperty(ITConstants.COMM_ADAPTOR) == null) {
                    if (manager.getCommAdaptor() != null) {
                        System.setProperty(ITConstants.COMM_ADAPTOR, manager.getCommAdaptor());
                    } else {
                        System.setProperty(ITConstants.COMM_ADAPTOR, ITConstants.DEFAULT_ADAPTOR);
                    }
                }
                if (System.getProperty(ITConstants.GAT_DEBUG) == null) {
                    System.setProperty(ITConstants.GAT_DEBUG, Boolean.toString(manager.isGATDebug()));
                }
                if (System.getProperty(ITConstants.IT_LANG) == null) {
                    System.setProperty(ITConstants.IT_LANG, manager.getLang());
                }
                if (System.getProperty(ITConstants.IT_GRAPH) == null) {
                    System.setProperty(ITConstants.IT_GRAPH, Boolean.toString(manager.isGraph()));
                }
                if (System.getProperty(ITConstants.IT_TRACING) == null) {
                    System.setProperty(ITConstants.IT_TRACING, String.valueOf(manager.getTracing()));
                }
                if (System.getProperty(ITConstants.IT_PRESCHED) == null) {
                    System.setProperty(ITConstants.IT_PRESCHED, Boolean.toString(manager.isPresched()));
                }

                if (manager.getContext() != null) {
                    System.setProperty(ITConstants.IT_CONTEXT, manager.getContext());
                }
                System.setProperty(ITConstants.IT_TO_FILE, Boolean.toString(manager.isToFile()));
            } else {
                setDefaultProperties();
            }
        } catch (Exception e) {
            System.err.println(WARN_IT_FILE_NOT_READ);
            e.printStackTrace();
        }
    }

    private static void setDefaultProperties() {
        System.err.println(WARN_FILE_EMPTY_DEFAULT);
        if (System.getProperty(ITConstants.IT_DEPLOYMENT_ID) == null || System.getProperty(ITConstants.IT_DEPLOYMENT_ID).equals("")) {
            System.setProperty(ITConstants.IT_DEPLOYMENT_ID, ITConstants.DEFAULT_DEPLOYMENT_ID);
        }
        if (System.getProperty(ITConstants.IT_RES_SCHEMA) == null || System.getProperty(ITConstants.IT_RES_SCHEMA).equals("")) {
            System.setProperty(ITConstants.IT_RES_SCHEMA, ITConstants.DEFAULT_RES_SCHEMA);
        }
        if (System.getProperty(ITConstants.IT_PROJ_SCHEMA) == null || System.getProperty(ITConstants.IT_PROJ_SCHEMA).equals("")) {
            System.setProperty(ITConstants.IT_PROJ_SCHEMA, ITConstants.DEFAULT_PROJECT_SCHEMA);
        }
        if (System.getProperty(ITConstants.GAT_ADAPTOR_PATH) == null || System.getProperty(ITConstants.GAT_ADAPTOR_PATH).equals("")) {
            System.setProperty(ITConstants.GAT_ADAPTOR_PATH, ITConstants.DEFAULT_GAT_ADAPTOR_LOCATION);
        }
        if (System.getProperty(ITConstants.COMM_ADAPTOR) == null || System.getProperty(ITConstants.COMM_ADAPTOR).equals("")) {
            System.setProperty(ITConstants.COMM_ADAPTOR, ITConstants.DEFAULT_ADAPTOR);
        }
        if (System.getProperty(ITConstants.IT_SCHEDULER) == null || System.getProperty(ITConstants.IT_SCHEDULER).equals("")) {
            System.setProperty(ITConstants.IT_SCHEDULER, ITConstants.DEFAULT_SCHEDULER);
        }
        if (System.getProperty(ITConstants.IT_TRACING) == null || System.getProperty(ITConstants.IT_TRACING).equals("")) {
            System.setProperty(ITConstants.IT_TRACING, ITConstants.DEFAULT_TRACING);
        }
    }

    private static InputStream findPropertiesConfigFile() {
        InputStream stream = COMPSsRuntimeImpl.class.getResourceAsStream(ITConstants.IT_CONFIG);
        if (stream != null) {
            return stream;
        } else {
            stream = COMPSsRuntimeImpl.class.getResourceAsStream(File.separator + ITConstants.IT_CONFIG);
            if (stream != null) {
                return stream;
            } else {
                //System.err.println("IT properties file not defined. Looking at classLoader ...");
                stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(ITConstants.IT_CONFIG);
                if (stream != null) {
                    return stream;
                } else {
                    stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(File.separator + ITConstants.IT_CONFIG);
                    if (stream != null) {
                        return stream;
                    } else {
                        //System.err.println("IT properties file not found in classloader. Looking at system resource ...");
                        stream = ClassLoader.getSystemResourceAsStream(ITConstants.IT_CONFIG);
                        if (stream != null) {
                            return stream;
                        } else {
                            stream = ClassLoader.getSystemResourceAsStream(File.separator + ITConstants.IT_CONFIG);
                            if (stream != null) {
                                return stream;
                            } else {
                                //System.err.println("IT properties file not found. Looking at parent ClassLoader");
                                stream = COMPSsRuntimeImpl.class.getClassLoader().getParent().getResourceAsStream(ITConstants.IT_CONFIG);
                                if (stream != null) {
                                    return stream;
                                } else {
                                    stream = COMPSsRuntimeImpl.class.getClassLoader().getParent().getResourceAsStream(File.separator + ITConstants.IT_CONFIG);
                                    if (stream != null) {
                                        return stream;
                                    } else {
                                        //System.err.println("IT properties file not found");
                                        return null;
                                    }
                                }
                            }
                        }
                    }
                }
            }
        }
    }
    
    /* ****************************************************
     * CONSTRUCTOR
     * ****************************************************/
    public COMPSsRuntimeImpl() {
        // Load COMPSs version and buildNumber
        try {
            Properties props = new Properties();
            props.load(this.getClass().getResourceAsStream("/version.properties"));
            COMPSs_VERSION = props.getProperty("compss.version");
            COMPSs_BUILDNUMBER = props.getProperty("compss.build");
        } catch (Exception e) {
            logger.warn(WARN_VERSION_PROPERTIES);
        }

        if (COMPSs_VERSION == null) {
            logger.info("Deploying COMPSs Runtime");
        } else if (COMPSs_BUILDNUMBER == null) {
            logger.info("Deploying COMPSs Runtime v" + COMPSs_VERSION);
        } else if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
            COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
            logger.info("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        } else {
            logger.info("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        }

        if (System.getProperty(ITConstants.IT_TRACING) != null
                && Integer.parseInt(System.getProperty(ITConstants.IT_TRACING)) > 0) {
            logger.debug("Tracing is activated");
        }

        ErrorManager.init(this);
    }

    
    /* ****************************************************
     * COMPSsRuntime INTERFACE IMPLEMENTATION
     * ****************************************************/
    /**
     * Starts the COMPSs Runtime
     * 
     */
    public synchronized void startIT() {
    	if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
            Tracer.emitEvent(Tracer.Event.START.getId(), Tracer.Event.START.getType());
        }

        // Console Log
        Thread.currentThread().setName("APPLICATION");
        if (COMPSs_VERSION == null) {
            logger.info("Starting COMPSs Runtime");
        } else if (COMPSs_BUILDNUMBER == null) {
            logger.info("Starting COMPSs Runtime v" + COMPSs_VERSION);
        } else if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
            COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
            logger.info("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        } else {
            logger.info("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        }

        // Init Runtime
        if (!initialized) {
            // Application
            synchronized (this) {
                logger.debug("Initializing components");

                td = new TaskDispatcher();
                ap = new AccessProcessor(td);

                if (GraphGenerator.isEnabled()) {
                    graphMonitor = new GraphGenerator();
                    ap.setGM(graphMonitor);
                }
                if (RuntimeMonitor.isEnabled()) {
                    runtimeMonitor = new RuntimeMonitor(ap, td, graphMonitor, Long.parseLong(System.getProperty(ITConstants.IT_MONITOR)));
                }

                // Python and C++
                String lang = System.getProperty(ITConstants.IT_LANG);
                if (! lang.toUpperCase().equals(ITConstants.Lang.JAVA.toString()) ) {
                    this.setObjectRegistry(new ObjectRegistry(this));
                }

                initialized = true;
                logger.debug("Ready to process tasks");
            }
        } else {
            // Service
            String className = Thread.currentThread().getStackTrace()[2].getClassName();
            logger.debug("Initializing " + className + "Itf");
            try {
                td.addInterface(Class.forName(className + "Itf"));
            } catch (Exception e) {
                ErrorManager.fatal("Error adding interface " + className + "Itf");
            }
        }

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

    }

    /**
     * Stops the COMPSsRuntime
     * 
     */
    public void stopIT(boolean terminate) {
        synchronized (this) {
            if (tracing) {
                Tracer.emitEvent(Tracer.Event.STOP.getId(), Tracer.Event.STOP.getType());
            }


            // Stop monitor components
            logger.debug("Stop IT reached");
            if (GraphGenerator.isEnabled()) {
                logger.debug("Stopping Graph generation...");
                // Graph commited by noMoreTasks, nothing to do
            }
            if (RuntimeMonitor.isEnabled()) {
                logger.debug("Stopping Monitor...");
                runtimeMonitor.shutdown();
            }

            // Stop runtime components
            logger.debug("Stopping AP...");
            if (ap != null) {
                ap.shutdown();
            } else {
                logger.debug("AP was not initialized...");
            }

            logger.debug("Stopping TD...");
            if (td != null) {
                td.shutdown();
            } else {
                logger.debug("TD was not initialized...");
            }

            logger.debug("Stopping Comm...");
            Comm.stop();
            logger.debug("Runtime stopped");
            
        }
        logger.info("Execution Finished");
    }

    /**
     * Returns the Application Directory
     * 
     */
    public String getApplicationDirectory() {
        return Comm.appHost.getAppLogDirPath();
    }
	
    
    /**
     * Registers a new CoreElement in the COMPSs Runtime
     */
    public void registerCE(String methodClass, String methodName, boolean hasTarget, boolean hasReturn, String constraints,
            int parameterCount, Object... parameters) {

        logger.debug("\nRegister CE parameters:");
        logger.debug("\tMethodClass: " + methodClass);
        logger.debug("\tMethodName: " + methodName);
        logger.debug("\tHasTarget: " + hasTarget);
        logger.debug("\tHasReturn: " + hasReturn);
        logger.debug("\tConstraints: " + constraints);
        logger.debug("\tParameters:");
        for (Object o : parameters) {
            logger.debug("\t: " + o.toString());
        }

        MethodResourceDescription mrd = new MethodResourceDescription(constraints);
        Parameter[] params = processParameters(parameterCount, parameters);

        String signature = MethodImplementation.getSignature(methodClass, methodName, hasTarget, hasReturn, params);

        td.registerCEI(signature, methodClass, mrd);
    }

    /**
     * Execute task
     * 
     */
    public int executeTask(Long appId, String methodClass, String methodName, boolean priority, boolean hasTarget, int parameterCount,
            Object... parameters) {

        if (tracing) {
            Tracer.emitEvent(Tracer.Event.TASK.getId(), Tracer.Event.TASK.getType());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Creating task from method " + methodName + " in " + methodClass);
            logger.debug("There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter" + (parameterCount > 1 ? "s" : ""));
        }

        Parameter[] pars = processParameters(parameterCount, parameters);
        int task = ap.newTask(appId, methodClass, methodName, priority, hasTarget, pars);

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return task;
    }

    /**
     * Execute task
     * 
     */
    public int executeTask(Long appId, String namespace, String service, String port, String operation, boolean priority, boolean hasTarget,
            int parameterCount, Object... parameters) {

        if (tracing) {
        	Tracer.emitEvent(Tracer.Event.TASK.getId(), Tracer.Event.TASK.getType());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Creating task from service " + service + ", namespace " + namespace + ", port " + port + ", operation " + operation);
            logger.debug("There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter" + (parameterCount > 1 ? "s" : ""));
        }

        Parameter[] pars = processParameters(parameterCount, parameters);
        int task = ap.newTask(appId, namespace, service, port, operation, priority, hasTarget, pars);

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return task;
    }

    /**
     * Notifies the Runtime that there are no more tasks created by the current appId
     * 
     */
    public void noMoreTasks(Long appId, boolean terminate) {
        if (tracing) {
            Tracer.emitEvent(Tracer.Event.NO_MORE_TASKS.getId(), Tracer.Event.NO_MORE_TASKS.getType());
        }

        logger.info("No more tasks for app " + appId);
        // Wait until all tasks have finished
        ap.noMoreTasks(appId);
        // Retrieve result files
        logger.info("Getting Result Files " + appId);
        ap.getResultFiles(appId);

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }
    }
    
    /**
     * Freezes the task generation until all previous tasks have been executed
     * 
     */
    public void waitForAllTasks(Long appId) {
    	if (tracing) {
            Tracer.emitEvent(Tracer.Event.WAIT_FOR_ALL_TASKS.getId(), Tracer.Event.WAIT_FOR_ALL_TASKS.getType());
        }
    	
    	// Wait until all tasks have finished
    	logger.info("Barrier for app " + appId);
    	ap.waitForAllTasks(appId); 	
    	
    	if (tracing) {
            Tracer.emitEvent(Tracer.Event.WAIT_FOR_ALL_TASKS.getId(), Tracer.Event.WAIT_FOR_ALL_TASKS.getType());
        }
    }
    
    /**
     * Deletes the specified version of a file
     * 
     */
    public boolean deleteFile(String fileName) {
        if (tracing) {
            Tracer.emitEvent(Tracer.Event.DELETE.getId(), Tracer.Event.DELETE.getType());
        }

        // Parse the file name and translate the access mode
        DataLocation loc = null;
        try {
            loc = getDataLocation(fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
        }
        ap.markForDeletion(loc);

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return true;
    }
    
    /**
     * Emit a tracing event (for bindings)
     * 
     */
    public void emitEvent(int type, long id) {
        Tracer.emitEvent(id, type);
    }

    
    /* ****************************************************
     * LoaderAPI INTERFACE IMPLEMENTATION
     * ****************************************************/
    /**
     * Returns a copy of the last file version
     * 
     */
    public String getFile(String fileName, String destDir) {
        if (tracing) {
            Tracer.emitEvent(Tracer.Event.GET_FILE.getId(), Tracer.Event.GET_FILE.getType());
        }

        if (!destDir.endsWith(File.separator)) {
            destDir += File.separator;
        }
        // Parse the file name
        DataLocation sourceLocation = null;
        try {
            sourceLocation = DataLocation.getLocation(Comm.appHost, fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
        }
        FileAccessParams fap = new FileAccessParams(AccessMode.R, sourceLocation);
        DataLocation targetLocation = ap.mainAccessToFile(sourceLocation, fap, destDir);
        String path;
        if (targetLocation == null) {
            URI u = sourceLocation.getURIInHost(Comm.appHost);
            if (u != null) {
                path = u.getPath();
            } else {
                path = fileName;
            }
        } else {
            // Return the name of the file (a renaming) on which the stream will be opened
            path = targetLocation.getPath();
        }

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return path;
    }

    /**
     * Returns a copy of the last object version
     * 
     */
    public Object getObject(Object o, int hashCode, String destDir) {
        /* We know that the object has been accessed before by a task, otherwise
         * the ObjectRegistry would have discarded it and this method
         * would not have been called.
         */
        if (tracing) {
            Tracer.emitEvent(Tracer.Event.GET_OBJECT.getId(), Tracer.Event.GET_OBJECT.getType());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Getting object with hash code " + hashCode);
        }
        boolean validValue = ap.isCurrentRegisterValueValid(hashCode);
        if (validValue) {
            // Main code is still performing the same modification. No need to
            // register it as a new version.
            if (tracing) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
            }

            return null;
        }

        Object oUpdated = ap.mainAcessToObject(o, hashCode, destDir);
        if (logger.isDebugEnabled()) {
            logger.debug("Object obtained " + oUpdated);
        }

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return oUpdated;

    }

    /**
     * Serializes a given object
     */
    public void serializeObject(Object o, int hashCode, String destDir) {
        /*System.out.println("IT: Serializing object");
         String rename = TP.getLastRenaming(hashCode);

         try {
         DataLocation loc = DataLocation.getLocation(Comm.appHost, destDir + rename);
         Serializer.serialize(o, destDir + rename);
         Comm.registerLocation(rename, loc);
         } catch (Exception e) {
         logger.fatal(ERROR_OBJECT_SERIALIZE + ": " + destDir + rename, e);
         System.exit(1);
         }*/
    	//throw new NotImplementedException();
    }

    /**
     * Sets the Object Registry
     */
    public void setObjectRegistry(ObjectRegistry oReg) {
        COMPSsRuntimeImpl.oReg = oReg;
    }

    /**
     * Returns the tmp dir configured by the Runtime
     * 
     */
    public String getTempDir() {
        return Comm.appHost.getTempDirPath();
    }
    
    
    /* ****************************************************
     * COMMON IN BOTH APIs
     * ****************************************************/ 
    /**
     * Returns the renaming of the file version opened
     * 
     */
    public String openFile(String fileName, DataDirection mode) {
        if (tracing) {
            Tracer.emitEvent(Tracer.Event.OPEN_FILE.getId(), Tracer.Event.OPEN_FILE.getType());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Opening file " + fileName + " in mode " + mode);
        }

        DataLocation loc = null;
        try {
            loc = getDataLocation(fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
        }
        AccessMode am = null;
        switch (mode) {
            case IN:
                am = AccessMode.R;
                break;
            case OUT:
                am = AccessMode.W;
                break;
            case INOUT:
                am = AccessMode.RW;
                break;
        }
        // Tell the DM that the application wants to access a file.
        logger.debug("Requesting mainAccess");
        FileAccessParams fap = new FileAccessParams(am, loc);
        DataLocation targetLocation = ap.mainAccessToFile(loc, fap, null);
        logger.debug("MainAccess finished");
        String path;
        if (targetLocation == null) {
            URI u = loc.getURIInHost(Comm.appHost);
            if (u != null) {
                logger.debug("File URI: " + u.toString());
                path = u.getPath();
            } else {
                path = fileName;
            }
        } else {
            /* Return the path that the application must use to access the (renamed) file
             * The file won't recover its origin)al name until stopIT is called
             */
            URI u = targetLocation.getURIInHost(Comm.appHost);
            if (u != null) {
                logger.debug("File URI: " + u.toString());
                path = u.getPath();
            } else {
                path = targetLocation.getPath();
            }
            logger.debug("File target Location: " + path);
        }

        if (tracing) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return path;
    }

    
    /* ****************************************************
     * PRIVATE HELPER METHODS
     * ****************************************************/    
    private Parameter[] processParameters(int parameterCount, Object[] parameters) {
        Parameter[] pars = new Parameter[parameterCount];
        // Parameter parsing needed, object is not serializable
        int i = 0;
        for (int npar = 0; npar < parameterCount; npar++) {
            DataType type = (DataType) parameters[i + 1];
            DataDirection direction = (DataDirection) parameters[i + 2];

            if (logger.isDebugEnabled()) {
                logger.debug("  Parameter " + (npar + 1) + " has type " + type.name());
            }
            switch (type) {
                case FILE_T:
                    DataLocation location = null;
                    try {
                        location = getDataLocation((String) parameters[i]);
                    } catch (Exception e) {
                        ErrorManager.fatal(ERROR_FILE_NAME, e);
                    }
                    pars[npar] = new FileParameter(direction, location);
                    break;

                case SCO_T:
                case PSCO_T:
                    Object internal = oReg.getInternalObject(parameters[i]);
                    if (internal != null) {
                        DataType internalType = LoaderUtils.checkSCOType(internal);
                        if ((type != internalType) && (internalType == DataType.PSCO_T)) {
                            if (logger.isDebugEnabled()) {
                                logger.debug("  Parameter " + (npar + 1) + " change type from " + type.name() + " to " + internalType.name());
                            }
                            parameters[i] = LoaderUtils.checkSCOPersistent(internal);
                            parameters[i + 1] = internalType;
                            type = internalType;
                        }
                    } else {
                        // Python and C++
                        String lang = System.getProperty(ITConstants.IT_LANG);
                        if (ITConstants.Lang.JAVA.toString().compareTo(lang.toUpperCase()) != 0) {
                            if (type == DataType.PSCO_T) {
                                if (!(parameters[i] instanceof StubItf)) {
                                    // There is no Python or C++ PSCO so create directly a new PSCOId(Object, String)
                                    PSCOId pscoId = new PSCOId(parameters[i], (String) parameters[i]);
                                    logger.debug("PSCO with id " + pscoId.getId() + " and hashcode " + pscoId.hashCode() + " detected");
                                    parameters[i] = pscoId;
                                }
                            }
                        }
                    }
                    pars[npar] = new SCOParameter(type,
                            direction,
                            parameters[i],
                            oReg.newObjectParameter(parameters[i])); // hashCode
                    break;

                case OBJECT_T:
                    pars[npar] = new ObjectParameter(direction, parameters[i], oReg.newObjectParameter(parameters[i])); // hashCode
                    break;

                default:
                    /* Basic types (including String).
                     * The only possible direction is IN, warn otherwise
                     */
                    if (direction != DataDirection.IN) {
                        logger.warn(WARN_WRONG_DIRECTION
                                + "Parameter " + npar
                                + " has a basic type, therefore it must have INPUT direction");
                    }
                    pars[npar] = new BasicTypeParameter(type, DataDirection.IN, parameters[i]);
                    break;
            }
            i += 3;
        }

        return pars;
    }

    // Private method for file name parsing. TODO: Logical file names?
    private DataLocation getDataLocation(String fullName) throws Exception {
        DataLocation loc;
        if (fullName.startsWith(FILE_URI)) {
            /* URI syntax with host name and absolute path, e.g. "file://bscgrid01.bsc.es/home/etejedor/file.txt"
             * Only used in grid-aware applications, using IT API and partial loader,
             * since total loader targets sequential applications that use local files.
             */
            /*String name, path, host;
             java.net.URI u = new java.net.URI(fullName);
             host = u.getHost();
             String fullPath = u.getPath();
             int pathEnd = fullPath.lastIndexOf(File.separator);
             path = fullPath.substring(0, pathEnd + 1);
             name = fullPath.substring(pathEnd + 1);*/
            throw new UnsupportedOperationException("Referencing files from remote hosts by URI is not supported yet.");
            // TODO To change body of generated methods, choose Tools | Templates.
        } else if (fullName.startsWith(SHARED_URI)) {
            java.net.URI u = new java.net.URI(fullName);
            logger.debug("Shared URI host: " + u.getHost() + " path:" + u.getPath());
            String sharedDisk = u.getHost();
            String fullPath = u.getPath();
            loc = DataLocation.getSharedLocation(sharedDisk, fullPath);
        } else {
            // Local file, format will depend on OS
            File f = new File(fullName);
            String canonicalPath = f.getCanonicalPath();
            loc = DataLocation.getLocation(Comm.appHost, canonicalPath);
        }

        return loc;
    }

}
