package integratedtoolkit.api.impl;

import java.io.File;
import java.io.IOException;
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
import integratedtoolkit.loader.total.ObjectRegistry;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.annotations.parameter.Direction;
import integratedtoolkit.types.annotations.parameter.Stream;
import integratedtoolkit.types.annotations.Constants;
import integratedtoolkit.types.data.AccessParams.AccessMode;
import integratedtoolkit.types.data.AccessParams.FileAccessParams;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.implementations.MethodImplementation;
import integratedtoolkit.types.parameter.BasicTypeParameter;
import integratedtoolkit.types.parameter.ExternalObjectParameter;
import integratedtoolkit.types.parameter.FileParameter;
import integratedtoolkit.types.parameter.ObjectParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;

import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.RuntimeConfigManager;
import integratedtoolkit.util.Tracer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class COMPSsRuntimeImpl implements COMPSsRuntime, LoaderAPI {

    // Exception constants definition
    private static final String WARN_IT_FILE_NOT_READ = "WARNING: IT Properties file could not be read";
    private static final String WARN_FILE_EMPTY_DEFAULT = "WARNING: IT Properties file is null. Setting default values";
    private static final String WARN_VERSION_PROPERTIES = "WARNING: COMPSs Runtime VERSION-BUILD properties file could not be read";
    private static final String ERROR_FILE_NAME = "ERROR: Cannot parse file name";
    private static final String WARN_WRONG_DIRECTION = "ERROR: Invalid parameter direction: ";

    // COMPSs Version and buildnumber attributes
    private static String COMPSs_VERSION = null;
    private static String COMPSs_BUILDNUMBER = null;

    // Boolean for initialization
    private static boolean initialized = false;

    // Object registry
    private static ObjectRegistry oReg;

    // Components
    private static AccessProcessor ap;
    private static TaskDispatcher<?, ?, ?> td;

    // Monitor
    private static GraphGenerator graphMonitor;
    private static RuntimeMonitor runtimeMonitor;

    // Logger
    private static final Logger logger = LogManager.getLogger(Loggers.API);

    static {
        // Load Runtime configuration parameters
        String propertiesLoc = System.getProperty(ITConstants.IT_CONFIG_LOCATION);
        if (propertiesLoc == null) {
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
                setPropertiesFromRuntime(new RuntimeConfigManager(propertiesLoc));
            } catch (Exception e) {
                System.err.println(WARN_IT_FILE_NOT_READ);
                e.printStackTrace();
            }
        }

        /*
         * Initializes the COMM library and the MasterResource (Master reconfigures the logger)
         */
        Comm.init();
    }


    // Code Added to support configuration files
    private static void setPropertiesFromRuntime(RuntimeConfigManager manager) {
        try {
            if (manager != null) {
                if (manager.getDeploymentId() != null && System.getProperty(ITConstants.IT_DEPLOYMENT_ID) == null) {
                    System.setProperty(ITConstants.IT_DEPLOYMENT_ID, manager.getDeploymentId());
                }
                if (manager.getMasterName() != null && System.getProperty(ITConstants.IT_MASTER_NAME) == null) {
                    System.setProperty(ITConstants.IT_MASTER_NAME, manager.getMasterName());
                }
                if (manager.getMasterPort() != null && System.getProperty(ITConstants.IT_MASTER_PORT) == null) {
                    System.setProperty(ITConstants.IT_MASTER_PORT, manager.getMasterPort());
                }
                if (manager.getAppName() != null && System.getProperty(ITConstants.IT_APP_NAME) == null) {
                    System.setProperty(ITConstants.IT_APP_NAME, manager.getAppName());
                }
                if (manager.getTaskSummary() != null && System.getProperty(ITConstants.IT_TASK_SUMMARY) == null) {
                    System.setProperty(ITConstants.IT_TASK_SUMMARY, manager.getTaskSummary());
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
                if (System.getProperty(ITConstants.IT_COMM_ADAPTOR) == null) {
                    if (manager.getCommAdaptor() != null) {
                        System.setProperty(ITConstants.IT_COMM_ADAPTOR, manager.getCommAdaptor());
                    } else {
                        System.setProperty(ITConstants.IT_COMM_ADAPTOR, ITConstants.DEFAULT_ADAPTOR);
                    }
                }
                if (System.getProperty(ITConstants.IT_CONN) == null) {
                    if (manager.getConn() != null) {
                        System.setProperty(ITConstants.IT_CONN, manager.getConn());
                    } else {
                        System.setProperty(ITConstants.IT_CONN, ITConstants.DEFAULT_CONNECTOR);
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
                if (System.getProperty(ITConstants.IT_EXTRAE_CONFIG_FILE) == null) {
                    System.setProperty(ITConstants.IT_EXTRAE_CONFIG_FILE, manager.getCustomExtraeFile());
                }
                if (System.getProperty(ITConstants.IT_TASK_EXECUTION) == null
                        || System.getProperty(ITConstants.IT_TASK_EXECUTION).equals("")) {
                    System.setProperty(ITConstants.IT_TASK_EXECUTION, ITConstants.EXECUTION_INTERNAL);
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
        if (System.getProperty(ITConstants.IT_DEPLOYMENT_ID) == null || System.getProperty(ITConstants.IT_DEPLOYMENT_ID).isEmpty()) {
            System.setProperty(ITConstants.IT_DEPLOYMENT_ID, ITConstants.DEFAULT_DEPLOYMENT_ID);
        }
        if (System.getProperty(ITConstants.IT_RES_SCHEMA) == null || System.getProperty(ITConstants.IT_RES_SCHEMA).isEmpty()) {
            System.setProperty(ITConstants.IT_RES_SCHEMA, ITConstants.DEFAULT_RES_SCHEMA);
        }
        if (System.getProperty(ITConstants.IT_PROJ_SCHEMA) == null || System.getProperty(ITConstants.IT_PROJ_SCHEMA).isEmpty()) {
            System.setProperty(ITConstants.IT_PROJ_SCHEMA, ITConstants.DEFAULT_PROJECT_SCHEMA);
        }
        if (System.getProperty(ITConstants.GAT_ADAPTOR_PATH) == null || System.getProperty(ITConstants.GAT_ADAPTOR_PATH).isEmpty()) {
            System.setProperty(ITConstants.GAT_ADAPTOR_PATH, ITConstants.DEFAULT_GAT_ADAPTOR_LOCATION);
        }
        if (System.getProperty(ITConstants.IT_COMM_ADAPTOR) == null || System.getProperty(ITConstants.IT_COMM_ADAPTOR).isEmpty()) {
            System.setProperty(ITConstants.IT_COMM_ADAPTOR, ITConstants.DEFAULT_ADAPTOR);
        }
        if (System.getProperty(ITConstants.IT_CONN) == null || System.getProperty(ITConstants.IT_CONN).isEmpty()) {
            System.setProperty(ITConstants.IT_CONN, ITConstants.DEFAULT_CONNECTOR);
        }
        if (System.getProperty(ITConstants.IT_SCHEDULER) == null || System.getProperty(ITConstants.IT_SCHEDULER).isEmpty()) {
            System.setProperty(ITConstants.IT_SCHEDULER, ITConstants.DEFAULT_SCHEDULER);
        }
        if (System.getProperty(ITConstants.IT_TRACING) == null || System.getProperty(ITConstants.IT_TRACING).isEmpty()) {
            System.setProperty(ITConstants.IT_TRACING, ITConstants.DEFAULT_TRACING);
        }
        if (System.getProperty(ITConstants.IT_EXTRAE_CONFIG_FILE) == null
                || System.getProperty(ITConstants.IT_EXTRAE_CONFIG_FILE).isEmpty()) {
            System.setProperty(ITConstants.IT_EXTRAE_CONFIG_FILE, ITConstants.DEFAULT_CUSTOM_EXTRAE_FILE);
        }
        if (System.getProperty(ITConstants.IT_TASK_EXECUTION) == null || System.getProperty(ITConstants.IT_TASK_EXECUTION).isEmpty()) {
            System.setProperty(ITConstants.IT_TASK_EXECUTION, ITConstants.EXECUTION_INTERNAL);
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
                // System.err.println("IT properties file not defined. Looking at classLoader ...");
                stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(ITConstants.IT_CONFIG);
                if (stream != null) {
                    return stream;
                } else {
                    stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(File.separator + ITConstants.IT_CONFIG);
                    if (stream != null) {
                        return stream;
                    } else {
                        // System.err.println("IT properties file not found in classloader. Looking at system resource
                        // ...");
                        stream = ClassLoader.getSystemResourceAsStream(ITConstants.IT_CONFIG);
                        if (stream != null) {
                            return stream;
                        } else {
                            stream = ClassLoader.getSystemResourceAsStream(File.separator + ITConstants.IT_CONFIG);
                            if (stream != null) {
                                return stream;
                            } else {
                                // System.err.println("IT properties file not found. Looking at parent ClassLoader");
                                stream = COMPSsRuntimeImpl.class.getClassLoader().getParent().getResourceAsStream(ITConstants.IT_CONFIG);
                                if (stream != null) {
                                    return stream;
                                } else {
                                    stream = COMPSsRuntimeImpl.class.getClassLoader().getParent()
                                            .getResourceAsStream(File.separator + ITConstants.IT_CONFIG);
                                    if (stream != null) {
                                        return stream;
                                    } else {
                                        // System.err.println("IT properties file not found");
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

    /*
     * ********************************************************************************************************
     * CONSTRUCTOR
     * ********************************************************************************************************
     */
    public COMPSsRuntimeImpl() {
        // Load COMPSs version and buildNumber
        try {
            Properties props = new Properties();
            props.load(this.getClass().getResourceAsStream("/version.properties"));
            COMPSs_VERSION = props.getProperty("compss.version");
            COMPSs_BUILDNUMBER = props.getProperty("compss.build");
        } catch (IOException e) {
            logger.warn(WARN_VERSION_PROPERTIES);
        }

        if (COMPSs_VERSION == null) {
            logger.debug("Deploying COMPSs Runtime");
        } else if (COMPSs_BUILDNUMBER == null) {
            logger.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION);
        } else if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
            COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
            logger.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        } else {
            logger.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        }

        ErrorManager.init(this);
    }

    /*
     * ********************************************************************************************************
     * COMPSsRuntime INTERFACE IMPLEMENTATION
     * ********************************************************************************************************
     */
    @SuppressWarnings("rawtypes")
    /**
     * Starts the COMPSs Runtime
     *
     */
    @Override
    public synchronized void startIT() {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
            Tracer.emitEvent(Tracer.Event.START.getId(), Tracer.Event.START.getType());
        }

        // Console Log
        Thread.currentThread().setName("APPLICATION");
        if (COMPSs_VERSION == null) {
            logger.warn("Starting COMPSs Runtime");
        } else if (COMPSs_BUILDNUMBER == null) {
            logger.warn("Starting COMPSs Runtime v" + COMPSs_VERSION);
        } else if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
            COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
            logger.warn("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        } else {
            logger.warn("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        }

        // Init Runtime
        if (!initialized) {
            // Application
            synchronized (this) {
                logger.debug("Initializing components");

                // Initialize object registry for bindings if needed
                String lang = System.getProperty(ITConstants.IT_LANG);
                if (lang != ITConstants.Lang.JAVA.name() && oReg == null) {
                    oReg = new ObjectRegistry(this);
                }

                // Initialize main runtime components
                td = new TaskDispatcher();
                ap = new AccessProcessor(td);

                // Initialize runtime tools components
                if (GraphGenerator.isEnabled()) {
                    graphMonitor = new GraphGenerator();
                    ap.setGM(graphMonitor);
                }
                if (RuntimeMonitor.isEnabled()) {
                    runtimeMonitor = new RuntimeMonitor(ap, td, graphMonitor, Long.parseLong(System.getProperty(ITConstants.IT_MONITOR)));
                }

                // Log initialization
                initialized = true;
                logger.debug("Ready to process tasks");
            }
        } else {
            // Service
            String className = Thread.currentThread().getStackTrace()[2].getClassName();
            logger.debug("Initializing " + className + "Itf");
            try {
                td.addInterface(Class.forName(className + "Itf"));
            } catch (ClassNotFoundException cnfe) {
                ErrorManager.fatal("Error adding interface " + className + "Itf", cnfe);
            }
        }

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

    }

    /**
     * Stops the COMPSsRuntime
     */
    @Override
    public void stopIT(boolean terminate) {
        synchronized (this) {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.Event.STOP.getId(), Tracer.Event.STOP.getType());
            }

            // Add task summary
            boolean taskSummaryEnabled = System.getProperty(ITConstants.IT_TASK_SUMMARY) != null
                    && !System.getProperty(ITConstants.IT_TASK_SUMMARY).isEmpty()
                    && Boolean.valueOf(System.getProperty(ITConstants.IT_TASK_SUMMARY));
            if (taskSummaryEnabled) {
                td.getTaskSummary(logger);
            }

            // Stop monitor components
            logger.info("Stop IT reached");
            if (GraphGenerator.isEnabled()) {
                logger.debug("Stopping Graph generation...");
                // Graph committed by noMoreTasks, nothing to do
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
        logger.warn("Execution Finished");
    }

    /**
     * Returns the Application Directory
     * 
     */
    @Override
    public String getApplicationDirectory() {
        return Comm.getAppHost().getAppLogDirPath();
    }

    /**
     * Returns the action orchestrator associated to the runtime (only for testing purposes)
     * 
     * @return
     */
    public static ActionOrchestrator<?, ?, ?> getOrchestrator() {
        return td;
    }

    /**
     * Registers a new CoreElement in the COMPSs Runtime
     * 
     */
    @Override
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

        td.registerCEI(signature, methodName, methodClass, mrd);
    }

    /**
     * Execute task: methods
     */
    @Override
    public int executeTask(Long appId, String methodClass, String methodName, boolean priority, boolean hasTarget, int parameterCount,
            Object... parameters) {

        return executeTask(appId, methodClass, methodName, priority, Constants.SINGLE_NODE, !Constants.REPLICATED_TASK,
                !Constants.DISTRIBUTED_TASK, hasTarget, parameterCount, parameters);
    }

    /**
     * Execute task: methods
     */
    @Override
    public int executeTask(Long appId, String methodClass, String methodName, boolean isPrioritary, int numNodes, boolean isReplicated,
            boolean isDistributed, boolean hasTarget, int parameterCount, Object... parameters) {

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.TASK.getId(), Tracer.Event.TASK.getType());
        }

        logger.info("Creating task from method " + methodName + " in " + methodClass);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter" + (parameterCount > 1 ? "s" : ""));
        }

        Parameter[] pars = processParameters(parameterCount, parameters);
        int task = ap.newTask(appId, methodClass, methodName, isPrioritary, numNodes, isReplicated, isDistributed, hasTarget, pars);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return task;
    }

    /**
     * Execute task: services
     */
    @Override
    public int executeTask(Long appId, String namespace, String service, String port, String operation, boolean priority, boolean hasTarget,
            int parameterCount, Object... parameters) {

        return executeTask(appId, namespace, service, port, operation, priority, Constants.SINGLE_NODE, !Constants.REPLICATED_TASK,
                !Constants.DISTRIBUTED_TASK, hasTarget, parameterCount, parameters);
    }

    /**
     * Execute task: services
     */
    @Override
    public int executeTask(Long appId, String namespace, String service, String port, String operation, boolean isPrioritary, int numNodes,
            boolean isReplicated, boolean isDistributed, boolean hasTarget, int parameterCount, Object... parameters) {

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.TASK.getId(), Tracer.Event.TASK.getType());
        }

        if (numNodes != Constants.SINGLE_NODE || isReplicated || isDistributed) {
            ErrorManager.fatal("ERROR: Unsupported feature for Services: multi-node, replicated or distributed");
        }

        logger.info("Creating task from service " + service + ", namespace " + namespace + ", port " + port + ", operation " + operation);
        if (logger.isDebugEnabled()) {
            logger.debug(
                    "There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter" + (parameterCount > 1 ? "s" : ""));
        }

        Parameter[] pars = processParameters(parameterCount, parameters);
        int task = ap.newTask(appId, namespace, service, port, operation, isPrioritary, hasTarget, pars);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return task;
    }

    /**
     * Notifies the Runtime that there are no more tasks created by the current appId
     */
    @Override
    public void noMoreTasks(Long appId, boolean terminate) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.NO_MORE_TASKS.getId(), Tracer.Event.NO_MORE_TASKS.getType());
        }

        logger.info("No more tasks for app " + appId);
        // Wait until all tasks have finished
        ap.noMoreTasks(appId);
        // Retrieve result files
        logger.debug("Getting Result Files " + appId);
        ap.getResultFiles(appId);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }
    }

    /**
     * Freezes the task generation until all previous tasks have been executed
     */
    @Override
    public void waitForAllTasks(Long appId) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.WAIT_FOR_ALL_TASKS.getId(), Tracer.Event.WAIT_FOR_ALL_TASKS.getType());
        }

        // Wait until all tasks have finished
        logger.info("Barrier for app " + appId);
        ap.waitForAllTasks(appId);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.WAIT_FOR_ALL_TASKS.getId(), Tracer.Event.WAIT_FOR_ALL_TASKS.getType());
        }
    }

    /**
     * Deletes the specified version of a file
     */
    @Override
    public boolean deleteFile(String fileName) {
        if (logger.isDebugEnabled()) {
            logger.debug("Access to delete file " + fileName);
        }
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.DELETE.getId(), Tracer.Event.DELETE.getType());
        }

        logger.info("Deleting File " + fileName);

        // Parse the file name and translate the access mode
        DataLocation loc = null;
        try {
            loc = createLocation(fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
        }
        ap.markForDeletion(loc);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return true;
    }

    /**
     * Emit a tracing event (for bindings)
     */
    @Override
    public void emitEvent(int type, long id) {
        Tracer.emitEvent(id, type);
    }

    /*
     * ********************************************************************************************************
     * LoaderAPI INTERFACE IMPLEMENTATION
     * ********************************************************************************************************
     */
    
    /**
     * Returns a copy of the last file version
     */
    @Override
    public String getFile(String fileName, String destDir) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.GET_FILE.getId(), Tracer.Event.GET_FILE.getType());
        }

        if (!destDir.endsWith(File.separator)) {
            destDir += File.separator;
        }
        // Parse the file name
        DataLocation sourceLocation;
        try {
            sourceLocation = createLocation(fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            return null;
        }
        if (sourceLocation == null) {
            ErrorManager.fatal(ERROR_FILE_NAME);
            return null;
        }

        FileAccessParams fap = new FileAccessParams(AccessMode.R, sourceLocation);
        DataLocation targetLocation = ap.mainAccessToFile(sourceLocation, fap, destDir);
        String path;
        if (targetLocation == null) {
            MultiURI u = sourceLocation.getURIInHost(Comm.getAppHost());
            if (u != null) {
                path = u.getPath();
            } else {
                path = fileName;
            }
        } else {
            // Return the name of the file (a renaming) on which the stream will be opened
            path = targetLocation.getPath();
        }

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return path;
    }

    /**
     * Returns a copy of the last object version
     */
    @Override
    public Object getObject(Object o, int hashCode, String destDir) {
        /*
         * We know that the object has been accessed before by a task, otherwise the ObjectRegistry would have discarded
         * it and this method would not have been called.
         */
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.GET_OBJECT.getId(), Tracer.Event.GET_OBJECT.getType());
        }

        if (logger.isDebugEnabled()) {
            logger.debug("Getting object with hash code " + hashCode);
        }
        boolean validValue = ap.isCurrentRegisterValueValid(hashCode);
        if (validValue) {
            // Main code is still performing the same modification. No need to register it as a new version.
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
            }

            return null;
        }

        // Otherwise we request it from a task
        Object oUpdated = ap.mainAcessToObject(o, hashCode, destDir);

        if (logger.isDebugEnabled()) {
            logger.debug("Object obtained " + ((oUpdated == null) ? oUpdated : oUpdated.hashCode()));
        }

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return oUpdated;

    }

    /**
     * Serializes a given object
     */
    @Override
    public void serializeObject(Object o, int hashCode, String destDir) {
        /*
         * System.out.println("IT: Serializing object"); String rename = TP.getLastRenaming(hashCode);
         * 
         * try { DataLocation loc = DataLocation.getLocation(Comm.appHost, destDir + rename); Serializer.serialize(o,
         * destDir + rename); Comm.registerLocation(rename, loc); } catch (Exception e) {
         * logger.fatal(ERROR_OBJECT_SERIALIZE + ": " + destDir + rename, e); System.exit(1); }
         */
        // throw new NotImplementedException();
    }

    /**
     * Sets the Object Registry
     */
    @Override
    public void setObjectRegistry(ObjectRegistry oReg) {
        COMPSsRuntimeImpl.oReg = oReg;
    }

    /**
     * Returns the tmp dir configured by the Runtime
     */
    @Override
    public String getTempDir() {
        return Comm.getAppHost().getTempDirPath();
    }

    /*
     * ********************************************************************************************************
     * COMMON IN BOTH APIs
     * ********************************************************************************************************
     */

    /**
     * Returns the renaming of the file version opened
     */
    @Override
    public String openFile(String fileName, Direction mode) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.OPEN_FILE.getId(), Tracer.Event.OPEN_FILE.getType());
        }

        logger.info("Opening file " + fileName + " in mode " + mode);

        DataLocation loc;
        try {
            loc = createLocation(fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
            return null;
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
        FileAccessParams fap = new FileAccessParams(am, loc);
        DataLocation targetLocation = ap.mainAccessToFile(loc, fap, null);

        String path = (targetLocation == null) ? fileName : targetLocation.getPath();
        DataLocation finalLocation = (targetLocation == null) ? loc : targetLocation;
        if (finalLocation == null) {
            ErrorManager.fatal(ERROR_FILE_NAME);
            return null;
        }

        String finalPath;
        MultiURI u = finalLocation.getURIInHost(Comm.getAppHost());
        if (u != null) {
            finalPath = u.getPath();
        } else {
            finalPath = path;
        }

        if (logger.isDebugEnabled()) {
            logger.debug("File target Location: " + finalPath);
        }

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return finalPath;
    }

    /*
     * ********************************************************************************************************
     * PRIVATE HELPER METHODS
     * ********************************************************************************************************
     */
    
    private Parameter[] processParameters(int parameterCount, Object[] parameters) {
        Parameter[] pars = new Parameter[parameterCount];
        // Parameter parsing needed, object is not serializable
        int i = 0;
        for (int npar = 0; npar < parameterCount; ++npar) {
            DataType type = (DataType) parameters[i + 1];
            Direction direction = (Direction) parameters[i + 2];
            Stream stream = (Stream) parameters[i + 3];
            String prefix = (String) parameters[i + 4];

            if (logger.isDebugEnabled()) {
                logger.debug("  Parameter " + (npar + 1) + " has type " + type.name());
            }

            switch (type) {
                case FILE_T:
                    DataLocation location = null;
                    try {
                        location = createLocation((String) parameters[i]);
                    } catch (Exception e) {
                        logger.error(ERROR_FILE_NAME, e);
                        ErrorManager.fatal(ERROR_FILE_NAME, e);
                    }
                    pars[npar] = new FileParameter(direction, stream, prefix, location);
                    break;

                case PSCO_T:
                case OBJECT_T:
                    pars[npar] = new ObjectParameter(direction, stream, prefix, parameters[i], oReg.newObjectParameter(parameters[i]) // hashCode
                    );
                    break;

                case EXTERNAL_PSCO_T:
                    pars[npar] = new ExternalObjectParameter(direction, stream, prefix, parameters[i],
                            oReg.newObjectParameter(parameters[i]) // hashCode
                    );
                    break;

                default:
                    /*
                     * Basic types (including String). The only possible direction is IN, warn otherwise
                     */
                    if (direction != Direction.IN) {
                        logger.warn(WARN_WRONG_DIRECTION + "Parameter " + npar + " is a basic type, therefore it must have IN direction");
                    }
                    pars[npar] = new BasicTypeParameter(type, Direction.IN, stream, prefix, parameters[i]);
                    break;
            }
            i += 5;
        }

        return pars;
    }

    private DataLocation createLocation(String fileName) throws IOException {
        // Check if fileName contains schema
        SimpleURI uri = new SimpleURI(fileName);
        if (uri.getSchema().isEmpty()) {
            // Add default File scheme and wrap local paths
            String canonicalPath = new File(fileName).getCanonicalPath();
            uri = new SimpleURI(Protocol.FILE_URI.getSchema() + canonicalPath);
        }

        // Check host
        Resource host = Comm.getAppHost();
        String hostName = uri.getHost();
        if (hostName != null && !hostName.isEmpty()) {
            host = Resource.getResource(hostName);
            if (host == null) {
                ErrorManager.error("Host " + hostName + " not found when creating data location.");
            }
        }

        // Create location
        return DataLocation.createLocation(host, uri);
    }

}
