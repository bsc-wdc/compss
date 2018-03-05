package es.bsc.compss.api.impl;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.types.data.location.DataLocation;

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.components.monitor.impl.RuntimeMonitor;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.loader.total.ObjectRegistry;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.Stream;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.data.AccessParams.AccessMode;
import es.bsc.compss.types.data.AccessParams.FileAccessParams;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.implementations.AbstractMethodImplementation.MethodType;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.ExternalObjectParameter;
import es.bsc.compss.types.parameter.FileParameter;
import es.bsc.compss.types.parameter.ObjectParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;

import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.RuntimeConfigManager;
import es.bsc.compss.util.Tracer;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class COMPSsRuntimeImpl implements COMPSsRuntime, LoaderAPI {

    // Exception constants definition
    private static final String WARN_IT_FILE_NOT_READ = "WARNING: COMPSs Properties file could not be read";
    private static final String WARN_FILE_EMPTY_DEFAULT = "WARNING: COMPSs Properties file is null. Setting default values";
    private static final String WARN_VERSION_PROPERTIES = "WARNING: COMPSs Runtime VERSION-BUILD properties file could not be read";
    private static final String ERROR_FILE_NAME = "ERROR: Cannot parse file name";
    private static final String WARN_WRONG_DIRECTION = "WARNING: Invalid parameter direction: ";

    // COMPSs Version and buildnumber attributes
    private static String COMPSs_VERSION = null;
    private static String COMPSs_BUILDNUMBER = null;

    // Boolean for initialization
    private static boolean initialized = false;

    // Object registry
    private static ObjectRegistry oReg;

    // Components
    private static AccessProcessor ap;
    private static TaskDispatcher td;

    // Monitor
    private static GraphGenerator graphMonitor;
    private static RuntimeMonitor runtimeMonitor;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);

    static {
        // Load Runtime configuration parameters
        String propertiesLoc = System.getProperty(COMPSsConstants.COMPSS_CONFIG_LOCATION);
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
                if (manager.getDeploymentId() != null && System.getProperty(COMPSsConstants.DEPLOYMENT_ID) == null) {
                    System.setProperty(COMPSsConstants.DEPLOYMENT_ID, manager.getDeploymentId());
                }
                if (manager.getMasterName() != null && System.getProperty(COMPSsConstants.MASTER_NAME) == null) {
                    System.setProperty(COMPSsConstants.MASTER_NAME, manager.getMasterName());
                }
                if (manager.getMasterPort() != null && System.getProperty(COMPSsConstants.MASTER_PORT) == null) {
                    System.setProperty(COMPSsConstants.MASTER_PORT, manager.getMasterPort());
                }
                if (manager.getAppName() != null && System.getProperty(COMPSsConstants.APP_NAME) == null) {
                    System.setProperty(COMPSsConstants.APP_NAME, manager.getAppName());
                }
                if (manager.getTaskSummary() != null && System.getProperty(COMPSsConstants.TASK_SUMMARY) == null) {
                    System.setProperty(COMPSsConstants.TASK_SUMMARY, manager.getTaskSummary());
                }
                if (manager.getCOMPSsBaseLogDir() != null && System.getProperty(COMPSsConstants.BASE_LOG_DIR) == null) {
                    System.setProperty(COMPSsConstants.BASE_LOG_DIR, manager.getCOMPSsBaseLogDir());
                }
                if (manager.getSpecificLogDir() != null && System.getProperty(COMPSsConstants.SPECIFIC_LOG_DIR) == null) {
                    System.setProperty(COMPSsConstants.SPECIFIC_LOG_DIR, manager.getSpecificLogDir());
                }
                if (manager.getLog4jConfiguration() != null && System.getProperty(COMPSsConstants.LOG4J) == null) {
                    System.setProperty(COMPSsConstants.LOG4J, manager.getLog4jConfiguration());
                }
                if (manager.getResourcesFile() != null && System.getProperty(COMPSsConstants.RES_FILE) == null) {
                    System.setProperty(COMPSsConstants.RES_FILE, manager.getResourcesFile());
                }
                if (manager.getResourcesSchema() != null && System.getProperty(COMPSsConstants.RES_SCHEMA) == null) {
                    System.setProperty(COMPSsConstants.RES_SCHEMA, manager.getResourcesSchema());
                }
                if (manager.getProjectFile() != null && System.getProperty(COMPSsConstants.PROJ_FILE) == null) {
                    System.setProperty(COMPSsConstants.PROJ_FILE, manager.getProjectFile());
                }
                if (manager.getProjectSchema() != null && System.getProperty(COMPSsConstants.PROJ_SCHEMA) == null) {
                    System.setProperty(COMPSsConstants.PROJ_SCHEMA, manager.getProjectSchema());
                }

                if (manager.getScheduler() != null && System.getProperty(COMPSsConstants.SCHEDULER) == null) {
                    System.setProperty(COMPSsConstants.SCHEDULER, manager.getScheduler());
                }
                if (manager.getMonitorInterval() > 0 && System.getProperty(COMPSsConstants.MONITOR) == null) {
                    System.setProperty(COMPSsConstants.MONITOR, Long.toString(manager.getMonitorInterval()));
                }
                if (manager.getGATAdaptor() != null && System.getProperty(COMPSsConstants.GAT_ADAPTOR_PATH) == null) {
                    System.setProperty(COMPSsConstants.GAT_ADAPTOR_PATH, manager.getGATAdaptor());
                }
                if (manager.getGATBrokerAdaptor() != null && System.getProperty(COMPSsConstants.GAT_BROKER_ADAPTOR) == null) {
                    System.setProperty(COMPSsConstants.GAT_BROKER_ADAPTOR, manager.getGATBrokerAdaptor());
                }
                if (manager.getGATFileAdaptor() != null && System.getProperty(COMPSsConstants.GAT_FILE_ADAPTOR) == null) {
                    System.setProperty(COMPSsConstants.GAT_FILE_ADAPTOR, manager.getGATFileAdaptor());
                }

                if (manager.getWorkerCP() != null && System.getProperty(COMPSsConstants.WORKER_CP) == null) {
                    System.setProperty(COMPSsConstants.WORKER_CP, manager.getWorkerCP());
                }
                if (manager.getWorkerJVMOpts() != null && System.getProperty(COMPSsConstants.WORKER_JVM_OPTS) == null) {
                    System.setProperty(COMPSsConstants.WORKER_JVM_OPTS, manager.getWorkerJVMOpts());
                }
                if (System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY) == null
                        || System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY).isEmpty()) {
                    System.setProperty(COMPSsConstants.WORKER_CPU_AFFINITY, Boolean.toString(manager.isWorkerCPUAffinityEnabled()));
                }
                if (System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY) == null
                        || System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY).isEmpty()) {
                    System.setProperty(COMPSsConstants.WORKER_GPU_AFFINITY, Boolean.toString(manager.isWorkerGPUAffinityEnabled()));
                }

                if (manager.getServiceName() != null && System.getProperty(COMPSsConstants.SERVICE_NAME) == null) {
                    System.setProperty(COMPSsConstants.SERVICE_NAME, manager.getServiceName());
                }
                if (System.getProperty(COMPSsConstants.COMM_ADAPTOR) == null) {
                    if (manager.getCommAdaptor() != null) {
                        System.setProperty(COMPSsConstants.COMM_ADAPTOR, manager.getCommAdaptor());
                    } else {
                        System.setProperty(COMPSsConstants.COMM_ADAPTOR, COMPSsConstants.DEFAULT_ADAPTOR);
                    }
                }
                if (System.getProperty(COMPSsConstants.CONN) == null) {
                    if (manager.getConn() != null) {
                        System.setProperty(COMPSsConstants.CONN, manager.getConn());
                    } else {
                        System.setProperty(COMPSsConstants.CONN, COMPSsConstants.DEFAULT_CONNECTOR);
                    }
                }
                if (System.getProperty(COMPSsConstants.GAT_DEBUG) == null) {
                    System.setProperty(COMPSsConstants.GAT_DEBUG, Boolean.toString(manager.isGATDebug()));
                }
                if (System.getProperty(COMPSsConstants.LANG) == null) {
                    System.setProperty(COMPSsConstants.LANG, manager.getLang());
                }
                if (System.getProperty(COMPSsConstants.GRAPH) == null) {
                    System.setProperty(COMPSsConstants.GRAPH, Boolean.toString(manager.isGraph()));
                }
                if (System.getProperty(COMPSsConstants.TRACING) == null) {
                    System.setProperty(COMPSsConstants.TRACING, String.valueOf(manager.getTracing()));
                }
                if (System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) == null) {
                    System.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, manager.getCustomExtraeFile());
                }
                if (System.getProperty(COMPSsConstants.TASK_EXECUTION) == null
                        || System.getProperty(COMPSsConstants.TASK_EXECUTION).equals("")) {
                    System.setProperty(COMPSsConstants.TASK_EXECUTION, COMPSsConstants.EXECUTION_INTERNAL);
                }

                if (manager.getContext() != null) {
                    System.setProperty(COMPSsConstants.COMPSS_CONTEXT, manager.getContext());
                }
                System.setProperty(COMPSsConstants.COMPSS_TO_FILE, Boolean.toString(manager.isToFile()));
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
        if (System.getProperty(COMPSsConstants.DEPLOYMENT_ID) == null || System.getProperty(COMPSsConstants.DEPLOYMENT_ID).isEmpty()) {
            System.setProperty(COMPSsConstants.DEPLOYMENT_ID, COMPSsConstants.DEFAULT_DEPLOYMENT_ID);
        }
        if (System.getProperty(COMPSsConstants.RES_SCHEMA) == null || System.getProperty(COMPSsConstants.RES_SCHEMA).isEmpty()) {
            System.setProperty(COMPSsConstants.RES_SCHEMA, COMPSsConstants.DEFAULT_RES_SCHEMA);
        }
        if (System.getProperty(COMPSsConstants.PROJ_SCHEMA) == null || System.getProperty(COMPSsConstants.PROJ_SCHEMA).isEmpty()) {
            System.setProperty(COMPSsConstants.PROJ_SCHEMA, COMPSsConstants.DEFAULT_PROJECT_SCHEMA);
        }
        if (System.getProperty(COMPSsConstants.GAT_ADAPTOR_PATH) == null
                || System.getProperty(COMPSsConstants.GAT_ADAPTOR_PATH).isEmpty()) {
            System.setProperty(COMPSsConstants.GAT_ADAPTOR_PATH, COMPSsConstants.DEFAULT_GAT_ADAPTOR_LOCATION);
        }
        if (System.getProperty(COMPSsConstants.COMM_ADAPTOR) == null || System.getProperty(COMPSsConstants.COMM_ADAPTOR).isEmpty()) {
            System.setProperty(COMPSsConstants.COMM_ADAPTOR, COMPSsConstants.DEFAULT_ADAPTOR);
        }
        if (System.getProperty(COMPSsConstants.CONN) == null || System.getProperty(COMPSsConstants.CONN).isEmpty()) {
            System.setProperty(COMPSsConstants.CONN, COMPSsConstants.DEFAULT_CONNECTOR);
        }
        if (System.getProperty(COMPSsConstants.SCHEDULER) == null || System.getProperty(COMPSsConstants.SCHEDULER).isEmpty()) {
            System.setProperty(COMPSsConstants.SCHEDULER, COMPSsConstants.DEFAULT_SCHEDULER);
        }
        if (System.getProperty(COMPSsConstants.TRACING) == null || System.getProperty(COMPSsConstants.TRACING).isEmpty()) {
            System.setProperty(COMPSsConstants.TRACING, COMPSsConstants.DEFAULT_TRACING);
        }
        if (System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) == null
                || System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE).isEmpty()) {
            System.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, COMPSsConstants.DEFAULT_CUSTOM_EXTRAE_FILE);
        }
        if (System.getProperty(COMPSsConstants.TASK_EXECUTION) == null || System.getProperty(COMPSsConstants.TASK_EXECUTION).isEmpty()) {
            System.setProperty(COMPSsConstants.TASK_EXECUTION, COMPSsConstants.EXECUTION_INTERNAL);
        }
    }

    private static InputStream findPropertiesConfigFile() {
        InputStream stream = COMPSsRuntimeImpl.class.getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
        if (stream != null) {
            return stream;
        } else {
            stream = COMPSsRuntimeImpl.class.getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
            if (stream != null) {
                return stream;
            } else {
                // System.err.println("IT properties file not defined. Looking at classLoader ...");
                stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                if (stream != null) {
                    return stream;
                } else {
                    stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
                    if (stream != null) {
                        return stream;
                    } else {
                        // System.err.println("IT properties file not found in classloader. Looking at system resource
                        // ...");
                        stream = ClassLoader.getSystemResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                        if (stream != null) {
                            return stream;
                        } else {
                            stream = ClassLoader.getSystemResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
                            if (stream != null) {
                                return stream;
                            } else {
                                // System.err.println("IT properties file not found. Looking at parent ClassLoader");
                                stream = COMPSsRuntimeImpl.class.getClassLoader().getParent()
                                        .getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                                if (stream != null) {
                                    return stream;
                                } else {
                                    stream = COMPSsRuntimeImpl.class.getClassLoader().getParent()
                                            .getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
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
            LOGGER.warn(WARN_VERSION_PROPERTIES);
        }

        if (COMPSs_VERSION == null) {
            LOGGER.debug("Deploying COMPSs Runtime");
        } else if (COMPSs_BUILDNUMBER == null) {
            LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION);
        } else if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
            COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
            LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        } else {
            LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        }

        ErrorManager.init(this);
    }

    /*
     * ********************************************************************************************************
     * COMPSsRuntime INTERFACE IMPLEMENTATION
     * ********************************************************************************************************
     */
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
            LOGGER.warn("Starting COMPSs Runtime");
        } else if (COMPSs_BUILDNUMBER == null) {
            LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION);
        } else if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
            COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
            LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        } else {
            LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
        }

        // Init Runtime
        if (!initialized) {
            // Application
            synchronized (this) {
                LOGGER.debug("Initializing components");

                // Initialize object registry for bindings if needed
                // String lang = System.getProperty(COMPSsConstants.LANG);
                // if (lang != COMPSsConstants.Lang.JAVA.name() && oReg == null) {
                // oReg = new ObjectRegistry(this);
                // }

                // Initialize main runtime components
                td = new TaskDispatcher();
                ap = new AccessProcessor(td);

                // Initialize runtime tools components
                if (GraphGenerator.isEnabled()) {
                    graphMonitor = new GraphGenerator();
                    ap.setGM(graphMonitor);
                }
                if (RuntimeMonitor.isEnabled()) {
                    runtimeMonitor = new RuntimeMonitor(ap, td, graphMonitor, Long.parseLong(System.getProperty(COMPSsConstants.MONITOR)));
                }

                // Log initialization
                initialized = true;
                LOGGER.debug("Ready to process tasks");
            }
        } else {
            // Service
            String className = Thread.currentThread().getStackTrace()[2].getClassName();
            LOGGER.debug("Initializing " + className + "Itf");
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
            boolean taskSummaryEnabled = System.getProperty(COMPSsConstants.TASK_SUMMARY) != null
                    && !System.getProperty(COMPSsConstants.TASK_SUMMARY).isEmpty()
                    && Boolean.parseBoolean(System.getProperty(COMPSsConstants.TASK_SUMMARY));
            if (taskSummaryEnabled) {
                td.getTaskSummary(LOGGER);
            }

            // Stop monitor components
            LOGGER.info("Stop IT reached");
            if (GraphGenerator.isEnabled()) {
                LOGGER.debug("Stopping Graph generation...");
                // Graph committed by noMoreTasks, nothing to do
            }
            if (RuntimeMonitor.isEnabled()) {
                LOGGER.debug("Stopping Monitor...");
                runtimeMonitor.shutdown();
            }

            // Stop runtime components
            LOGGER.debug("Stopping AP...");
            if (ap != null) {
                ap.shutdown();
            } else {
                LOGGER.debug("AP was not initialized...");
            }

            LOGGER.debug("Stopping TD...");
            if (td != null) {
                td.shutdown();
            } else {
                LOGGER.debug("TD was not initialized...");
            }

            LOGGER.debug("Stopping Comm...");
            Comm.stop();
            LOGGER.debug("Runtime stopped");

        }
        LOGGER.warn("Execution Finished");
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
    public static ActionOrchestrator getOrchestrator() {
        return td;
    }

    /**
     * Registers a new CoreElement in the COMPSs Runtime
     *
     */
    @Override
    public void registerCoreElement(String coreElementSignature, String implSignature, String implConstraints, String implType,
            String... implTypeArgs) {

        LOGGER.info("Registering CoreElement " + coreElementSignature);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("\t - Implementation: " + implSignature);
            LOGGER.debug("\t - Constraints   : " + implConstraints);
            LOGGER.debug("\t - Type          : " + implType);
            LOGGER.debug("\t - ImplTypeArgs  : ");
            for (String implTypeArg : implTypeArgs) {
                LOGGER.debug("\t\t Arg: " + implTypeArg);
            }
        }

        MethodResourceDescription mrd = new MethodResourceDescription(implConstraints);
        MethodType mt;
        switch (implType) {
            case "METHOD":
                mt = MethodType.METHOD;
                break;
            case "MPI":
                mt = MethodType.MPI;
                break;
            case "DECAF":
                mt = MethodType.DECAF;
                break;
            case "BINARY":
                mt = MethodType.BINARY;
                break;
            case "OMPSS":
                mt = MethodType.OMPSS;
                break;
            case "OPENCL":
                mt = MethodType.OPENCL;
                break;
            default:
                ErrorManager.error("Unrecognised method type " + implType);
                return;
        }

        td.registerNewCoreElement(coreElementSignature, implSignature, mrd, mt, implTypeArgs);
    }

    /**
     * Execute task: methods
     */
    @Override
    public int executeTask(Long appId, String methodClass, String methodName, boolean isPrioritary, boolean hasTarget, int parameterCount,
            Object... parameters) {

        return executeTask(appId, false, methodClass, methodName, null, isPrioritary, Constants.SINGLE_NODE,
                Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK), Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK), hasTarget,
                parameterCount, parameters);
    }

    /**
     * Execute task: methods with method class and method name
     *
     */
    @Override
    public int executeTask(Long appId, String methodClass, String methodName, boolean isPrioritary, int numNodes, boolean isReplicated,
            boolean isDistributed, boolean hasTarget, int parameterCount, Object... parameters) {

        return executeTask(appId, false, methodClass, methodName, null, isPrioritary, numNodes, isReplicated, isDistributed, hasTarget,
                parameterCount, parameters);
    }

    /**
     * Execute task: methods with signature
     */
    @Override
    public int executeTask(Long appId, String signature, boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed,
            boolean hasTarget, int parameterCount, Object... parameters) {

        return executeTask(appId, true, null, null, signature, isPrioritary, numNodes, isReplicated, isDistributed, hasTarget,
                parameterCount, parameters);
    }

    /**
     * Internal execute task to make API options only as a wrapper
     * 
     * @param appId
     * @param hasSignature
     *            indicates whether the signature parameter is valid or must be constructed from the methodName and
     *            methodClass parameters
     * @param methodClass
     * @param methodName
     * @param signature
     * @param isPrioritary
     * @param numNodes
     * @param isReplicated
     * @param isDistributed
     * @param hasTarget
     * @param parameterCount
     * @param parameters
     * @return
     */
    private int executeTask(Long appId, boolean hasSignature, String methodClass, String methodName, String signature, boolean isPrioritary,
            int numNodes, boolean isReplicated, boolean isDistributed, boolean hasTarget, int parameterCount, Object... parameters) {

        // Tracing flag for task creation
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.TASK.getId(), Tracer.Event.TASK.getType());
        }

        // Log the details
        if (hasSignature) {
            LOGGER.info("Creating task from method " + signature);
        } else {
            LOGGER.info("Creating task from method " + methodName + " in " + methodClass);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter" + (parameterCount > 1 ? "s" : ""));
        }

        // Process the parameters
        Parameter[] pars = processParameters(parameterCount, parameters);
        boolean hasReturn = hasReturn(pars);

        // Create the signature if it is not created
        if (!hasSignature) {
            signature = MethodImplementation.getSignature(methodClass, methodName, hasTarget, hasReturn, pars);
        }

        // Register the task
        int task = ap.newTask(appId, signature, isPrioritary, numNodes, isReplicated, isDistributed, hasTarget, hasReturn, pars);

        // End tracing event
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        // Return the taskId
        return task;
    }

    /**
     * Returns whether the method parameters define a return or not
     * 
     * @param parameters
     * @return
     */
    private boolean hasReturn(Parameter[] parameters) {
        boolean hasReturn = false;
        if (parameters.length != 0) {
            Parameter lastParam = parameters[parameters.length - 1];
            DataType type = lastParam.getType();
            hasReturn = (lastParam.getDirection() == Direction.OUT
                    && (type == DataType.OBJECT_T || type == DataType.PSCO_T || type == DataType.EXTERNAL_OBJECT_T));
        }

        return hasReturn;
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

        LOGGER.info("Creating task from service " + service + ", namespace " + namespace + ", port " + port + ", operation " + operation);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug(
                    "There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter" + (parameterCount > 1 ? "s" : ""));
        }

        // Process the parameters
        Parameter[] pars = processParameters(parameterCount, parameters);
        boolean hasReturn = hasReturn(pars);

        // Register the task
        int task = ap.newTask(appId, namespace, service, port, operation, isPrioritary, hasTarget, hasReturn, pars);

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

        LOGGER.info("No more tasks for app " + appId);
        // Wait until all tasks have finished
        ap.noMoreTasks(appId);
        // Retrieve result files
        LOGGER.debug("Getting Result Files " + appId);
        ap.getResultFiles(appId);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }
    }

    /**
     * Freezes the task generation until all previous tasks have been executed
     */
    @Override
    public void barrier(Long appId) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.WAIT_FOR_ALL_TASKS.getId(), Tracer.Event.WAIT_FOR_ALL_TASKS.getType());
        }

        // Wait until all tasks have finished
        LOGGER.info("Barrier for app " + appId);
        ap.barrier(appId);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.WAIT_FOR_ALL_TASKS.getId(), Tracer.Event.WAIT_FOR_ALL_TASKS.getType());
        }
    }

    /**
     * Deletes the specified version of a file
     */
    @Override
    public boolean deleteFile(String fileName) {
        // Check parameters
        if (fileName == null || fileName.isEmpty()) {
            return false;
        }

        LOGGER.info("Deleting File " + fileName);

        // Emit event
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.DELETE.getId(), Tracer.Event.DELETE.getType());
        }

        // Parse the file name and translate the access mode
        try {
            DataLocation loc = createLocation(fileName);
            ap.markForDeletion(loc);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
            }
        }

        // Return deletion was successful
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

        // Parse the destination path
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

        // Ask the AP to
        String finalPath = mainAccessToFile(fileName, sourceLocation, AccessMode.R, destDir);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return finalPath;
    }

    /**
     * Returns a copy of the last object version
     */
    @Override
    public Object getObject(Object obj, int hashCode, String destDir) {
        /*
         * We know that the object has been accessed before by a task, otherwise the ObjectRegistry would have discarded
         * it and this method would not have been called.
         */
        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.Event.GET_OBJECT.getId(), Tracer.Event.GET_OBJECT.getType());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting object with hash code " + hashCode);
        }

        Object oUpdated = mainAccessToObject(obj, hashCode);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Object obtained " + ((oUpdated == null) ? oUpdated : oUpdated.hashCode()));
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
     * ***********************************************************************************************************
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

        LOGGER.info("Opening " + fileName + " in mode " + mode);

        // Parse arguments to internal structures
        DataLocation loc;
        try {
            loc = createLocation(fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
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

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        String finalPath;
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                finalPath = mainAccessToFile(fileName, loc, am, null);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("File target Location: " + finalPath);
                }
                break;
            case PERSISTENT:
                finalPath = mainAccessToExternalObject(fileName, loc);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("External Object target Location: " + finalPath);
                }
                break;
            default:
                finalPath = null;
                ErrorManager.error("ERROR: Unrecognised protocol requesting openFile " + fileName);
        }

        if (Tracer.isActivated()) {
            Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        }

        return finalPath;
    }

    @Override
    public void closeFile(String fileName, Direction mode) {
        // if (Tracer.isActivated()) {
        // Tracer.emitEvent(Tracer.Event.CLOSE_FILE.getId(), Tracer.Event.CLOSE_FILE.getType());
        // }

        LOGGER.info("Closing " + fileName + " in mode " + mode);

        // Parse arguments to internal structures
        DataLocation loc;
        try {
            loc = createLocation(fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
            return;
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

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                finishAccessToFile(fileName, loc, am, null);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Closing file " + loc.getPath());
                }
                break;
            case PERSISTENT:
                // Nothing to do
                ErrorManager.warn("WARN: Cannot close file " + fileName + " with PSCO protocol");
                break;
            default:
                ErrorManager.error("ERROR: Unrecognised protocol requesting closeFile " + fileName);
        }

        // if (Tracer.isActivated()) {
        // Tracer.emitEvent(Tracer.EVENT_END, Tracer.getRuntimeEventsType());
        // }
    }

    /*
     * ************************************************************************************************************
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

            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("  Parameter " + (npar + 1) + " has type " + type.name());
            }

            switch (type) {
                case FILE_T:
                    try {
                        String fileName = (String) parameters[i];
                        String originalName = new File(fileName).getName();
                        DataLocation location = createLocation((String) parameters[i]);
                        pars[npar] = new FileParameter(direction, stream, prefix, location, originalName);
                    } catch (Exception e) {
                        LOGGER.error(ERROR_FILE_NAME, e);
                        ErrorManager.fatal(ERROR_FILE_NAME, e);
                    }

                    break;

                case PSCO_T:
                case OBJECT_T:
                    pars[npar] = new ObjectParameter(direction, stream, prefix, parameters[i], oReg.newObjectParameter(parameters[i]));
                    break;

                case EXTERNAL_OBJECT_T:
                    String id = (String) parameters[i];
                    pars[npar] = new ExternalObjectParameter(direction, stream, prefix, id, externalObjectHashcode(id));
                    break;

                default:
                    /*
                     * Basic types (including String). The only possible direction is IN, warn otherwise
                     */
                    if (direction != Direction.IN) {
                        LOGGER.warn(WARN_WRONG_DIRECTION + "Parameter " + npar + " is a basic type, therefore it must have IN direction");
                    }
                    pars[npar] = new BasicTypeParameter(type, Direction.IN, stream, prefix, parameters[i]);
                    break;
            }
            i += 5;
        }

        return pars;
    }

    private int externalObjectHashcode(String id) {
        int hashCode = 7;
        for (int i = 0; i < id.length(); ++i) {
            hashCode = hashCode * 31 + id.charAt(i);
        }

        return hashCode;
    }

    private void finishAccessToFile(String fileName, DataLocation loc, AccessMode am, String destDir) {
        FileAccessParams fap = new FileAccessParams(am, loc);
        ap.finishAccessToFile(loc, fap, destDir);
    }

    private String mainAccessToFile(String fileName, DataLocation loc, AccessMode am, String destDir) {
        // Tell the AP that the application wants to access a file.
        FileAccessParams fap = new FileAccessParams(am, loc);
        DataLocation targetLocation = ap.mainAccessToFile(loc, fap, destDir);

        // Checks on target
        String path = (targetLocation == null) ? fileName : targetLocation.getPath();
        DataLocation finalLocation = (targetLocation == null) ? loc : targetLocation;
        if (finalLocation == null) {
            ErrorManager.fatal(ERROR_FILE_NAME);
            return null;
        }

        // Return the final target path
        String finalPath;
        MultiURI u = finalLocation.getURIInHost(Comm.getAppHost());
        if (u != null) {
            finalPath = u.getPath();
        } else {
            finalPath = path;
        }

        return finalPath;
    }

    private Object mainAccessToObject(Object obj, int hashCode) {
        boolean validValue = ap.isCurrentRegisterValueValid(hashCode);
        if (validValue) {
            // Main code is still performing the same modification.
            // No need to register it as a new version.
            return null;
        }

        // Otherwise we request it from a task
        return ap.mainAcessToObject(obj, hashCode);
    }

    private String mainAccessToExternalObject(String fileName, DataLocation loc) {
        String id = ((PersistentLocation) loc).getId();
        int hashCode = externalObjectHashcode(id);
        boolean validValue = ap.isCurrentRegisterValueValid(hashCode);
        if (validValue) {
            // Main code is still performing the same modification.
            // No need to register it as a new version.
            return fileName;
        }

        // Otherwise we request it from a task
        return ap.mainAcessToExternalObject(id, hashCode);
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
