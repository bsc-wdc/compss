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
package es.bsc.compss.api.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.components.monitor.impl.GraphGenerator;
import es.bsc.compss.components.monitor.impl.RuntimeMonitor;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.DoNothingTaskMonitor;
import es.bsc.compss.types.ErrorHandler;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;
import es.bsc.compss.types.data.accessparams.FileAccessParams;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.implementations.MethodImplementation;
import es.bsc.compss.types.implementations.definition.ImplementationDefinition;
import es.bsc.compss.types.listeners.CancelTaskGroupOnResourceCreation;
import es.bsc.compss.types.parameter.BasicTypeParameter;
import es.bsc.compss.types.parameter.BindingObjectParameter;
import es.bsc.compss.types.parameter.CollectionParameter;
import es.bsc.compss.types.parameter.DictCollectionParameter;
import es.bsc.compss.types.parameter.DirectoryParameter;
import es.bsc.compss.types.parameter.ExternalPSCOParameter;
import es.bsc.compss.types.parameter.ExternalStreamParameter;
import es.bsc.compss.types.parameter.FileParameter;
import es.bsc.compss.types.parameter.ObjectParameter;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.parameter.StreamParameter;
import es.bsc.compss.types.resources.MasterResourceImpl;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourcesPool;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.RuntimeConfigManager;
import es.bsc.compss.util.TraceEvent;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileSystemException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class COMPSsRuntimeImpl implements COMPSsRuntime, LoaderAPI, ErrorHandler {

    // Exception constants definition
    private static final String WARN_IT_FILE_NOT_READ = "WARNING: COMPSs Properties file could not be read";
    private static final String WARN_FILE_EMPTY_DEFAULT =
        "WARNING: COMPSs Properties file is null." + " Setting default values";
    private static final String WARN_VERSION_PROPERTIES =
        "WARNING: COMPSs Runtime VERSION-BUILD" + " properties file could not be read";
    private static final String ERROR_FILE_NAME = "ERROR: Cannot parse file name";
    private static final String ERROR_DIR_NAME = "ERROR: Not a valid directory";
    private static final String ERROR_BINDING_OBJECT_PARAMS =
        "ERROR: Incorrect number of parameters" + " for external objects";
    private static final String WARN_WRONG_DIRECTION = "WARNING: Invalid parameter direction: ";

    // COMPSS Version and buildnumber attributes
    private static String COMPSs_VERSION = null;
    private static String COMPSs_BUILDNUMBER = null;

    // Boolean for initialization
    private static boolean initialized = false;

    // Number of fields per parameter
    public static final int NUM_FIELDS_PER_PARAM = 9;

    // Language
    protected static final String DEFAULT_LANG_STR = System.getProperty(COMPSsConstants.LANG);
    protected static final Lang DEFAULT_LANG =
        ((DEFAULT_LANG_STR == null) ? Lang.JAVA : Lang.valueOf(DEFAULT_LANG_STR.toUpperCase()));

    // Registries
    private static ObjectRegistry oReg;
    private static StreamRegistry sReg;

    // Components
    private static AccessProcessor ap;
    private static TaskDispatcher td;

    // Monitor
    private static GraphGenerator graphMonitor;
    private static RuntimeMonitor runtimeMonitor;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);

    // External Task monitor
    private static final TaskMonitor DO_NOTHING_MONITOR = new DoNothingTaskMonitor();

    static {
        // Load Runtime configuration parameters
        String propertiesLoc = System.getProperty(COMPSsConstants.COMPSS_CONFIG_LOCATION);
        if (propertiesLoc == null) {
            InputStream stream = findPropertiesConfigFile();
            if (stream != null) {
                try {
                    setPropertiesFromRuntime(new RuntimeConfigManager(stream));
                } catch (Exception e) {
                    System.err.println(WARN_IT_FILE_NOT_READ); // NOSONAR
                    e.printStackTrace();// NOSONAR
                }
            } else {
                setDefaultProperties();
            }
        } else {
            try {
                setPropertiesFromRuntime(new RuntimeConfigManager(propertiesLoc));
            } catch (Exception e) {
                System.err.println(WARN_IT_FILE_NOT_READ); // NOSONAR
                e.printStackTrace(); // NOSONAR
            }
        }

        /*
         * Initializes the COMM library and the MasterResource (Master reconfigures the logger)
         */
        Comm.init(new MasterResourceImpl());
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
                if (manager.getSpecificLogDir() != null
                    && System.getProperty(COMPSsConstants.SPECIFIC_LOG_DIR) == null) {
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
                if (manager.getGATBrokerAdaptor() != null
                    && System.getProperty(COMPSsConstants.GAT_BROKER_ADAPTOR) == null) {
                    System.setProperty(COMPSsConstants.GAT_BROKER_ADAPTOR, manager.getGATBrokerAdaptor());
                }
                if (manager.getGATFileAdaptor() != null
                    && System.getProperty(COMPSsConstants.GAT_FILE_ADAPTOR) == null) {
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
                    System.setProperty(COMPSsConstants.WORKER_CPU_AFFINITY,
                        Boolean.toString(manager.isWorkerCPUAffinityEnabled()));
                }
                if (System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY) == null
                    || System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY).isEmpty()) {
                    System.setProperty(COMPSsConstants.WORKER_GPU_AFFINITY,
                        Boolean.toString(manager.isWorkerGPUAffinityEnabled()));
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
                    System.setProperty(COMPSsConstants.TASK_EXECUTION, COMPSsConstants.TaskExecution.COMPSS.toString());
                }

                if (manager.getContext() != null) {
                    System.setProperty(COMPSsConstants.COMPSS_CONTEXT, manager.getContext());
                }
                System.setProperty(COMPSsConstants.COMPSS_TO_FILE, Boolean.toString(manager.isToFile()));

            } else {
                setDefaultProperties();
            }
        } catch (Exception e) {
            System.err.println(WARN_IT_FILE_NOT_READ); // NOSONAR
            e.printStackTrace();// NOSONAR
        }
    }

    private static void setDefaultProperties() {
        System.err.println(WARN_FILE_EMPTY_DEFAULT);
        if (System.getProperty(COMPSsConstants.DEPLOYMENT_ID) == null
            || System.getProperty(COMPSsConstants.DEPLOYMENT_ID).isEmpty()) {
            System.setProperty(COMPSsConstants.DEPLOYMENT_ID, COMPSsConstants.DEFAULT_DEPLOYMENT_ID);
        }
        if (System.getProperty(COMPSsConstants.RES_SCHEMA) == null
            || System.getProperty(COMPSsConstants.RES_SCHEMA).isEmpty()) {
            System.setProperty(COMPSsConstants.RES_SCHEMA, COMPSsConstants.DEFAULT_RES_SCHEMA);
        }
        if (System.getProperty(COMPSsConstants.PROJ_SCHEMA) == null
            || System.getProperty(COMPSsConstants.PROJ_SCHEMA).isEmpty()) {
            System.setProperty(COMPSsConstants.PROJ_SCHEMA, COMPSsConstants.DEFAULT_PROJECT_SCHEMA);
        }
        if (System.getProperty(COMPSsConstants.GAT_ADAPTOR_PATH) == null
            || System.getProperty(COMPSsConstants.GAT_ADAPTOR_PATH).isEmpty()) {
            System.setProperty(COMPSsConstants.GAT_ADAPTOR_PATH, COMPSsConstants.DEFAULT_GAT_ADAPTOR_LOCATION);
        }
        if (System.getProperty(COMPSsConstants.COMM_ADAPTOR) == null
            || System.getProperty(COMPSsConstants.COMM_ADAPTOR).isEmpty()) {
            System.setProperty(COMPSsConstants.COMM_ADAPTOR, COMPSsConstants.DEFAULT_ADAPTOR);
        }
        if (System.getProperty(COMPSsConstants.CONN) == null || System.getProperty(COMPSsConstants.CONN).isEmpty()) {
            System.setProperty(COMPSsConstants.CONN, COMPSsConstants.DEFAULT_CONNECTOR);
        }
        if (System.getProperty(COMPSsConstants.SCHEDULER) == null
            || System.getProperty(COMPSsConstants.SCHEDULER).isEmpty()) {
            System.setProperty(COMPSsConstants.SCHEDULER, COMPSsConstants.DEFAULT_SCHEDULER);
        }
        if (System.getProperty(COMPSsConstants.TRACING) == null
            || System.getProperty(COMPSsConstants.TRACING).isEmpty()) {
            System.setProperty(COMPSsConstants.TRACING, COMPSsConstants.DEFAULT_TRACING);
        }
        if (System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) == null
            || System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE).isEmpty()) {
            System.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, COMPSsConstants.DEFAULT_CUSTOM_EXTRAE_FILE);
        }
        if (System.getProperty(COMPSsConstants.TASK_EXECUTION) == null
            || System.getProperty(COMPSsConstants.TASK_EXECUTION).isEmpty()) {
            System.setProperty(COMPSsConstants.TASK_EXECUTION, COMPSsConstants.TaskExecution.COMPSS.toString());
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
                // System.err.println("IT properties file not defined. Looking at classLoader...");
                stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                if (stream != null) {
                    return stream;
                } else {
                    stream = COMPSsRuntimeImpl.class.getClassLoader()
                        .getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
                    if (stream != null) {
                        return stream;
                    } else {
                        // System.err.println("IT properties file not found in classloader. Looking at system
                        // resource...");
                        stream = ClassLoader.getSystemResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                        if (stream != null) {
                            return stream;
                        } else {
                            stream =
                                ClassLoader.getSystemResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
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
     * ************************************************************************************************************
     * CONSTRUCTOR
     * ************************************************************************************************************
     */
    /**
     * Creates a new COMPSs Runtime instance.
     */
    public COMPSsRuntimeImpl() {
        // Load COMPSS version and buildNumber
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
        } else {
            if (COMPSs_BUILDNUMBER == null) {
                LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION);
            } else {
                if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
                    COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
                    LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
                } else {
                    LOGGER.debug("Deploying COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
                }
            }
        }

        ErrorManager.init(this);
        ((MasterResourceImpl) Comm.getAppHost()).setupNestedSupport(this, this);
    }

    /*
     * ************************************************************************************************************
     * COMPSsRuntime INTERFACE
     * ************************************************************************************************************
     */
    @Override
    public synchronized void startIT() {
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.STATIC_IT.getType());
            Tracer.emitEvent(TraceEvent.START.getId(), TraceEvent.START.getType());
        }

        // Console Log
        Thread.currentThread().setName("APPLICATION");
        if (COMPSs_VERSION == null) {
            LOGGER.warn("Starting COMPSs Runtime");
        } else {
            if (COMPSs_BUILDNUMBER == null) {
                LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION);
            } else {
                if (COMPSs_BUILDNUMBER.endsWith("rnull")) {
                    COMPSs_BUILDNUMBER = COMPSs_BUILDNUMBER.substring(0, COMPSs_BUILDNUMBER.length() - 6);
                    LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
                } else {
                    LOGGER.warn("Starting COMPSs Runtime v" + COMPSs_VERSION + " (build " + COMPSs_BUILDNUMBER + ")");
                }
            }
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
                    runtimeMonitor = new RuntimeMonitor(ap, td, graphMonitor,
                        Long.parseLong(System.getProperty(COMPSsConstants.MONITOR)));
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

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.START.getType());
        }

    }

    @Override
    public void stopIT(boolean terminate) {
        synchronized (this) {
            if (Tracer.extraeEnabled()) {
                Tracer.emitEvent(TraceEvent.STOP.getId(), TraceEvent.STOP.getType());
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
            Comm.stop(CoreManager.getSignaturesToCoreIds());
            LOGGER.debug("Runtime stopped");

        }
        LOGGER.warn("Execution Finished");
    }

    @Override
    public String getApplicationDirectory() {
        return Comm.getAppHost().getAppLogDirPath();
    }

    /**
     * Returns the action orchestrator associated to the Runtime (only for testing purposes).
     *
     * @return The action orchestrator associated to the Runtime.
     */
    public static ActionOrchestrator getOrchestrator() {
        return td;
    }

    @Override
    public long registerApplication() {
        Application app = Application.registerApplication();
        return app.getId();
    }

    @Override
    public long registerApplication(String parallelismSource) {
        Application app = Application.registerApplication(parallelismSource);
        return app.getId();
    }

    @Override
    public void registerApplication(Long appId) {
        Application.registerApplication(appId);
    }

    @Override
    public void registerApplication(Long appId, String parallelismSource) {
        Application.registerApplication(appId, parallelismSource);
    }

    @Override
    public void deregisterApplication(Long appId) {
        Application.deregisterApplication(appId);
    }

    @Override
    public void registerCoreElement(String coreElementSignature, String implSignature, String implConstraints,
        String implType, String implIO, String... implTypeArgs) {

        LOGGER.info("Registering CoreElement " + coreElementSignature);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("\t - Implementation: " + implSignature);
            LOGGER.debug("\t - Constraints   : " + implConstraints);
            LOGGER.debug("\t - Type          : " + implType);
            LOGGER.debug("\t - I/O            : " + implIO);
            LOGGER.debug("\t - ImplTypeArgs  : ");
            for (String implTypeArg : implTypeArgs) {
                LOGGER.debug("\t\t Arg: " + implTypeArg);
            }
        }

        MethodResourceDescription mrd = new MethodResourceDescription(implConstraints);
        boolean implisIO = Boolean.parseBoolean(implIO);

        if (implisIO) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Nulling computing resources for I/O task: " + implSignature);
            }
            mrd.setIOResources();
        }

        CoreElementDefinition ced = new CoreElementDefinition();
        ced.setCeSignature(coreElementSignature);
        ImplementationDefinition<?> implDef =
            ImplementationDefinition.defineImplementation(implType, implSignature, mrd, implTypeArgs);
        ced.addImplementation(implDef);

        td.registerNewCoreElement(ced);
    }

    @Override
    public void registerCoreElement(CoreElementDefinition ced) {
        LOGGER.info("Registering CoreElement " + ced.getCeSignature());
        if (LOGGER.isDebugEnabled()) {
            int implId = 0;
            for (ImplementationDefinition<?> implDef : ced.getImplementations()) {
                LOGGER.debug("\t - Implementation " + implId + ":");
                try {
                    LOGGER.debug(implDef.toString());
                } catch (Exception e) {
                    LOGGER.debug("Error printing implDef", e);
                }
            }
        }

        td.registerNewCoreElement(ced);
    }

    @Override
    public void registerData(Long appId, DataType type, Object stub, String data) {
        Application app = Application.registerApplication(appId);
        switch (type) {
            case DIRECTORY_T:
            case FILE_T:
                try {
                    String fileName = (String) stub;
                    // Parse arguments to internal structures
                    DataLocation loc;
                    try {
                        loc = createLocation(fileName);
                    } catch (IOException ioe) {
                        ErrorManager.fatal(ERROR_FILE_NAME, ioe);
                        return;
                    }
                    ap.registerRemoteFile(app, loc, data);
                } catch (NullPointerException npe) {
                    LOGGER.error(ERROR_FILE_NAME, npe);
                    ErrorManager.fatal(ERROR_FILE_NAME, npe);
                }
                break;
            case OBJECT_T:
            case PSCO_T:
                int hashcode = oReg.newObjectParameter(appId, stub);
                ap.registerRemoteObject(app, hashcode, data);
                break;
            case STREAM_T:
                // int streamCode = oReg.newObjectParameter(stub);
                break;
            case EXTERNAL_STREAM_T:
                try {
                    String fileName = (String) stub;
                    new File(fileName).getName();
                } catch (NullPointerException npe) {
                    LOGGER.error(ERROR_FILE_NAME, npe);
                    ErrorManager.fatal(ERROR_FILE_NAME, npe);
                }
                break;
            case EXTERNAL_PSCO_T:
                // String id = (String) stub;
                break;
            case BINDING_OBJECT_T:
                String value = (String) stub;
                if (value.contains(":")) {
                    String[] fields = value.split(":");
                    if (fields.length == 3) {
                        String extObjectId = fields[0];
                        int extObjectType = Integer.parseInt(fields[1]);
                        int extObjectElements = Integer.parseInt(fields[2]);
                        // BindingObject bo = new BindingObject(extObjectId, extObjectType, extObjectElements);
                        new BindingObject(extObjectId, extObjectType, extObjectElements);
                        // int externalCode = externalObjectHashcode(extObjectId);
                        externalObjectHashcode(extObjectId);
                    } else {
                        LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    }
                } else {
                    LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                }
                break;
            case COLLECTION_T:
                throw new UnsupportedOperationException("Not implemented yet.");
            case DICT_COLLECTION_T:
                throw new UnsupportedOperationException("Not implemented yet.");
            default:
                // Basic types (including String)
                // Already passed in as a value
                break;
        }
    }

    // C
    @Override
    public int executeTask(Long appId, String methodClass, String onFailure, int timeOut, String methodName,
        boolean isPrioritary, boolean hasTarget, Integer numReturns, int parameterCount, Object... parameters) {

        boolean isReplicated = Boolean.parseBoolean(Constants.IS_NOT_REPLICATED_TASK);
        boolean isDistributed = Boolean.parseBoolean(Constants.IS_NOT_DISTRIBUTED_TASK);
        return executeTask(appId, null, Lang.C, false, methodClass, methodName, null, OnFailure.valueOf(onFailure),
            timeOut, isPrioritary, Constants.SINGLE_NODE, isReplicated, isDistributed, hasTarget, numReturns,
            parameterCount, parameters);
    }

    // Python
    @Override
    public int executeTask(Long appId, String signature, String onFailure, int timeOut, boolean isPrioritary,
        int numNodes, boolean isReplicated, boolean isDistributed, boolean hasTarget, Integer numReturns,
        int parameterCount, Object... parameters) {

        return executeTask(appId, null, Lang.PYTHON, true, null, null, signature, OnFailure.valueOf(onFailure), timeOut,
            isPrioritary, numNodes, isReplicated, isDistributed, hasTarget, numReturns, parameterCount, parameters);
    }

    // Java - Loader
    @Override
    public int executeTask(Long appId, TaskMonitor monitor, Lang lang, String methodClass, String methodName,
        boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed, boolean hasTarget,
        int parameterCount, OnFailure onFailure, int timeOut, Object... parameters) {

        return executeTask(appId, monitor, lang, false, methodClass, methodName, null, onFailure, timeOut, isPrioritary,
            numNodes, isReplicated, isDistributed, hasTarget, null, parameterCount, parameters);
    }

    // Services
    @Override
    public int executeTask(Long appId, TaskMonitor monitor, String namespace, String service, String port,
        String operation, boolean isPrioritary, int numNodes, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, int parameterCount, OnFailure onFailure, int timeOut, Object... parameters) {

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.TASK.getId(), TraceEvent.TASK.getType());
        }

        if (numNodes != Constants.SINGLE_NODE || isReplicated || isDistributed) {
            ErrorManager.fatal("ERROR: Unsupported feature for Services: multi-node, replicated or distributed");
        }

        LOGGER.info("Creating task from service " + service + ", namespace " + namespace + ", port " + port
            + ", operation " + operation + " for application " + appId);
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter"
                + (parameterCount > 1 ? "s" : ""));
        }

        Application app = Application.registerApplication(appId);
        // Process the parameters
        List<Parameter> pars = processParameters(app, parameterCount, parameters);
        boolean hasReturn = hasReturn(pars);
        int numReturns = hasReturn ? 1 : 0;

        if (monitor == null) {
            monitor = DO_NOTHING_MONITOR;
        }

        // Register the task
        int task = ap.newTask(app, monitor, namespace, service, port, operation, isPrioritary, hasTarget, numReturns,
            pars, onFailure, timeOut);

        for (Parameter p : pars) {
            if (p.getDirection().equals(Direction.IN_DELETE)) {
                processDelete(app, p);
            }
        }
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.TASK.getType());
        }

        return task;
    }

    /**
     * Internal execute task to make API options only as a wrapper.
     *
     * @param appId Application Id.
     * @param monitor Task monitor.
     * @param lang Task language
     * @param hasSignature indicates whether the signature parameter is valid or must be constructed from the methodName
     *            and methodClass parameters.
     * @param methodClass Method class.
     * @param methodName Method name.
     * @param signature Method signature.
     * @param onFailure On failure behavior.
     * @param timeOut Amount of time for an application time out.
     * @param isPrioritary Whether the task has priority or not.
     * @param numNodes Number of associated nodes.
     * @param isReplicated Whether it is a replicated task or not.
     * @param isDistributed Whether the task must be round-robin distributed or not.
     * @param hasTarget Whether the task has a return value or not.
     * @param numReturns Number of return values of the task.
     * @param parameterCount Number of parameters of the task.
     * @param parameters Parameter values.
     * @return The task id.
     */
    public int executeTask(Long appId, TaskMonitor monitor, Lang lang, boolean hasSignature, String methodClass,
        String methodName, String signature, OnFailure onFailure, int timeOut, boolean isPrioritary, int numNodes,
        boolean isReplicated, boolean isDistributed, boolean hasTarget, Integer numReturns, int parameterCount,
        Object... parameters) {

        // Tracing flag for task creation
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.TASK.getId(), TraceEvent.TASK.getType());
        }

        // Log the details
        if (hasSignature) {
            LOGGER.info("Creating task from method " + signature + " for application " + appId);
        } else {
            LOGGER.info("Creating task from method " + methodName + " in " + methodClass + " for application " + appId);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("There " + (parameterCount == 1 ? "is " : "are ") + parameterCount + " parameter"
                + (parameterCount > 1 ? "s" : ""));
        }

        Application app = Application.registerApplication(appId);
        // Process the parameters
        List<Parameter> pars = processParameters(app, parameterCount, parameters);

        if (numReturns == null) {
            numReturns = hasReturn(pars) ? 1 : 0;
        }

        // Create the signature if it is not created
        if (!hasSignature) {
            signature = MethodImplementation.getSignature(methodClass, methodName, hasTarget, numReturns, pars);
        }

        if (monitor == null) {
            monitor = DO_NOTHING_MONITOR;
        }

        if (lang == null) {
            lang = DEFAULT_LANG;
        }

        // Register the task
        int task = ap.newTask(app, monitor, lang, signature, isPrioritary, numNodes, isReplicated, isDistributed,
            hasTarget, numReturns, pars, onFailure, timeOut);

        for (Parameter p : pars) {
            if (p.getDirection().equals(Direction.IN_DELETE)) {
                processDelete(app, p);
            }
        }

        // End tracing event
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.TASK.getType());
        }

        // Return the taskId
        return task;
    }

    @Override
    public void noMoreTasks(Long appId) {
        Application app = Application.registerApplication(appId);
        noMoreTasks(app);
    }

    /**
     * Notifies the runtime that an application will not produce more tasks.
     *
     * @param app Application that finished generating tasks
     */
    public void noMoreTasks(Application app) {
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.NO_MORE_TASKS.getId(), TraceEvent.NO_MORE_TASKS.getType());
        }

        LOGGER.info("No more tasks for app " + app.getId());
        // Wait until all tasks have finished
        ap.noMoreTasks(app);
        // Retrieve result files
        LOGGER.debug("Getting Result Files for app" + app.getId());
        ap.getResultFiles(app);

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.NO_MORE_TASKS.getType());
        }
    }

    @Override
    public void barrier(Long appId) {
        barrier(appId, false);
    }

    @Override
    public void barrier(Long appId, boolean noMoreTasks) {
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.WAIT_FOR_ALL_TASKS.getId(), TraceEvent.WAIT_FOR_ALL_TASKS.getType());
        }

        Application app = Application.registerApplication(appId);
        // Wait until all tasks have finished
        LOGGER.info("Barrier for app " + appId + " with noMoreTasks = " + noMoreTasks);
        if (noMoreTasks) {
            // No more tasks expected, we can unregister application
            noMoreTasks(app);
        } else {
            // Regular barrier
            ap.barrier(app);
        }

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.WAIT_FOR_ALL_TASKS.getType());
        }
    }

    @Override
    public void barrierGroup(Long appId, String groupName) throws COMPSsException {
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.WAIT_FOR_ALL_TASKS.getId(), TraceEvent.WAIT_FOR_ALL_TASKS.getType());
        }

        Application app = Application.registerApplication(appId);
        // Regular barrier
        ap.barrierGroup(app, groupName);

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.WAIT_FOR_ALL_TASKS.getType());
        }
    }

    @Override
    public boolean deleteFile(Long appId, String fileName, boolean waitForData) {
        // Check parameters
        if (fileName == null || fileName.isEmpty()) {
            return false;
        }

        LOGGER.info("Deleting File " + fileName + " with wait for data " + waitForData);

        // Emit event
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.DELETE.getId(), TraceEvent.DELETE.getType());
        }

        // Parse the file name and translate the access mode
        try {
            DataLocation loc = createLocation(fileName);
            Application app = Application.registerApplication(appId);
            ap.markForDeletion(app, loc, waitForData);
            // Java case where task files are stored in the registry
            if (sReg != null) {
                sReg.deleteTaskFile(appId, fileName);
            }
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
        } finally {
            if (Tracer.extraeEnabled()) {
                Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.DELETE.getType());
            }
        }
        LOGGER.info("File " + fileName + " Deleted.");
        // Return deletion was successful
        return true;
    }

    @Override
    public boolean deleteFile(Long appId, String fileName) {
        return deleteFile(appId, fileName, true);
    }

    @Override
    public void emitEvent(int type, long id) {
        Tracer.emitEvent(id, type);
    }

    @Override
    public void openTaskGroup(String groupName, boolean implicitBarrier, Long appId) {
        Application app = Application.registerApplication(appId);
        ap.setCurrentTaskGroup(groupName, implicitBarrier, app);
    }

    @Override
    public void closeTaskGroup(String groupName, Long appId) {
        Application app = Application.registerApplication(appId);
        ap.closeCurrentTaskGroup(app);
    }

    @Override
    public String getBindingObject(Long appId, String fileName) {
        // Parse the file name
        LOGGER.debug(" Calling get binding object : " + fileName);

        Application app = Application.registerApplication(appId);
        BindingObjectLocation sourceLocation =
            new BindingObjectLocation(Comm.getAppHost(), BindingObject.generate(fileName));
        // Ask the AP to
        String finalPath = mainAccessToBindingObject(app, fileName, sourceLocation);
        LOGGER.debug(" Returning binding object as id: " + finalPath);
        return finalPath;
    }

    @Override
    public boolean deleteBindingObject(Long appId, String fileName) {
        // Check parameters
        if (fileName == null || fileName.isEmpty()) {
            return false;
        }

        LOGGER.info("Deleting BindingObject " + fileName);

        // Emit event
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.DELETE.getId(), TraceEvent.DELETE.getType());
        }

        // Parse the binding object name and translate the access mode
        BindingObject bo = BindingObject.generate(fileName);
        int hashCode = externalObjectHashcode(bo.getId());
        ap.markForBindingObjectDeletion(hashCode);
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.DELETE.getType());
        }

        // Return deletion was successful
        return true;
    }

    @Override
    public void cancelApplicationTasks(Long appId) {
        Application app = Application.registerApplication(appId);
        ap.cancelApplicationTasks(app);
    }

    @Override
    public void deregisterObject(Long appId, Object o) {
        oReg.delete(appId, o);
    }

    @Override
    public void removeObject(Object o, int hashcode) {
        // This will remove the object from the Object Registry and the Data Info Provider
        // eventually allowing the garbage collector to free it (better use of memory)
        ap.deregisterObject(o);
    }

    @Override
    public int getNumberOfResources() {
        LOGGER.info("Received request for number of active resources");
        return ResourceManager.getTotalNumberOfWorkers();
    }

    @Override
    public void requestResources(Long appId, int numResources, String groupName) {
        LOGGER.info("Received request to create " + numResources + " resources and notify " + groupName
            + " for application " + appId);

        if (numResources > 0) {
            Application app = Application.registerApplication(appId);
            // Create listener to cancel the associated task group
            CancelTaskGroupOnResourceCreation rcl =
                new CancelTaskGroupOnResourceCreation(ap, app, numResources, groupName);

            // Request first resource
            // The rest will be automatically requested by the CancelTaskGroupOnResource listener
            ResourceManager.requestResources(1, rcl);
        }
    }

    @Override
    public void freeResources(Long appId, int numResources, String groupName) {
        LOGGER.info("Received request to destroy " + numResources + " resources and notify " + groupName
            + " for application " + appId);

        Application app = Application.registerApplication(appId);
        // Cancel associated task group (if provided)
        if (groupName != null && !groupName.isEmpty()) {
            ap.cancelTaskGroup(app, groupName);
        }

        // Destroy resources
        // No need to sync since task will be re-scheduled as soon as the workers are available
        ResourceManager.freeResources(numResources);
    }

    /*
     * *********************************************************************************************************
     * LoaderAPI INTERFACE IMPLEMENTATION
     * *********************************************************************************************************
     */
    @Override
    public void getFile(Long appId, String fileName) {
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.GET_FILE.getId(), TraceEvent.GET_FILE.getType());
        }
        String destDir = Comm.getAppHost().getTempDirPath();
        // Parse the destination path
        if (!destDir.endsWith(File.separator)) {
            destDir += File.separator;
        }

        // Parse the file name
        DataLocation sourceLocation = null;
        try {
            sourceLocation = createLocation(fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
        }
        if (sourceLocation == null) {
            ErrorManager.fatal(ERROR_FILE_NAME);
        }

        LOGGER.debug("Getting file " + fileName);
        Application app = Application.registerApplication(appId);
        String renamedPath = openFile(app, fileName, Direction.INOUT);
        // If renamePth is the same as original, file has not accessed. Nothing to do.
        if (!renamedPath.equals(sourceLocation.getPath())) {
            String intermediateTmpPath = renamedPath + ".tmp";
            rename(renamedPath, intermediateTmpPath);
            closeFile(app, fileName, Direction.INOUT);
            ap.markForDeletion(app, sourceLocation, true);
            // In the case of Java file can be stored in the Stream Registry
            if (sReg != null) {
                sReg.deleteTaskFile(appId, fileName);
            }
            rename(intermediateTmpPath, fileName);
        }
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.GET_FILE.getType());
        }
    }

    @Override
    public void getDirectory(Long appId, String dirName) {
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.GET_DIRECTORY.getId(), TraceEvent.GET_DIRECTORY.getType());
        }

        // Parse the dir name
        DataLocation sourceLocation = null;
        try {
            // Check if dirName contains schema
            SimpleURI uri = new SimpleURI(dirName);
            if (uri.getSchema().isEmpty()) {
                // Add default Dir scheme and wrap local paths
                String canonicalPath = new File(dirName).getCanonicalPath();
                dirName = ProtocolType.DIR_URI.getSchema() + canonicalPath;
            }
            sourceLocation = createLocation(dirName);

        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_DIR_NAME, ioe);
        }
        if (sourceLocation == null) {
            ErrorManager.fatal(ERROR_DIR_NAME);
        }

        LOGGER.debug("Getting directory " + dirName);
        Application app = Application.registerApplication(appId);
        String renamedPath = openDirectory(app, dirName, Direction.IN);

        LOGGER.debug("Getting directory renamed path: " + renamedPath);
        String intermediateTmpPath = renamedPath + ".tmp";
        rename(renamedPath, intermediateTmpPath);
        closeFile(app, dirName, Direction.IN);

        ap.markForDeletion(app, sourceLocation, true);
        // In the case of Java file can be stored in the Stream Registry
        if (sReg != null) {
            sReg.deleteTaskFile(appId, dirName);
        }

        moveDirectory(intermediateTmpPath, dirName);

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.GET_DIRECTORY.getType());
        }
    }

    /**
     * Moves the file to its target location.
     *
     * @param source Source file path.
     * @param target Target file path.
     */
    private void rename(String source, String target) {
        Path sourcePath = Paths.get(source);
        Path destinationPath = Paths.get(target);
        LOGGER.info("Moving file from " + source + " to " + target);
        try {
            Files.move(sourcePath, destinationPath, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException e) {
            try {
                Files.move(sourcePath, destinationPath);
            } catch (IOException e1) {
                LOGGER.error("Move not possible ", e1);
            }
        } catch (IOException e) {
            LOGGER.error("Atomic move not possible ", e);
        }
    }

    /**
     * Moves the directory to its target location.
     *
     * @param source Source dir path.
     * @param target Target dir path.
     */
    private void moveDirectory(String source, String target) {
        if (target.contains(ProtocolType.DIR_URI.getSchema())) {
            target = target.substring(target.indexOf(File.separator));
        }
        LOGGER.info("Moving dir from " + source + " to " + target);
        Path sourcePath = Paths.get(source);
        Path destinationPath = Paths.get(target);

        // todo: recursively?
        try {
            Files.move(sourcePath, destinationPath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        } catch (AtomicMoveNotSupportedException e) {
            try {
                Files.move(sourcePath, destinationPath);
            } catch (IOException e1) {
                LOGGER.error("Move not possible ", e1);
            }
        } catch (FileSystemException fse) {
            // Target Folder already Exists
            deleteDestinationAndMoveFolder(sourcePath, destinationPath);
        } catch (Exception e) {
            LOGGER.error("Directory move failed", e);
        }
    }

    private void deleteDestinationAndMoveFolder(Path sourcePath, Path destinationPath) {
        deleteFolder(destinationPath.toFile());
        try {
            Files.move(sourcePath, destinationPath, StandardCopyOption.ATOMIC_MOVE,
                StandardCopyOption.REPLACE_EXISTING);
        } catch (Exception e) {
            LOGGER.error("Could not move folder " + sourcePath + " to " + destinationPath, e);
        }
    }

    private void deleteFolder(File folder) {
        if (folder.isDirectory() && folder.listFiles() != null) {
            for (File f : folder.listFiles()) {
                deleteFolder(f);
            }
        }
        if (!folder.delete()) {
            LOGGER.error("Error deleting folder " + folder.getName());
        }
    }

    private void processDelete(Application app, Parameter p) {
        switch (p.getType()) {
            case DIRECTORY_T:
                ap.markForDeletion(app, ((DirectoryParameter) p).getLocation(), false);
                // Java case where task files are stored in the registry
                if (sReg != null) {
                    sReg.deleteTaskFile(app.getId(), ((DirectoryParameter) p).getOriginalName());
                }
                break;
            case FILE_T:
                ap.markForDeletion(app, ((FileParameter) p).getLocation(), false);
                // Java case where task files are stored in the registry
                if (sReg != null) {
                    sReg.deleteTaskFile(app.getId(), ((FileParameter) p).getOriginalName());
                }
                break;
            case BINDING_OBJECT_T:
                ap.markForBindingObjectDeletion(((BindingObjectParameter) p).getCode());
                break;
            case OBJECT_T:
                ObjectParameter op = (ObjectParameter) p;
                oReg.delete(app.getId(), op.getValue());
                break;
            case COLLECTION_T:
                for (Parameter sp : ((CollectionParameter) p).getParameters()) {
                    processDelete(app, sp);
                }
                break;
            case DICT_COLLECTION_T:
                for (Map.Entry<Parameter, Parameter> sp : ((DictCollectionParameter) p).getParameters().entrySet()) {
                    processDelete(app, sp.getKey());
                    processDelete(app, sp.getValue());
                }
                break;
            default:
                break;
        }
    }

    @Override
    public Object getObject(Long appId, Object obj, int hashCode, String destDir) {
        Application app = Application.registerApplication(appId);
        /*
         * We know that the object has been accessed before by a task, otherwise the ObjectRegistry would have discarded
         * it and this method would not have been called.
         */
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.GET_OBJECT.getId(), TraceEvent.GET_OBJECT.getType());
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting object with hash code " + hashCode);
        }

        Object oUpdated = mainAccessToObject(app, obj, hashCode);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Object obtained " + ((oUpdated == null) ? oUpdated : oUpdated.hashCode()));
        }

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.GET_OBJECT.getType());
        }

        return oUpdated;
    }

    @Override
    public void serializeObject(Object o, int hashCode, String destDir) {
        // throw new NotImplementedException();
    }

    @Override
    public ObjectRegistry getObjectRegistry() {
        return oReg;
    }

    @Override
    public StreamRegistry getStreamRegistry() {
        return sReg;
    }

    @Override
    public void setObjectRegistry(ObjectRegistry oReg) {
        COMPSsRuntimeImpl.oReg = oReg;
    }

    @Override
    public void setStreamRegistry(StreamRegistry sReg) {
        COMPSsRuntimeImpl.sReg = sReg;
    }

    @Override
    public String getTempDir() {
        return Comm.getAppHost().getTempDirPath();
    }

    /*
     * ************************************************************************************************************
     * COMMON IN BOTH APIs
     * ************************************************************************************************************
     */
    @Override
    public boolean isFileAccessed(Long appId, String fileName) {
        DataLocation loc;
        try {
            loc = createLocation(fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            loc = null;
        }
        if (loc != null) {
            Application app = Application.registerApplication(appId);
            return ap.alreadyAccessed(app, loc);
        } else {
            return false;
        }
    }

    @Override
    public String openFile(Long appId, String fileName, Direction mode) {
        Application app = Application.registerApplication(appId);
        return openFile(app, fileName, mode);
    }

    /**
     * Main code opens a file version that the runtime may need to fetch.
     *
     * @param app Application opening the file
     * @param fileName location of the file
     * @param mode access mode
     * @return the renaming of the file version opened.
     */
    public String openFile(Application app, String fileName, Direction mode) {
        LOGGER.info("Opening " + fileName + " in mode " + mode);

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.OPEN_FILE.getId(), TraceEvent.OPEN_FILE.getType());
        }
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
            case IN_DELETE:
                am = AccessMode.R;
                break;
            case OUT:
                am = AccessMode.W;
                break;
            case INOUT:
                am = AccessMode.RW;
                break;
            case CONCURRENT:
                am = AccessMode.C;
                break;
            case COMMUTATIVE:
                am = AccessMode.CV;
                break;
        }

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        String finalPath;
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                finalPath = mainAccessToFile(app, fileName, loc, am, null, false);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("File target Location: " + finalPath);
                }
                break;
            case PERSISTENT:
                finalPath = mainAccessToExternalPSCO(app, fileName, loc);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("External PSCO target Location: " + finalPath);
                }
                break;
            default:
                finalPath = null;
                ErrorManager.error("ERROR: Unrecognised protocol requesting openFile " + fileName);
        }

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.OPEN_FILE.getType());
        }

        return finalPath;
    }

    @Override
    public String openDirectory(Long appId, String dirName, Direction mode) {
        Application app = Application.registerApplication(appId);
        return openDirectory(app, dirName, mode);
    }

    /**
     * Main code opens a directory that the runtime may need to fetch.
     *
     * @param app application accessing the data.
     * @param dirName Directory name.
     * @param mode Access mode.
     * @return the renaming of the file version opened.
     */
    public String openDirectory(Application app, String dirName, Direction mode) {
        // todo: common in both api's ??
        LOGGER.info("Opening " + dirName + " in mode " + mode);

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.OPEN_DIRECTORY.getId(), TraceEvent.OPEN_DIRECTORY.getType());
        }
        // Parse arguments to internal structures
        DataLocation loc;
        try {
            loc = createLocation(dirName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_DIR_NAME, ioe);
            return null;
        }

        AccessMode am = null;
        switch (mode) {
            case IN:
            case IN_DELETE:
                am = AccessMode.R;
                break;
            case OUT:
                am = AccessMode.W;
                break;
            case INOUT:
                am = AccessMode.RW;
                break;
            case CONCURRENT:
                am = AccessMode.C;
                break;
            case COMMUTATIVE:
                am = AccessMode.CV;
                break;
        }

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        String finalPath;
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                finalPath = mainAccessToFile(app, dirName, loc, am, null, true);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("File (dir) target Location: " + finalPath);
                }
                break;
            default:
                finalPath = null;
                ErrorManager.error("ERROR: Unrecognised protocol requesting openDirectory " + dirName);
        }

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.OPEN_DIRECTORY.getType());
        }

        return finalPath;
    }

    @Override
    public void closeFile(Long appId, String fileName, Direction mode) {
        Application app = Application.registerApplication(appId);
        closeFile(app, fileName, mode);
    }

    /**
     * Closes the opened file version.
     *
     * @param app application closing the file.
     * @param fileName File name.
     * @param mode Access mode.
     */
    public void closeFile(Application app, String fileName, Direction mode) {

        // if (Tracer.isActivated()) {
        // Tracer.emitEvent(TraceEvent.CLOSE_FILE.getId(),
        // TraceEvent.CLOSE_FILE.getType());
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
            case IN_DELETE:
                am = AccessMode.R;
                break;
            case OUT:
                am = AccessMode.W;
                break;
            case INOUT:
                am = AccessMode.RW;
                break;
            case CONCURRENT:
                am = AccessMode.C;
                break;
            case COMMUTATIVE:
                am = AccessMode.CV;
                break;
        }

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                finishAccessToFile(app, fileName, loc, am, null);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Closing file " + loc.getPath());
                }
                break;
            case PERSISTENT:
                // Nothing to do
                ErrorManager.warn("WARN: Cannot close file " + fileName + " with PSCO protocol");
                break;
            case BINDING:
                // Nothing to do
                ErrorManager.warn("WARN: Cannot close binding object " + fileName + " with PSCO protocol");
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
     * FatalErrorHandler INTERFACE
     * ************************************************************************************************************
     */

    @Override
    public boolean handleError() {
        return handleFatalError();
    }

    @Override
    public boolean handleFatalError() {
        ErrorManager.info("Shutting down COMPSs...", null, System.err);
        new Thread() {

            public void run() {
                ErrorManager.logError("Error detected. Shutting down COMPSs", null);
                COMPSsRuntimeImpl.this.stopIT(true);
                System.err.println("Shutting down the running process");
                System.exit(1);
            }
        }.start();
        return true;
    }

    /*
     * ************************************************************************************************************
     * PRIVATE HELPER METHODS
     * ************************************************************************************************************
     */
    private boolean hasReturn(List<Parameter> parameters) {
        boolean hasReturn = false;
        if (parameters.size() != 0) {
            Parameter lastParam = parameters.get(parameters.size() - 1);
            DataType type = lastParam.getType();
            hasReturn = (lastParam.getDirection() == Direction.OUT && (type == DataType.OBJECT_T
                || type == DataType.PSCO_T || type == DataType.EXTERNAL_PSCO_T || type == DataType.BINDING_OBJECT_T));
        }

        return hasReturn;
    }

    private int addParameter(Application app, Object content, DataType type, Direction direction, StdIOStream stream,
        String prefix, String name, String pyType, double weight, boolean keepRename, ArrayList<Parameter> pars,
        int offset, String[] vals) {
        long appId = app.getId();
        switch (type) {
            case DIRECTORY_T:
                try {
                    String dirName = (String) content;
                    File dirFile = new File(dirName);
                    String originalName = dirFile.getName();
                    String canonicalPath = dirFile.getCanonicalPath();
                    String fullPath = ProtocolType.DIR_URI.getSchema() + canonicalPath;
                    DataLocation location = createLocation(fullPath);
                    pars.add(new DirectoryParameter(direction, stream, prefix, name, pyType, weight, keepRename,
                        location, originalName));
                } catch (Exception e) {
                    LOGGER.error(ERROR_DIR_NAME + " : " + e.getMessage());
                    ErrorManager.fatal(ERROR_DIR_NAME, e);
                }
                break;
            case FILE_T:
                try {
                    String fileName = (String) content;
                    String originalName = new File(fileName).getName();
                    DataLocation location = createLocation((String) content);
                    pars.add(new FileParameter(direction, stream, prefix, name, pyType, weight, keepRename, location,
                        originalName));
                } catch (Exception e) {
                    LOGGER.error(ERROR_FILE_NAME, e);
                    ErrorManager.fatal(ERROR_FILE_NAME, e);
                }
                break;
            case OBJECT_T:
            case PSCO_T:
                int code = oReg.newObjectParameter(appId, content);
                pars.add(new ObjectParameter(direction, stream, prefix, name, pyType, weight, content, code));
                break;
            case STREAM_T:
                int streamCode = oReg.newObjectParameter(appId, content);
                pars.add(new StreamParameter(direction, stream, prefix, name, content, streamCode));
                break;
            case EXTERNAL_STREAM_T:
                try {
                    String fileName = (String) content;
                    String canonicalPath = new File(fileName).getCanonicalPath();
                    String locationPath = ProtocolType.EXTERNAL_STREAM_URI.getSchema() + canonicalPath;
                    DataLocation location = createLocation(locationPath);
                    String originalName = new File(fileName).getName();
                    pars.add(new ExternalStreamParameter(direction, stream, prefix, name, location, originalName));
                } catch (Exception e) {
                    LOGGER.error(ERROR_FILE_NAME, e);
                    ErrorManager.fatal(ERROR_FILE_NAME, e);
                }
                break;
            case EXTERNAL_PSCO_T:
                String id = (String) content;
                pars.add(
                    new ExternalPSCOParameter(direction, stream, prefix, name, weight, id, externalObjectHashcode(id)));
                break;
            case BINDING_OBJECT_T:
                String value = (String) content;
                if (value.contains(":")) {
                    String[] fields = value.split(":");
                    if (fields.length == 3) {
                        String extObjectId = fields[0];
                        int extObjectType = Integer.parseInt(fields[1]);
                        int extObjectElements = Integer.parseInt(fields[2]);
                        pars.add(new BindingObjectParameter(direction, stream, prefix, name, pyType, weight,
                            new BindingObject(extObjectId, extObjectType, extObjectElements),
                            externalObjectHashcode(extObjectId)));
                    } else {
                        LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    }
                } else {
                    LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                }
                break;
            case COLLECTION_T:
                // A collection value contains the file of the collection object and the collection
                // elements, separated by spaces
                String[] values = vals == null ? ((String) content).split(" ") : vals;
                String collectionId = values[offset];
                int numOfElements = Integer.parseInt(values[offset + 1]);
                String colPyType = values[offset + 2];
                // The elements of the collection are all the elements of the list except for the first one
                // Each element is defined by TYPE VALUE PYTHON_CONTENT_TYPE
                // Also note the +3 offset!
                List<DataType> contentTypes = new ArrayList<>();
                List<String> contentIds = new ArrayList<>();
                ArrayList<Parameter> collectionParameters = new ArrayList<>();
                // Ret = number of read elements by this recursive step (atm 3: id + numOfElements + pyContentType)
                int ret = 3;
                for (int j = 0; j < numOfElements; ++j) {
                    // First element is the type, translate it to the corresponding DataType field by direct indexing
                    int idx = Integer.parseInt(values[offset + ret]);
                    DataType dataType = DataType.values()[idx];
                    contentTypes.add(dataType);
                    // Second element is the content
                    contentIds.add(values[offset + ret + 1]);
                    DataType elemType = contentTypes.get(j);
                    Direction elemDir = direction;
                    // Third element is the Python type of the object
                    String elemPyType = values[offset + ret + 2];
                    // Prepare stuff for recursive call
                    Object elemContent = elemType == DataType.COLLECTION_T ? values : contentIds.get(j);
                    // N/A to non-direct parameters
                    StdIOStream elemStream = StdIOStream.UNSPECIFIED;
                    String elemPrefix = Constants.PREFIX_EMPTY;
                    String elemName = name + "." + j;
                    // Add @ only for the first time
                    // This names elements as @collection.0, @collection.1, etc
                    // Easily extended in the case of nested collections
                    // @collection.1.0.1.2
                    // Means that this is the third element of the second element of the first element
                    // of the named collection "collection1"
                    if (!elemName.startsWith("@")) {
                        elemName = "@" + elemName;
                    }
                    ret += addParameter(app, elemContent, elemType, elemDir, elemStream, elemPrefix, elemName,
                        elemPyType, weight, keepRename, collectionParameters, offset + ret + 1, values) + 2;
                }
                CollectionParameter cp = new CollectionParameter(collectionId, collectionParameters, direction, stream,
                    prefix, name, colPyType, weight, keepRename);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER
                        .debug("Add Collection " + cp.getName() + " with " + cp.getParameters().size() + " parameters");
                    LOGGER.debug(cp.toString());
                }
                pars.add(cp);
                return ret;
            case DICT_COLLECTION_T:
                // TODO: Simplify this case.
                // A dictionary collection value contains the file of the dictionary collection object
                // and the dictionary collection elements, separated by spaces
                String[] values1 = vals == null ? ((String) content).split(" ") : vals;
                String dictCollectionId = values1[offset];
                int numOfEntries = Integer.parseInt(values1[offset + 1]);
                String dictColPyType = values1[offset + 2];
                // Each element is defined by TYPE VALUE PYTHON_CONTENT_TYPE. Also note the +3 offset!
                ArrayList<Parameter> dictCollectionParametersKeys = new ArrayList<>();
                ArrayList<Parameter> dictCollectionParametersValues = new ArrayList<>();
                // dret = number of read elements by this recursive step (atm 3: id + numOfEntries + pyContentType)
                int pointer = 3;
                for (int j = 0; j < numOfEntries; ++j) {
                    // First element is the type, translate it to the corresponding DataType field by direct indexing
                    int idKey = Integer.parseInt(values1[offset + pointer]);
                    DataType dataTypeKey = DataType.values()[idKey];
                    // Second element is the content
                    String contentKey = values1[offset + pointer + 1];
                    // Third element is the Python type of the object
                    final String elemPyTypeKey = values1[offset + pointer + 2];

                    // N/A to non-direct parameters
                    final StdIOStream elemStreamKey = StdIOStream.UNSPECIFIED;
                    final String elemPrefixKey = Constants.PREFIX_EMPTY;

                    String elemNameKey = name + "." + j;
                    // Add @key only for the first time - as in collections
                    if (!elemNameKey.startsWith("@key")) {
                        elemNameKey = "@key" + elemNameKey;
                    }
                    Direction elemDirKey = direction;

                    // Key element recursive call
                    Object elemContentKey = contentKey;
                    int extraKey = 2;
                    if (dataTypeKey == DataType.DICT_COLLECTION_T || dataTypeKey == DataType.COLLECTION_T) {
                        elemContentKey = values1;
                        pointer += 1;
                        extraKey = 0;
                    }
                    int kDret = addParameter(app, elemContentKey, dataTypeKey, elemDirKey, elemStreamKey, elemPrefixKey,
                        elemNameKey, elemPyTypeKey, weight, keepRename, dictCollectionParametersKeys, offset + pointer,
                        values1) + extraKey;
                    pointer += kDret;

                    // Next three elements correspond to the VALUE
                    // First element is the type, translate it to the corresponding DataType field by direct indexing
                    int idValue = Integer.parseInt(values1[offset + pointer]);
                    DataType dataTypeValue = DataType.values()[idValue];
                    // Second element is the content
                    String contentValue = values1[offset + pointer + 1];
                    // Third element is the Python type of the object
                    final String elemPyTypeValue = values1[offset + pointer + 2];

                    // N/A to non-direct parameters
                    final StdIOStream elemStreamValue = StdIOStream.UNSPECIFIED;
                    final String elemPrefixValue = Constants.PREFIX_EMPTY;

                    String elemNameValue = name + "." + j;
                    // Add @key only for the first time - as in collections
                    if (!elemNameValue.startsWith("@value")) {
                        elemNameValue = "@value" + elemNameKey;
                    }
                    Direction elemDirValue = direction;

                    // Value element recursive call
                    Object elemContentValue = contentValue;
                    int extraValue = 2;
                    if (dataTypeValue == DataType.DICT_COLLECTION_T || dataTypeValue == DataType.COLLECTION_T) {
                        elemContentValue = values1;
                        pointer += 1;
                        extraValue = 0;
                    }
                    int vDret = addParameter(app, elemContentValue, dataTypeValue, elemDirValue, elemStreamValue,
                        elemPrefixValue, elemNameValue, elemPyTypeValue, weight, keepRename,
                        dictCollectionParametersValues, offset + pointer, values1) + extraValue;
                    pointer += vDret;
                }
                Map<Parameter, Parameter> dictCollectionParams =
                    IntStream.range(0, dictCollectionParametersKeys.size()).boxed().collect(
                        Collectors.toMap(dictCollectionParametersKeys::get, dictCollectionParametersValues::get));
                DictCollectionParameter dcp = new DictCollectionParameter(dictCollectionId, dictCollectionParams,
                    direction, stream, prefix, name, dictColPyType, weight, keepRename);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Add Dictionary Collection " + dcp.getName() + " with " + dcp.getParameters().size()
                        + " entries");
                    LOGGER.debug(dcp.toString());
                }
                pars.add(dcp);
                return pointer;
            default:
                // Basic types (including String)
                // The only possible direction is IN, warn otherwise
                if (direction != Direction.IN && direction != Direction.IN_DELETE) {
                    LOGGER.warn(WARN_WRONG_DIRECTION + "Parameter " + name
                        + " is a basic type, therefore it must have IN direction");
                }
                pars.add(new BasicTypeParameter(type, Direction.IN, stream, prefix, name, content, weight, "null"));
                break;
        }
        return 1;
    }

    /*
     * *********************************************************************************************************
     * *********************************** PRIVATE HELPER METHODS **********************************************
     * *********************************************************************************************************
     */
    private List<Parameter> processParameters(Application app, int parameterCount, Object[] parameters) {
        ArrayList<Parameter> pars = new ArrayList<>();
        // Parameter parsing needed, object is not serializable
        for (int i = 0; i < parameterCount; ++i) {
            Object content = parameters[NUM_FIELDS_PER_PARAM * i];
            DataType type = (DataType) parameters[NUM_FIELDS_PER_PARAM * i + 1];
            if (type == null) {
                type = DataType.NULL_T;
            }
            Direction direction = (Direction) parameters[NUM_FIELDS_PER_PARAM * i + 2];
            StdIOStream stream = (StdIOStream) parameters[NUM_FIELDS_PER_PARAM * i + 3];
            String prefix = (String) parameters[NUM_FIELDS_PER_PARAM * i + 4];
            String name = (String) parameters[NUM_FIELDS_PER_PARAM * i + 5];
            // String pyContent = (String) parameters[NUM_FIELDS_PER_PARAM * i + 6];
            double weight = Double
                .parseDouble(EnvironmentLoader.loadFromEnvironment((String) parameters[NUM_FIELDS_PER_PARAM * i + 7]));
            boolean keepRename = (Boolean) parameters[NUM_FIELDS_PER_PARAM * i + 8];
            // Add parameter to list
            // This function call is isolated for better readability and to easily
            // allow recursion in the case of collections
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(" Parameter " + i + " has type " + type.name());
            }
            addParameter(app, content, type, direction, stream, prefix, name, null, weight, keepRename, pars, 0, null);
        }

        // Return parameters
        return pars;
    }

    private int externalObjectHashcode(String id) {
        int hashCode = 7;
        for (int i = 0; i < id.length(); ++i) {
            hashCode = hashCode * 31 + id.charAt(i);
        }

        return hashCode;
    }

    private void finishAccessToFile(Application app, String fileName, DataLocation loc, AccessMode am, String destDir) {
        FileAccessParams fap = new FileAccessParams(app, am, loc);
        ap.finishAccessToFile(loc, fap, destDir);
    }

    private String mainAccessToFile(Application app, String fileName, DataLocation loc, AccessMode am, String destDir,
        boolean isDirectory) {
        // Tell the AP that the application wants to access a file.
        FileAccessParams fap = new FileAccessParams(app, am, loc);
        DataLocation targetLocation;
        if (isDirectory) {
            targetLocation = ap.mainAccessToDirectory(app, loc, fap, destDir);
        } else {
            targetLocation = ap.mainAccessToFile(app, loc, fap, destDir);
        }

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

    private Object mainAccessToObject(Application app, Object obj, int hashCode) {
        boolean validValue = ap.isCurrentRegisterValueValid(hashCode);
        if (validValue) {
            // Main code is still performing the same modification.
            // No need to register it as a new version.
            return null;
        }

        // Otherwise we request it from a task
        return ap.mainAccessToObject(app, obj, hashCode);
    }

    private String mainAccessToExternalPSCO(Application app, String fileName, DataLocation loc) {
        String id = ((PersistentLocation) loc).getId();
        int hashCode = externalObjectHashcode(id);
        boolean validValue = ap.isCurrentRegisterValueValid(hashCode);
        if (validValue) {
            // Main code is still performing the same modification.
            // No need to register it as a new version.
            return fileName;
        }

        // Otherwise we request it from a task
        return ap.mainAccessToExternalPSCO(app, id, hashCode);
    }

    private String mainAccessToBindingObject(Application app, String fileName, BindingObjectLocation loc) {
        String id = loc.getId();
        int hashCode = externalObjectHashcode(id);
        boolean validValue = ap.isCurrentRegisterValueValid(hashCode);
        if (validValue) {
            // Main code is still performing the same modification.
            // No need to register it as a new version.
            return fileName;
        }

        // Otherwise we request it from a task
        return ap.mainAccessToBindingObject(app, loc.getBindingObject(), hashCode);
    }

    private DataLocation createLocation(String fileName) throws IOException {
        // Check if fileName contains schema
        SimpleURI uri = new SimpleURI(fileName);
        if (uri.getSchema().isEmpty()) {
            if(fileName.startsWith("/")){
                // todo: make pretty and sure it works
                uri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + fileName);
            } else{
                // Add default File scheme and wrap local paths
                String canonicalPath = new File(fileName).getCanonicalPath();
                uri = new SimpleURI(ProtocolType.FILE_URI.getSchema() + canonicalPath);
            }
        }

        // Check host
        Resource host = Comm.getAppHost();
        String hostName = uri.getHost();
        if (hostName != null && !hostName.isEmpty()) {
            host = ResourcesPool.getResource(hostName);
            if (host == null) {
                ErrorManager.error("Host " + hostName + " not found when creating data location.");
            }
        }

        // Create location
        return DataLocation.createLocation(host, uri);
    }

    @Override
    public void removeApplicationData(Long appId) {
        Application app = Application.registerApplication(appId);
        ap.deleteAllApplicationDataRequest(app);
    }

}
