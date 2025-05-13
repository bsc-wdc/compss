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
package es.bsc.compss.api.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.COMPSsPaths;
import es.bsc.compss.api.ApplicationRunner;
import es.bsc.compss.api.COMPSsRuntime;
import es.bsc.compss.api.ParameterCollectionMonitor;
import es.bsc.compss.api.ParameterMonitor;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.components.monitor.impl.RuntimeMonitor;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.loader.LoaderAPI;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.BindingObject;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.ErrorHandler;
import es.bsc.compss.types.WallClockTimerTask;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.Direction;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.annotations.parameter.StdIOStream;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.access.BindingObjectMainAccess;
import es.bsc.compss.types.data.access.DirectoryMainAccess;
import es.bsc.compss.types.data.access.ExternalPSCObjectMainAccess;
import es.bsc.compss.types.data.access.FileMainAccess;
import es.bsc.compss.types.data.access.ObjectMainAccess;
import es.bsc.compss.types.data.location.BindingObjectLocation;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.PersistentLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.data.params.BindingObjectData;
import es.bsc.compss.types.data.params.CollectionData;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.data.params.FileData;
import es.bsc.compss.types.data.params.ObjectData;
import es.bsc.compss.types.implementations.ExecType;
import es.bsc.compss.types.implementations.ImplementationDescription;
import es.bsc.compss.types.implementations.definition.ContainerDescription;
import es.bsc.compss.types.listeners.CancelTaskGroupOnResourceCreation;
import es.bsc.compss.types.parameter.impl.BasicTypeParameter;
import es.bsc.compss.types.parameter.impl.BindingObjectParameter;
import es.bsc.compss.types.parameter.impl.CollectiveParameter;
import es.bsc.compss.types.parameter.impl.DirectoryParameter;
import es.bsc.compss.types.parameter.impl.ExternalPSCOParameter;
import es.bsc.compss.types.parameter.impl.ExternalStreamParameter;
import es.bsc.compss.types.parameter.impl.FileParameter;
import es.bsc.compss.types.parameter.impl.ObjectParameter;
import es.bsc.compss.types.parameter.impl.Parameter;
import es.bsc.compss.types.parameter.impl.StreamParameter;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.resources.MasterResourceImpl;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourcesPool;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.EnvironmentLoader;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.FileOpsManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.RuntimeConfigManager;
import es.bsc.compss.util.SignatureBuilder;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.time.Instant;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.Timer;
import java.util.TimerTask;

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
    private static final String WARN_NULL_PARAM = "WARNING: Optional parameter: ";

    // COMPSS Version and buildnumber attributes
    private static String COMPSs_VERSION = null;
    private static String COMPSs_BUILDNUMBER = null;

    // Boolean for initialization
    private static boolean initialized = false;
    private boolean stopped = false;

    // Number of fields per parameter
    public static final int NUM_FIELDS_PER_PARAM = 9;

    // Language
    protected static final Lang DEFAULT_LANG;

    // Registries
    private static ObjectRegistry oReg;
    private static StreamRegistry sReg;

    // Components
    private static AccessProcessor ap;
    private static TaskDispatcher td;

    // Monitor
    private static RuntimeMonitor runtimeMonitor;

    // Application Timer
    private static Timer timer = null;

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.API);
    // Data Provenance logger
    private static final Logger DP_LOGGER = LogManager.getLogger(Loggers.DATA_PROVENANCE);
    private static final boolean DP_ENABLED = Boolean.parseBoolean(System.getProperty(COMPSsConstants.DATA_PROVENANCE));

    static {
        String defaultLang = System.getProperty(COMPSsConstants.LANG);
        Lang lang;
        if (defaultLang == null) {
            lang = Lang.JAVA;
        } else {
            lang = Lang.valueOf(defaultLang.toUpperCase());
        }
        DEFAULT_LANG = lang;

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


    private static void setPropertyFromRuntime(String propertyName, String managerValue) {
        if (managerValue != null && System.getProperty(propertyName) == null) {
            System.setProperty(propertyName, managerValue);
        }
    }

    // Code Added to support configuration files
    private static void setPropertiesFromRuntime(RuntimeConfigManager manager) {
        try {
            if (manager != null) {
                setPropertyFromRuntime(COMPSsConstants.DEPLOYMENT_ID, manager.getDeploymentId());
                setPropertyFromRuntime(COMPSsConstants.MASTER_NAME, manager.getMasterName());
                setPropertyFromRuntime(COMPSsConstants.MASTER_PORT, manager.getMasterPort());
                setPropertyFromRuntime(COMPSsConstants.APP_NAME, manager.getAppName());
                setPropertyFromRuntime(COMPSsConstants.TASK_SUMMARY, manager.getTaskSummary());
                setPropertyFromRuntime(COMPSsConstants.LOG_DIR, manager.getLogDir());
                setPropertyFromRuntime(COMPSsConstants.WORKING_DIR, manager.getWorkingDir());
                setPropertyFromRuntime(COMPSsConstants.LOG4J, manager.getLog4jConfiguration());
                setPropertyFromRuntime(COMPSsConstants.RES_FILE, manager.getResourcesFile());
                setPropertyFromRuntime(COMPSsConstants.RES_SCHEMA, manager.getResourcesSchema());
                setPropertyFromRuntime(COMPSsConstants.PROJ_FILE, manager.getProjectFile());
                setPropertyFromRuntime(COMPSsConstants.PROJ_SCHEMA, manager.getProjectSchema());
                setPropertyFromRuntime(COMPSsConstants.SCHEDULER, manager.getScheduler());
                setPropertyFromRuntime(COMPSsConstants.MONITOR, Long.toString(manager.getMonitorInterval()));
                setPropertyFromRuntime(COMPSsConstants.GAT_ADAPTOR_PATH, manager.getGATAdaptor());
                setPropertyFromRuntime(COMPSsConstants.GAT_BROKER_ADAPTOR, manager.getGATBrokerAdaptor());
                setPropertyFromRuntime(COMPSsConstants.GAT_FILE_ADAPTOR, manager.getGATFileAdaptor());
                if (System.getProperty(COMPSsConstants.REUSE_RESOURCES_ON_BLOCK) == null
                    || System.getProperty(COMPSsConstants.REUSE_RESOURCES_ON_BLOCK).isEmpty()) {
                    System.setProperty(COMPSsConstants.REUSE_RESOURCES_ON_BLOCK,
                        Boolean.toString(manager.getReuseResourcesOnBlock()));
                }
                if (System.getProperty(COMPSsConstants.ENABLED_NESTED_TASKS_DETECTION) == null
                    || System.getProperty(COMPSsConstants.ENABLED_NESTED_TASKS_DETECTION).isEmpty()) {
                    System.setProperty(COMPSsConstants.ENABLED_NESTED_TASKS_DETECTION,
                        Boolean.toString(manager.isNestedDetectionEnabled()));
                }
                setPropertyFromRuntime(COMPSsConstants.WORKER_CP, manager.getWorkerCP());
                setPropertyFromRuntime(COMPSsConstants.WORKER_JVM_OPTS, manager.getWorkerJVMOpts());

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

                setPropertyFromRuntime(COMPSsConstants.SERVICE_NAME, manager.getServiceName());
                if (System.getProperty(COMPSsConstants.COMM_ADAPTOR) == null) {
                    if (manager.getCommAdaptor() != null) {
                        System.setProperty(COMPSsConstants.COMM_ADAPTOR, manager.getCommAdaptor());
                    } else {
                        System.setProperty(COMPSsConstants.COMM_ADAPTOR, COMPSsDefaults.ADAPTOR);
                    }
                }
                if (System.getProperty(COMPSsConstants.CONN) == null) {
                    if (manager.getConn() != null) {
                        System.setProperty(COMPSsConstants.CONN, manager.getConn());
                    } else {
                        System.setProperty(COMPSsConstants.CONN, COMPSsDefaults.CONNECTOR);
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
                if (System.getProperty(COMPSsConstants.EXTRAE_WORKING_DIR) == null) {
                    System.setProperty(COMPSsConstants.EXTRAE_WORKING_DIR, manager.getExtraeWDir());
                }
                if (System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) == null) {
                    System.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, manager.getCustomExtraeFile());
                }
                if (System.getProperty(COMPSsConstants.TRACING_TASK_DEPENDENCIES) == null) {
                    System.setProperty(COMPSsConstants.TRACING_TASK_DEPENDENCIES,
                        String.valueOf(manager.getTracingTaskDep()));
                }
                if (System.getProperty(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE) == null) {
                    System.setProperty(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE, manager.getCustomExtraeFilePython());
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
        setDefaultProperty(COMPSsConstants.DEPLOYMENT_ID, COMPSsDefaults.DEPLOYMENT_ID);
        setDefaultProperty(COMPSsConstants.RES_SCHEMA, COMPSsPaths.LOCAL_RES_SCHEMA);
        setDefaultProperty(COMPSsConstants.PROJ_SCHEMA, COMPSsPaths.LOCAL_PROJECT_SCHEMA);
        setDefaultProperty(COMPSsConstants.GAT_ADAPTOR_PATH, COMPSsPaths.GAT_ADAPTOR_LOCATION);
        setDefaultProperty(COMPSsConstants.COMM_ADAPTOR, COMPSsDefaults.ADAPTOR);
        setDefaultProperty(COMPSsConstants.REUSE_RESOURCES_ON_BLOCK, COMPSsDefaults.REUSE_RESOURCES_ON_BLOCK);
        setDefaultProperty(COMPSsConstants.ENABLED_NESTED_TASKS_DETECTION,
            COMPSsDefaults.ENABLED_NESTED_TASKS_DETECTION);
        setDefaultProperty(COMPSsConstants.CONN, COMPSsDefaults.CONNECTOR);
        setDefaultProperty(COMPSsConstants.SCHEDULER, COMPSsDefaults.SCHEDULER);
        setDefaultProperty(COMPSsConstants.TRACING, COMPSsDefaults.TRACING);
        setDefaultProperty(COMPSsConstants.EXTRAE_WORKING_DIR, ".");
        setDefaultProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, COMPSsDefaults.CUSTOM_EXTRAE_FILE);
        setDefaultProperty(COMPSsConstants.TASK_EXECUTION, COMPSsConstants.TaskExecution.COMPSS.toString());
    }

    private static void setDefaultProperty(String propertyName, String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null || propertyValue.isEmpty()) {
            System.setProperty(propertyName, defaultValue);
        }
    }

    private static InputStream findPropertiesConfigFile() {
        InputStream stream = COMPSsRuntimeImpl.class.getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
        if (stream == null) {
            stream = COMPSsRuntimeImpl.class.getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
            if (stream == null) {
                // System.err.println("IT properties file not defined. Looking at classLoader...");
                stream = COMPSsRuntimeImpl.class.getClassLoader().getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                if (stream == null) {
                    stream = COMPSsRuntimeImpl.class.getClassLoader()
                        .getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
                    if (stream == null) {
                        // System.err.println("IT properties file not found in classloader. Looking at system
                        // resource...");
                        stream = ClassLoader.getSystemResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                        if (stream == null) {
                            stream =
                                ClassLoader.getSystemResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
                            if (stream == null) {
                                // System.err.println("IT properties file not found. Looking at parent ClassLoader");
                                stream = COMPSsRuntimeImpl.class.getClassLoader().getParent()
                                    .getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
                                if (stream == null) {
                                    stream = COMPSsRuntimeImpl.class.getClassLoader().getParent()
                                        .getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);

                                }
                            }
                        }
                    }
                }
            }
        }
        return stream;
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
     * ***************************************** RUNTIME CONTROL **************************************************
     * ************************************************************************************************************
     */
    @Override
    public synchronized void startIT() {
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.STATIC_IT);
            Tracer.emitEvent(TraceEvent.START);
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

                // Initialize main runtime components
                td = new TaskDispatcher();
                ap = new AccessProcessor(td);

                // Initialize runtime tools components
                long monitoringPeriod = 1000;
                try {
                    String monitoringPeriodStr = System.getProperty(COMPSsConstants.MONITOR);
                    monitoringPeriod = Long.parseLong(monitoringPeriodStr);
                } catch (Exception e) {
                    // Keep default value
                }
                runtimeMonitor = new RuntimeMonitor(ap, td, monitoringPeriod);
                Application.setGH(runtimeMonitor.getGraphHandler());

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
            Tracer.emitEventEnd(TraceEvent.START);
        }

        if (DP_ENABLED) {
            DP_LOGGER.info(COMPSs_VERSION);
            DP_LOGGER.info(System.getProperty(COMPSsConstants.APP_NAME));
            DP_LOGGER.info(System.getProperty(COMPSsConstants.OUTPUT_PROFILE));
            DP_LOGGER.info(Instant.now().toString());
        }
    }

    @Override
    public void stopIT(boolean terminate) {
        synchronized (this) {
            if (!stopped) {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(TraceEvent.STOP);
                }

                LOGGER.debug("Stopping Wall Clock limit Timer");
                if (timer != null) {
                    timer.cancel();
                }

                LOGGER.debug("Cancelling all remaining tasks...");
                // In some case, when runtime is stop because an error the java process is not stopped
                // because some threads are blocked at barriers waiting for the end of tasks
                for (Application app : Application.getApplications()) {
                    ap.cancelApplicationTasks(app);
                    // ap.barrier(app);
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
                runtimeMonitor.shutdown();

                // Stop runtime components
                LOGGER.debug("Stopping AP...");
                if (ap != null) {
                    ap.shutdown();
                } else {
                    LOGGER.debug("AP was not initialized...");
                }
                runtimeMonitor.getGraphHandler().removeCurrentGraph();

                LOGGER.debug("Stopping TD...");
                if (td != null) {
                    td.shutdown();
                } else {
                    LOGGER.debug("TD was not initialized...");
                }

                LOGGER.debug("Stopping Comm...");
                Comm.stop(CoreManager.getSignaturesToCEIds());
                LOGGER.debug("Runtime stopped");
                stopped = true;
                // LOGGER.debug("Releasing all barriers...");
                // In some case, when runtime is stop because an error the java process is not stopped
                // because some threads are blocked at barriers waiting for the end of tasks
                for (Application app : Application.getApplications()) {
                    app.getBaseTaskGroup().releaseBarrier();
                }
            } else {
                LOGGER.debug("Duplicated Stop");
                throw (new RuntimeException("Runtime already stopped"));
            }

        }

        LOGGER.warn("Execution Finished");

        if (DP_ENABLED) {
            DP_LOGGER.info(Instant.now().toString());
        }

    }

    /*
     * ************************************************************************************************************
     * ************************************* RUNTIME SETUP MANAGEMENT *********************************************
     * ************************************************************************************************************
     */
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
        return Comm.getAppHost().getWorkingDirectory();
    }

    @Override
    public String getApplicationDirectory() {
        return LoggerManager.getLogDir();
    }

    /**
     * Returns the action orchestrator associated to the Runtime (only for testing purposes).
     *
     * @return The action orchestrator associated to the Runtime.
     */
    public static ActionOrchestrator getOrchestrator() {
        return td;
    }

    /*
     * ************************************************************************************************************
     * ************************************* APPLICATION MANAGEMENT ***********************************************
     * ************************************************************************************************************
     */

    @Override
    public long registerApplication(String parallelismSource, ApplicationRunner runner) {
        Application app = Application.registerApplication(parallelismSource, runner);
        return app.getId();
    }

    @Override
    public void deregisterApplication(Long appId) {
        Application app = Application.deregisterApplication(appId);
        ap.deleteAllApplicationDataRequest(app);
    }

    @Override
    public void registerCoreElement(String coreElementSignature, String implSignature, String implConstraints,
        String implType, String implLocal, String implIO, String[] prolog, String[] epilog, String[] container,
        String... implTypeArgs) {

        LOGGER.info("Registering CoreElement " + coreElementSignature);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("\t - Implementation: " + implSignature);
            LOGGER.debug("\t - Constraints   : " + implConstraints);
            LOGGER.debug("\t - Local process : " + implLocal);
            LOGGER.debug("\t - Type          : " + implType);
            LOGGER.debug("\t - I/O           : " + implIO);
            LOGGER.debug("\t - Prolog        : ");
            for (String pro : prolog) {
                LOGGER.debug("\t\t -- : " + pro);
            }
            LOGGER.debug("\t - Epilog        : ");
            for (String epi : epilog) {
                LOGGER.debug("\t\t -- : " + epi);
            }

            LOGGER.debug("\t - Container        : ");
            for (String cont : container) {
                LOGGER.debug("\t\t -- : " + cont);
            }

            LOGGER.debug("\t - ImplTypeArgs  : ");
            for (String implTypeArg : implTypeArgs) {
                LOGGER.debug("\t\t Arg: " + implTypeArg);
            }
        }

        MethodResourceDescription mrd = new MethodResourceDescription(implConstraints);
        boolean isImplIO = Boolean.parseBoolean(implIO);
        if (isImplIO) {
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("Nulling computing resources for I/O task: " + implSignature);
            }
            mrd.setIOResources();
        }
        boolean isLocalImpl;
        isLocalImpl = Boolean.parseBoolean(implLocal);

        CoreElementDefinition ced = new CoreElementDefinition();
        ced.setCeSignature(coreElementSignature);

        ExecType pro = null;
        if (prolog != null && prolog.length > 0) {
            if (prolog.length != ExecType.ARRAY_LENGTH) {
                throw new IllegalArgumentException("Incorrect number of parameters in prolog.");
            }
            pro = new ExecType(prolog[0], prolog[1], Boolean.parseBoolean(prolog[2]));
            if (!pro.isAssigned()) {
                pro = null;
            }
        }

        ExecType epi = null;
        if (epilog != null && epilog.length > 0) {
            if (epilog.length != ExecType.ARRAY_LENGTH) {
                throw new IllegalArgumentException("Incorrect number of parameters in epilog.");
            }
            epi = new ExecType(epilog[0], epilog[1], Boolean.parseBoolean(epilog[2]));
            if (!epi.isAssigned()) {
                epi = null;
            }
        }

        ContainerDescription cont;
        if (container != null && container.length > 0 && container[0] != null && !container[0].isEmpty()
            && !container[0].equals(Constants.UNASSIGNED)) {
            String engineStr = container[0].toUpperCase();
            ContainerDescription.ContainerEngine engine = ContainerDescription.ContainerEngine.valueOf(engineStr);
            cont = new ContainerDescription(engine, container[1], container[2]);
        } else {
            cont = null;
        }

        ImplementationDescription<?, ?> implDef = ImplementationDescription.defineImplementation(implType,
            implSignature, isLocalImpl, mrd, pro, epi, cont, implTypeArgs);
        ced.addImplementation(implDef);

        td.registerNewCoreElement(ced);
    }

    @Override
    public void registerCoreElement(CoreElementDefinition ced) {
        LOGGER.info("Registering CoreElement " + ced.getCeSignature());
        if (LOGGER.isDebugEnabled()) {
            int implId = 0;
            for (ImplementationDescription<?, ?> implDef : ced.getImplementations()) {
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

    /*
     * ************************************************************************************************************
     * **************************************** DATA MANAGEMENT ***************************************************
     * ************************************************************************************************************
     */
    @Override
    public void registerData(Long appId, DataType type, Object stub, String data) {

        Application app = Application.registerApplication(appId);
        DataParams dp = null;
        switch (type) {
            case DIRECTORY_T:
            case FILE_T:
                try {
                    String fileName = (String) stub;
                    // Parse arguments to internal structures
                    DataLocation loc;
                    try {
                        loc = createLocation(ProtocolType.FILE_URI, fileName);
                    } catch (IOException ioe) {
                        ErrorManager.fatal(ERROR_FILE_NAME, ioe);
                        return;
                    }
                    dp = new FileData(loc);
                } catch (NullPointerException npe) {
                    LOGGER.error(ERROR_FILE_NAME, npe);
                    ErrorManager.fatal(ERROR_FILE_NAME, npe);
                }
                break;
            case OBJECT_T:
            case PSCO_T:
                int hashcode = oReg.newObjectParameter(appId, stub);
                dp = new ObjectData(hashcode);
                break;
            case STREAM_T:
                // int streamCode = oReg.newObjectParameter(stub);
                throw new UnsupportedOperationException("Not implemented yet.");
            case EXTERNAL_STREAM_T:
                try {
                    String fileName = (String) stub;
                    new File(fileName).getName();
                } catch (NullPointerException npe) {
                    LOGGER.error(ERROR_FILE_NAME, npe);
                    ErrorManager.fatal(ERROR_FILE_NAME, npe);
                }
                throw new UnsupportedOperationException("Not implemented yet.");
            case EXTERNAL_PSCO_T:
                // String id = (String) stub;
                throw new UnsupportedOperationException("Not implemented yet.");
            case BINDING_OBJECT_T:
                String value = (String) stub;
                if (value.contains(":")) {
                    String[] fields = value.split(":");
                    // if (fields.length == 3) {
                    // String extObjectId = fields[0];
                    // int extObjectType = Integer.parseInt(fields[1]);
                    // int extObjectElements = Integer.parseInt(fields[2]);
                    // BindingObject bo = new BindingObject(extObjectId, extObjectType, extObjectElements);
                    // new BindingObject(extObjectId, extObjectType, extObjectElements);
                    // int externalCode = externalObjectHashcode(extObjectId);
                    // externalObjectHashcode(extObjectId);
                    // }
                    if (fields.length != 3) {
                        LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                        ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    }
                } else {
                    LOGGER.error(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                    ErrorManager.fatal(ERROR_BINDING_OBJECT_PARAMS + " received value is " + value);
                }
                throw new UnsupportedOperationException("Not implemented yet.");
            case COLLECTION_T:
                dp = new CollectionData((String) stub);
                break;
            case DICT_COLLECTION_T:
                throw new UnsupportedOperationException("Not implemented yet.");
            default:
                // Basic types (including String)
                // Already passed in as a value
                break;
        }
        if (dp != null) {
            ap.registerRemoteData(app, dp, data);
        }
    }

    @Override
    public boolean bindExistingVersionToData(Long appId, String fileName, String dataId) {
        // Parse the file name
        DataLocation sourceLocation = null;
        try {
            sourceLocation = createLocation(ProtocolType.FILE_URI, fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
        }
        if (sourceLocation == null) {
            ErrorManager.fatal(ERROR_FILE_NAME);
        }

        Application app = Application.registerApplication(appId);
        FileData fd = new FileData(sourceLocation);
        return bindExistingVersionToData(app, fd, dataId);
    }

    @Override
    public boolean bindExistingVersionToData(Long appId, Object o, Integer hashCode, String dataId) {
        Application app = Application.registerApplication(appId);
        ObjectData od = new ObjectData(hashCode);
        return bindExistingVersionToData(app, od, dataId);
    }

    private boolean bindExistingVersionToData(Application app, DataParams data, String dataId) {
        LOGGER.debug("Binding " + data.getDescription() + "'s last version to data " + dataId);
        LogicalData lastVersion = ap.getDataLastVersion(app, data);
        if (lastVersion != null) {
            LogicalData src = Comm.getData(dataId);
            try {
                LOGGER.debug("Binding " + src.getKnownAlias() + " to data " + dataId);
                LogicalData.link(src, lastVersion);
                return true;
            } catch (CommException e) {
                LOGGER.warn("Could not link " + dataId + " and " + lastVersion.getName());
            }
        }
        return false;
    }

    @Override
    public void getFile(Long appId, String fileName) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.GET_FILE);
        }

        // Parse the file name
        DataLocation sourceLocation = null;
        try {
            sourceLocation = createLocation(ProtocolType.FILE_URI, fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
        }
        if (sourceLocation == null) {
            ErrorManager.fatal(ERROR_FILE_NAME);
        }

        LOGGER.debug("Getting file " + fileName);
        Application app = Application.registerApplication(appId);
        String renamedPath = openFileSystemData(app, fileName, Direction.INOUT, false);
        // If renamePth is the same as original, file has not accessed. Nothing to do.
        if (!renamedPath.equals(sourceLocation.getPath())) {
            try {
                String intermediateTmpPath = renamedPath + ".tmp";
                FileOpsManager.moveSync(new File(renamedPath), new File(intermediateTmpPath));
                closeFile(app, fileName, Direction.INOUT);
                ap.deleteData(app, new FileData(sourceLocation), true, false);
                // In the case of Java file can be stored in the Stream Registry
                if (sReg != null) {
                    sReg.deleteTaskFile(appId, fileName);
                }
                FileOpsManager.moveSync(new File(intermediateTmpPath), new File(fileName));
            } catch (IOException ioe) {
                LOGGER.error("Move not possible ", ioe);
            }
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.GET_FILE);
        }
    }

    @Override
    public String openFile(Long appId, String fileName, Direction mode) {
        Application app = Application.registerApplication(appId);
        return openFileSystemData(app, fileName, mode, false);
    }

    @Override
    public void getDirectory(Long appId, String dirName) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.GET_DIRECTORY);
        }

        // Parse the dir name
        DataLocation sourceLocation = null;
        try {
            sourceLocation = createLocation(ProtocolType.DIR_URI, dirName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_DIR_NAME, ioe);
        }
        if (sourceLocation == null) {
            ErrorManager.fatal(ERROR_DIR_NAME);
        }

        LOGGER.debug("Getting directory " + dirName);
        Application app = Application.registerApplication(appId);
        String renamedPath = openFileSystemData(app, dirName, Direction.IN, true);
        try {
            LOGGER.debug("Getting directory renamed path: " + renamedPath);
            String intermediateTmpPath = renamedPath + ".tmp";
            FileOpsManager.moveDirSync(new File(renamedPath), new File(intermediateTmpPath));
            closeFile(app, dirName, Direction.IN);

            ap.deleteData(app, new FileData(sourceLocation), true, false);
            // In the case of Java file can be stored in the Stream Registry
            if (sReg != null) {
                sReg.deleteTaskFile(appId, dirName);
            }

            FileOpsManager.moveDirSync(new File(intermediateTmpPath), new File(dirName));
        } catch (IOException ioe) {
            LOGGER.error("Move not possible ", ioe);
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.GET_DIRECTORY);
        }
    }

    @Override
    public String openDirectory(Long appId, String dirName, Direction mode) {
        Application app = Application.registerApplication(appId);
        return openFileSystemData(app, dirName, mode, true);
    }

    private String openFileSystemData(Application app, String fileName, Direction direction, boolean isDir) {
        LOGGER.info("Opening " + fileName + " in direction " + direction);
        TraceEvent tEvent = null;
        if (Tracer.isActivated()) {
            if (isDir) {
                tEvent = TraceEvent.OPEN_DIRECTORY;
            } else {
                tEvent = TraceEvent.OPEN_FILE;
            }
            Tracer.emitEvent(tEvent);
        }
        // Parse arguments to internal structures
        DataLocation loc;
        try {
            loc = createLocation(isDir ? ProtocolType.DIR_URI : ProtocolType.FILE_URI, fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            return null;
        }

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        String finalPath;
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                FileMainAccess<?, ?> access;
                if (isDir) {
                    access = DirectoryMainAccess.constructDMA(app, direction, loc);
                } else {
                    access = FileMainAccess.constructFMA(app, direction, loc);
                }
                finalPath = mainAccessToFile(access, fileName);
                if (LOGGER.isDebugEnabled()) {

                    LOGGER.debug("File " + (isDir ? "(dir) " : "") + "target Location: " + finalPath);
                }
                break;
            case PERSISTENT:
                String id = ((PersistentLocation) loc).getId();
                int hashCode = externalObjectHashcode(id);
                ExternalPSCObjectMainAccess eoap;
                eoap = ExternalPSCObjectMainAccess.constructEPOMA(app, Direction.INOUT, id, hashCode);

                // Otherwise we request it from a task
                try {
                    String newPscoId = ap.mainAccess(eoap);
                    finalPath = ProtocolType.PERSISTENT_URI.getSchema() + newPscoId;
                } catch (ValueUnawareRuntimeException e) {
                    finalPath = id;
                }
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("External PSCO target Location: " + finalPath);
                }
                break;

            default:
                finalPath = null;
                ErrorManager.error(
                    "ERROR: Unrecognised protocol requesting " + (isDir ? "openDirectory " : "openFile ") + fileName);
        }

        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(tEvent);
        }

        return finalPath;
    }

    @Override
    public boolean isFileAccessed(Long appId, String fileName) {
        DataLocation loc;
        try {
            loc = createLocation(ProtocolType.FILE_URI, fileName);
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
            loc = null;
        }
        if (loc != null) {
            Application app = Application.registerApplication(appId);
            FileData fd = new FileData(loc);
            return ap.alreadyAccessed(app, fd);
        } else {
            return false;
        }
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
     * @param direction Access mode.
     */
    public void closeFile(Application app, String fileName, Direction direction) {

        // if (Tracer.isActivated()) {
        // Tracer.emitEvent(TraceEvent.CLOSE_FILE.getId(),
        // TraceEvent.CLOSE_FILE.getType());
        // }
        LOGGER.info("Closing " + fileName + " in direction " + direction);

        // Parse arguments to internal structures
        DataLocation loc;
        try {
            loc = createLocation(ProtocolType.FILE_URI, fileName);
        } catch (Exception e) {
            ErrorManager.fatal(ERROR_FILE_NAME, e);
            return;
        }

        // Request AP that the application wants to access a FILE or a EXTERNAL_PSCO
        switch (loc.getType()) {
            case PRIVATE:
            case SHARED:
                FileMainAccess fma = FileMainAccess.constructFMA(app, direction, loc);
                ap.finishDataAccess(fma, null);
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

    @Override
    public Object getObject(Long appId, Object obj, int hashCode, String destDir) {
        /*
         * We know that the object has been accessed before by a task, otherwise the ObjectRegistry would have discarded
         * it and this method would not have been called.
         */
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.GET_OBJECT);
        }

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Getting object with hash code " + hashCode);
        }

        Application app = Application.registerApplication(appId);
        ObjectMainAccess<?, ?, ?> oap = ObjectMainAccess.constructOMA(app, Direction.INOUT, obj, hashCode);
        Object oUpdated;
        try {
            oUpdated = ap.mainAccess(oap);
        } catch (ValueUnawareRuntimeException e) {
            oUpdated = null;
        }
        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("Object obtained " + ((oUpdated == null) ? oUpdated : oUpdated.hashCode()));
        }

        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.GET_OBJECT);
        }

        return oUpdated;
    }

    @Override
    public String getBindingObject(Long appId, String fileName) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.GET_BINDING_OBJECT);
        }

        // Parse the file name
        LOGGER.debug(" Calling get binding object : " + fileName);
        BindingObject bo = BindingObject.generate(fileName);
        BindingObjectLocation boLoc = new BindingObjectLocation(Comm.getAppHost(), bo);
        String boId = boLoc.getId();
        int hashCode = externalObjectHashcode(boId);
        Application app = Application.registerApplication(appId);
        BindingObjectMainAccess boap = BindingObjectMainAccess.constructBOMA(app, Direction.INOUT, bo, hashCode);

        // Otherwise we request it from a task
        String finalPath;
        try {
            BindingObject newBO = ap.mainAccess(boap);
            String bindingObjectID = newBO.getName();
            finalPath = bindingObjectID;
        } catch (ValueUnawareRuntimeException e) {
            finalPath = bo.toString();
        }
        LOGGER.debug("Returning binding object as id: " + finalPath);
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.GET_BINDING_OBJECT);
        }
        return finalPath;
    }

    @Override
    public boolean deleteFile(Long appId, String fileName) {
        return deleteFile(appId, fileName, true, true);
    }

    @Override
    public boolean deleteFile(Long appId, String fileName, boolean waitForData, boolean applicationDelete) {
        // Check parameters
        if (fileName == null || fileName.isEmpty()) {
            return false;
        }

        LOGGER.info("Deleting File " + fileName + " with wait for data " + waitForData);

        // Emit event
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.DELETE);
        }

        // Parse the file name and translate the access mode
        try {
            DataLocation loc = createLocation(ProtocolType.FILE_URI, fileName);
            Application app = Application.registerApplication(appId);
            ap.deleteData(app, new FileData(loc), waitForData, applicationDelete);
            // Java case where task files are stored in the registry
            if (sReg != null) {
                sReg.deleteTaskFile(appId, fileName);
            }
        } catch (IOException ioe) {
            ErrorManager.fatal(ERROR_FILE_NAME, ioe);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TraceEvent.DELETE);
            }
        }
        LOGGER.info("File " + fileName + " Deleted.");
        // Return deletion was successful
        return true;
    }

    @Override
    public void deregisterObject(Long appId, Object o) {
        oReg.delete(appId, o);
    }

    @Override
    public void removeObject(Long appId, Object o, int hashcode) {
        Application app = Application.registerApplication(appId);
        // This will remove the object from the Object Registry and the Data Info Provider
        // eventually allowing the garbage collector to free it (better use of memory)
        ap.deleteData(app, new ObjectData(hashcode), false, false);
    }

    @Override
    public boolean deleteBindingObject(Long appId, String fileName) {
        // Check parameters
        if (fileName == null || fileName.isEmpty()) {
            return false;
        }

        LOGGER.info("Deleting BindingObject " + fileName);

        // Emit event
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.DELETE);
        }

        Application app = Application.registerApplication(appId);
        // Parse the binding object name and translate the access mode
        BindingObject bo = BindingObject.generate(fileName);
        int hashCode = externalObjectHashcode(bo.getId());
        ap.deleteData(app, new BindingObjectData(hashCode), false, false);
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.DELETE);
        }

        // Return deletion was successful
        return true;
    }

    /*
     * ************************************************************************************************************
     * **************************************** TASK MANAGEMENT ***************************************************
     * ************************************************************************************************************
     */
    // C
    @Override
    public int executeTask(Long appId, String methodClass, String onFailure, int timeOut, String methodName,
        boolean isPrioritary, int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated,
        boolean isDistributed, boolean hasTarget, Integer numReturns, int parameterCount, Object... parameters) {

        return executeTask(appId, Lang.C, false, methodClass, methodName, null, OnFailure.valueOf(onFailure), timeOut,
            isPrioritary, Constants.SINGLE_NODE, false, 0, isReplicated, isDistributed, hasTarget, numReturns,
            parameterCount, parameters);
    }

    // Python
    @Override
    public int executeTask(Long appId, String signature, String onFailure, int timeOut, boolean isPrioritary,
        int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, Integer numReturns, int parameterCount, Object... parameters) {
        Lang lang = Lang.PYTHON;
        if (DEFAULT_LANG == Lang.R) {
            lang = Lang.R;
        }
        return executeTask(appId, lang, true, null, null, signature, OnFailure.valueOf(onFailure), timeOut,
            isPrioritary, numNodes, isReduce, reduceChunkSize, isReplicated, isDistributed, hasTarget, numReturns,
            parameterCount, parameters);
    }

    // Java - Loader
    @Override
    public int executeTask(Long appId, Lang lang, String methodClass, String methodName, boolean isPrioritary,
        int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed,
        boolean hasTarget, int parameterCount, OnFailure onFailure, int timeOut, Object... parameters) {

        return executeTask(appId, lang, false, methodClass, methodName, null, onFailure, timeOut, isPrioritary,
            numNodes, isReduce, reduceChunkSize, isReplicated, isDistributed, hasTarget, null, parameterCount,
            parameters);
    }

    // Services
    @Override
    public int executeTask(Long appId, String namespace, String service, String port, String operation,
        boolean isPrioritary, int numNodes, boolean isReduce, int reduceChunkSize, boolean isReplicated,
        boolean isDistributed, boolean hasTarget, int parameterCount, OnFailure onFailure, int timeOut,
        Object... parameters) {
        throw new UnsupportedOperationException();
    }

    // HTTP
    // This function is called dynamically by Javassist (you will not find direct calls in the Java project)
    @Override
    public int executeTask(Long appId, String declareMethodFullyQualifiedName, boolean isPrioritary, int numNodes,
        boolean isReduce, int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget,
        int parameterCount, OnFailure onFailure, int timeOut, Object... parameters) {

        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.TASK);
        }

        if (numNodes != Constants.SINGLE_NODE || isReplicated || isDistributed) {
            ErrorManager.fatal("ERROR: Unsupported feature for HTTP: multi-node, replicated or distributed");
        }

        LOGGER.info(
            "Creating HTTP task for application " + appId + " and declaring class:" + declareMethodFullyQualifiedName);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("There " + (parameterCount > 1 ? "are " : "is ") + parameterCount + " parameter"
                + (parameterCount > 1 ? "s" : ""));
        }

        Application app = Application.registerApplication(appId);
        TaskMonitor monitor = app.getTaskMonitor();
        // Process the parameters
        List<Parameter> pars = processParameters(app, parameterCount, parameters, monitor);
        boolean hasReturn = hasReturn(pars);
        int numReturns = hasReturn ? 1 : 0;

        // Register the task
        int task = ap.newTask(app, monitor, declareMethodFullyQualifiedName, isPrioritary, isReduce, reduceChunkSize,
            hasTarget, numReturns, pars, onFailure, timeOut);

        for (Parameter p : pars) {
            if (p.getDirection().equals(Direction.IN_DELETE)) {
                deleteParameter(app, p);
            }
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.TASK);
        }

        return task;
    }

    /**
     * Internal execute task to make API options only as a wrapper.
     *
     * @param appId Application Id.
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
     * @param isReduce Whether it is a reduce task.
     * @param reduceChunkSize The size of the chunks to be reduced.
     * @param isReplicated Whether it is a replicated task or not.
     * @param isDistributed Whether the task must be round-robin distributed or not.
     * @param hasTarget Whether the task has a return value or not.
     * @param numReturns Number of return values of the task.
     * @param parameterCount Number of parameters of the task.
     * @param parameters Parameter values.
     * @return The task id.
     */
    public int executeTask(Long appId, Lang lang, boolean hasSignature, String methodClass, String methodName,
        String signature, OnFailure onFailure, int timeOut, boolean isPrioritary, int numNodes, boolean isReduce,
        int reduceChunkSize, boolean isReplicated, boolean isDistributed, boolean hasTarget, Integer numReturns,
        int parameterCount, Object... parameters) {
        // Tracing flag for task creation
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.TASK);
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
        TaskMonitor monitor = app.getTaskMonitor();

        // Process the parameters
        List<Parameter> pars = processParameters(app, parameterCount, parameters, monitor);

        if (numReturns == null) {
            numReturns = hasReturn(pars) ? 1 : 0;
        }

        // Create the signature if it is not created
        if (!hasSignature) {
            signature = SignatureBuilder.getMethodSignature(methodClass, methodName, hasTarget, numReturns, pars);
        }

        if (lang == null) {
            lang = DEFAULT_LANG;
        }

        int task = ap.newTask(app, monitor, lang, signature, isPrioritary, numNodes, isReduce, reduceChunkSize,
            isReplicated, isDistributed, hasTarget, numReturns, pars, onFailure, timeOut);

        for (Parameter p : pars) {
            if (p.getDirection().equals(Direction.IN_DELETE)) {
                deleteParameter(app, p);
            }
        }

        // End tracing event
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.TASK);
        }

        // Return the taskId
        return task;
    }

    @Override
    public void cancelApplicationTasks(Long appId) {
        Application app = Application.registerApplication(appId);
        ap.cancelApplicationTasks(app);
    }

    @Override
    public void openTaskGroup(String groupName, boolean implicitBarrier, Long appId) {
        Application app = Application.registerApplication(appId);
        ap.setCurrentTaskGroup(groupName, app);
    }

    @Override
    public void closeTaskGroup(String groupName, Long appId) {
        Application app = Application.registerApplication(appId);
        ap.closeCurrentTaskGroup(app);
    }

    @Override
    public void cancelTaskGroup(String groupName, Long appId) throws COMPSsException {
        Application app = Application.registerApplication(appId);
        ap.cancelTaskGroup(app, groupName);
        // This is required that changes in metadata have been applied before
        // generating new tasks
        ap.barrierGroup(app, groupName);
    }

    @Override
    public void barrierGroup(Long appId, String groupName) throws COMPSsException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.WAIT_FOR_ALL_TASKS);
        }

        Application app = Application.registerApplication(appId);
        // Regular barrier
        ap.barrierGroup(app, groupName);

        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.WAIT_FOR_ALL_TASKS);
        }
    }

    @Override
    public void barrier(Long appId) {
        barrier(appId, false);
    }

    @Override
    public void barrier(Long appId, boolean noMoreTasks) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.WAIT_FOR_ALL_TASKS);
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

        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.WAIT_FOR_ALL_TASKS);
        }
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
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.NO_MORE_TASKS);
        }

        LOGGER.info("No more tasks for app " + app.getId());
        // Wait until all tasks have finished
        ap.noMoreTasks(app);
        if (!stopped) {
            app.cancelTimerTask();
            // Retrieve result files
            LOGGER.debug("Getting Result Files for app" + app.getId());
            ap.getResultFiles(app);
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.NO_MORE_TASKS);
        }
    }

    /*
     * ************************************************************************************************************
     * ************************************** RESOURCE MANAGEMENT *************************************************
     * ************************************************************************************************************
     */
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
        if (numResources > 0) {
            ResourceManager.freeResources(numResources);
        }
    }

    /*
     * ************************************************************************************************************
     * ************************************** OTHER FUNCTIONALITIES ***********************************************
     * ************************************************************************************************************
     */
    @Override
    public void serializeObject(Object o, int hashCode, String destDir) {
        // throw new NotImplementedException();
    }

    @Override
    public void emitEvent(int type, long id) {
        Tracer.emitEvent(type, id);
    }

    @Override
    public void snapshot(Long appId) {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.SNAPSHOT_API);
        }
        Application app = Application.registerApplication(appId);
        // Wait until all tasks have finished
        LOGGER.info("Requesting snapshot for application " + appId);

        ap.snapshot(app);
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.SNAPSHOT_API);
        }
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
                ErrorManager.logError("Shutting down the running process", null);
                Runtime.getRuntime().halt(1);
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

    private int addParameter(Application app, ParameterMonitor monitor, Object content, DataType type,
        Direction direction, StdIOStream stream, String prefix, String name, String pyType, double weight,
        boolean keepRename, ArrayList<Parameter> pars, int offset, String[] vals) {
        long appId = app.getId();
        switch (type) {
            case DIRECTORY_T:
                try {
                    String dirName = content.toString();
                    File dirFile = new File(dirName);
                    String originalName = dirFile.getName();
                    DataLocation location = createLocation(ProtocolType.DIR_URI, dirName);
                    pars.add(DirectoryParameter.newDP(app, direction, stream, prefix, name, pyType, weight, keepRename,
                        location, originalName, monitor));
                    if (DP_ENABLED) {
                        // Log access to directory in the dataprovenance.log
                        String finalPath = location.toString();
                        if (finalPath.startsWith("shared")) { // Need to fix URI from SharedDisks
                            Resource host = Comm.getAppHost();
                            String absolute = dirFile.getAbsolutePath();
                            String fixedFinalPath = "dir://" + host.getName() + absolute;
                            DP_LOGGER.info(fixedFinalPath + " " + direction.toString());

                        } else {
                            DP_LOGGER.info(finalPath + " " + direction.toString());
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error(ERROR_DIR_NAME + " : " + e.getMessage());
                    ErrorManager.fatal(ERROR_DIR_NAME, e);
                }
                break;
            case FILE_T:
                try {
                    String fileName = content.toString();
                    File f = new File(fileName);
                    String originalName = f.getName();
                    DataLocation location = createLocation(ProtocolType.FILE_URI, content.toString());
                    pars.add(FileParameter.newFP(app, direction, stream, prefix, name, pyType, weight, keepRename,
                        location, originalName, monitor));
                    if (DP_ENABLED) {
                        // Log access to file in the dataprovenance.log.
                        // Corner case: PyCOMPSs objects are passed as files to the runtime
                        String finalPath = location.toString();
                        if (!finalPath.contains("tmpFiles/pycompss")) {
                            if (finalPath.startsWith("shared")) { // Need to fix URI from SharedDisks
                                Resource host = Comm.getAppHost();
                                String absolute = f.getAbsolutePath();
                                String fixedFinalPath = "file://" + host.getName() + absolute;
                                DP_LOGGER.info(fixedFinalPath + " " + direction.toString());

                            } else {
                                DP_LOGGER.info(finalPath + " " + direction.toString());
                            }
                        }
                    }
                } catch (Exception e) {
                    LOGGER.error(ERROR_FILE_NAME, e);
                    ErrorManager.fatal(ERROR_FILE_NAME, e);
                }
                break;
            case OBJECT_T:
            case PSCO_T:
                int code = oReg.newObjectParameter(appId, content);
                pars.add(ObjectParameter.newOP(app, direction, stream, prefix, name, pyType, weight, content, code,
                    monitor));
                break;
            case STREAM_T:
                int streamCode = oReg.newObjectParameter(appId, content);
                pars.add(StreamParameter.newSP(app, direction, stream, prefix, name, content, streamCode, monitor));
                break;
            case EXTERNAL_STREAM_T:
                try {
                    String fileName = content.toString();
                    DataLocation location = createLocation(ProtocolType.EXTERNAL_STREAM_URI, fileName);
                    String originalName = new File(fileName).getName();
                    pars.add(ExternalStreamParameter.newESP(app, direction, stream, prefix, name, location,
                        originalName, monitor));
                } catch (Exception e) {
                    LOGGER.error(ERROR_FILE_NAME, e);
                    ErrorManager.fatal(ERROR_FILE_NAME, e);
                }
                break;
            case EXTERNAL_PSCO_T:
                String id = content.toString();
                pars.add(ExternalPSCOParameter.newEPOP(app, direction, stream, prefix, name, weight, id,
                    externalObjectHashcode(id), monitor));
                break;
            case BINDING_OBJECT_T:
                String value = content.toString();
                if (value.contains(":")) {
                    String[] fields = value.split(":");
                    if (fields.length == 3) {
                        String extObjectId = fields[0];
                        int extObjectType = Integer.parseInt(fields[1]);
                        int extObjectElements = Integer.parseInt(fields[2]);
                        pars.add(BindingObjectParameter.newBOP(app, direction, stream, prefix, name, pyType, weight,
                            new BindingObject(extObjectId, extObjectType, extObjectElements),
                            externalObjectHashcode(extObjectId), monitor));
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
                final String collectionId = values[offset];
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
                    final DataType dataType = DataType.values()[idx];
                    contentTypes.add(dataType);
                    // Second element is the content
                    contentIds.add(values[offset + ret + 1]);
                    final DataType elemType = contentTypes.get(j);
                    final Direction elemDir = direction;
                    // Third element is the Python type of the object
                    final String elemPyType = values[offset + ret + 2];
                    // Prepare stuff for recursive call
                    final Object elemContent = elemType == DataType.COLLECTION_T ? values : contentIds.get(j);
                    // N/A to non-direct parameters
                    final StdIOStream elemStream = StdIOStream.UNSPECIFIED;
                    final String elemPrefix = Constants.PREFIX_EMPTY;
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
                    ParameterMonitor submonitor = ((ParameterCollectionMonitor) monitor).getParameterMonitor(j);
                    ret += addParameter(app, submonitor, elemContent, elemType, elemDir, elemStream, elemPrefix,
                        elemName, elemPyType, weight, keepRename, collectionParameters, offset + ret + 1, values) + 2;
                }
                CollectiveParameter cp = CollectiveParameter.newCP(app, type, collectionId, direction, stream, prefix,
                    name, colPyType, weight, keepRename, monitor, collectionParameters);
                pars.add(cp);
                return ret;
            case DICT_COLLECTION_T:
                // TODO: Simplify this case.
                // A dictionary collection value contains the file of the dictionary collection object
                // and the dictionary collection elements, separated by spaces
                String[] values1 = vals == null ? (content.toString()).split(" ") : vals;
                String dictCollectionId = values1[offset];
                int numOfEntries = Integer.parseInt(values1[offset + 1]);
                String dictColPyType = values1[offset + 2];
                // Each element is defined by TYPE VALUE PYTHON_CONTENT_TYPE. Also note the +3 offset!
                ArrayList<Parameter> dictCollectionParams = new ArrayList<>();
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
                    ParameterMonitor submonitor = ((ParameterCollectionMonitor) monitor).getParameterMonitor(j * 2);
                    int kDret = addParameter(app, submonitor, elemContentKey, dataTypeKey, elemDirKey, elemStreamKey,
                        elemPrefixKey, elemNameKey, elemPyTypeKey, weight, keepRename, dictCollectionParams,
                        offset + pointer, values1) + extraKey;
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
                    submonitor = ((ParameterCollectionMonitor) monitor).getParameterMonitor(j * 2 + 1);
                    int vDret = addParameter(app, submonitor, elemContentValue, dataTypeValue, elemDirValue,
                        elemStreamValue, elemPrefixValue, elemNameValue, elemPyTypeValue, weight, keepRename,
                        dictCollectionParams, offset + pointer, values1) + extraValue;
                    pointer += vDret;
                }
                CollectiveParameter dcp = CollectiveParameter.newCP(app, type, dictCollectionId, direction, stream,
                    prefix, name, dictColPyType, weight, keepRename, monitor, dictCollectionParams);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Add Dictionary Collection " + dcp.getName() + " with " + dcp.getElements().size() / 2
                        + " entries");
                    LOGGER.debug(dcp.toString());
                }
                pars.add(dcp);
                return pointer;
            case NULL_T:
                LOGGER.warn(WARN_NULL_PARAM + "Parameter " + name + " is defined as None or Null");
                pars.add(BasicTypeParameter.newBP(type, Direction.IN, stream, prefix, name, content, weight, "null",
                    monitor));
                break;
            default:
                // Basic types (including String)
                // The only possible direction is IN, warn otherwise
                if (direction != Direction.IN && direction != Direction.IN_DELETE) {
                    LOGGER.warn(WARN_WRONG_DIRECTION + "Parameter " + name
                        + " is a basic type, therefore it must have IN direction");
                }
                pars.add(BasicTypeParameter.newBP(type, Direction.IN, stream, prefix, name, content, weight, pyType,
                    monitor));
                break;
        }
        return 1;
    }

    private List<Parameter> processParameters(Application app, int parameterCount, Object[] parameters,
        ParameterCollectionMonitor monitors) {
        ArrayList<Parameter> pars = new ArrayList<>();
        // Parameter parsing needed, object is not serializable
        for (int paramIdx = 0; paramIdx < parameterCount; ++paramIdx) {
            int paramOffset = NUM_FIELDS_PER_PARAM * paramIdx;
            Object content = parameters[paramOffset];
            DataType type = (DataType) parameters[paramOffset + 1];
            if (type == null) {
                type = DataType.NULL_T;
            }
            Direction direction = (Direction) parameters[paramOffset + 2];
            StdIOStream stream = (StdIOStream) parameters[paramOffset + 3];
            String prefix = (String) parameters[paramOffset + 4];
            String name = (String) parameters[paramOffset + 5];
            String contentType = (String) parameters[paramOffset + 6];
            String wStr = EnvironmentLoader.loadFromEnvironment((String) parameters[paramOffset + 7]);
            double weight = Double.parseDouble(wStr);
            boolean keepRename = (Boolean) parameters[paramOffset + 8];
            // Add parameter to list
            // This function call is isolated for better readability and to easily
            // allow recursion in the case of collections
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug(" Parameter " + paramIdx + " has type " + type.name());
            }
            ParameterMonitor monitor = monitors.getParameterMonitor(paramIdx);
            addParameter(app, monitor, content, type, direction, stream, prefix, name, contentType, weight, keepRename,
                pars, 0, null);
        }

        // Return parameters
        return pars;
    }

    private void deleteParameter(Application app, Parameter p) {
        switch (p.getType()) {
            case DIRECTORY_T:
            case FILE_T:
                ap.deleteData(app, ((FileParameter<?, ?>) p).getAccess().getData(), false, false);
                // Java case where task files are stored in the registry
                if (sReg != null) {
                    sReg.deleteTaskFile(app.getId(), ((FileParameter) p).getOriginalName());
                }
                break;
            case BINDING_OBJECT_T:
                ap.deleteData(app, ((BindingObjectParameter) p).getAccess().getData(), false, false);
                break;
            case OBJECT_T:
                ObjectParameter op = (ObjectParameter) p;
                oReg.delete(app.getId(), op.getValue());
                break;
            case COLLECTION_T:
            case DICT_COLLECTION_T:
                for (Parameter sp : ((CollectiveParameter) p).getElements()) {
                    deleteParameter(app, sp);
                }
                break;
            default:
                break;
        }
    }

    private int externalObjectHashcode(String id) {
        int hashCode = 7;
        for (int i = 0; i < id.length(); ++i) {
            hashCode = hashCode * 31 + id.charAt(i);
        }

        return hashCode;
    }

    private String mainAccessToFile(FileMainAccess<?, ?> access, String fileName) {
        // Tell the AP that the application wants to access a file.
        DataLocation targetLocation;
        try {
            targetLocation = ap.mainAccess(access);
        } catch (ValueUnawareRuntimeException ex) {
            targetLocation = access.getParameters().getLocation();
        }

        // Checks on target
        String path = (targetLocation == null) ? fileName : targetLocation.getPath();
        DataLocation finalLocation = (targetLocation == null) ? access.getParameters().getLocation() : targetLocation;
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

    private DataLocation createLocation(ProtocolType defaultSchema, String fileName) throws IOException {
        // Check if fileName contains schema
        SimpleURI uri = new SimpleURI(fileName);

        // Check host
        Resource host;
        String hostName = uri.getHost();
        host = Comm.getAppHost();
        if (hostName != null && !hostName.isEmpty()) {
            Resource uriHost = ResourcesPool.getResource(hostName);
            if (uriHost == null) {
                ErrorManager.error("Host " + hostName + " not found when creating data location.");
            } else {
                host = uriHost;
                fileName = uri.getPath();
            }
        }

        if (uri.getSchema().isEmpty()) {
            if (fileName.startsWith("/")) {
                // todo: make pretty and sure it works
                uri = new SimpleURI(defaultSchema.getSchema() + fileName);
            } else {
                // Add default File scheme and wrap local paths
                String canonicalPath = new File(fileName).getCanonicalPath();
                uri = new SimpleURI(defaultSchema.getSchema() + canonicalPath);
            }
        }

        // Create location
        return DataLocation.createLocation(host, uri);
    }

    @Override
    public void setWallClockLimit(Long appId, long wcl, boolean stopRT) {
        if (wcl > 0) {

            if (timer == null) {
                if (Tracer.isActivated()) {
                    Tracer.enablePThreads(1);
                }
                timer = new Timer("Application wall clock limit timer");
                timer.schedule(new TimerTask() {

                    @Override
                    public void run() {
                        if (Tracer.isActivated()) {
                            Tracer.disablePThreads(1);
                            Tracer.emitEvent(TraceEvent.WALLCLOCK_THREAD_ID);
                        }
                    }
                }, 0);
            }

            LOGGER.info("Setting wall clock limit for app " + appId + " of " + wcl + "seconds.");
            Application app = Application.registerApplication(appId);
            WallClockTimerTask wcTask = new WallClockTimerTask(app, ap, (stopRT ? this : null));
            app.setTimerTask(wcTask);
            // One second is added to allow possible stop from the binding
            timer.schedule(wcTask, (wcl + 1) * 1000);
        }
    }

}
