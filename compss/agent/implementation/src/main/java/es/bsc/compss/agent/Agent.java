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
package es.bsc.compss.agent;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.COMPSsPaths;
import es.bsc.compss.agent.types.ApplicationParameter;
import es.bsc.compss.agent.types.ApplicationParameterCollection;
import es.bsc.compss.agent.types.PrivateRemoteDataLocation;
import es.bsc.compss.agent.types.RemoteDataInformation;
import es.bsc.compss.agent.types.RemoteDataLocation;
import es.bsc.compss.agent.types.Resource;
import es.bsc.compss.agent.types.SharedRemoteDataLocation;
import es.bsc.compss.api.impl.COMPSsRuntimeImpl;
import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.CommException;
import es.bsc.compss.exceptions.ConstructConfigurationException;
import es.bsc.compss.loader.total.ObjectRegistry;
import es.bsc.compss.loader.total.StreamRegistry;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.CoreElementDefinition;
import es.bsc.compss.types.ErrorHandler;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;
import es.bsc.compss.util.RuntimeConfigManager;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import storage.StorageException;
import storage.StorageItf;


public class Agent {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.AGENT);

    private static final String AGENT_NAME;

    private static final COMPSsRuntimeImpl RUNTIME;

    private static final List<AgentInterface<?>> INTERFACES;

    private static final int PARAM_LENGTH = COMPSsRuntimeImpl.NUM_FIELDS_PER_PARAM;

    private static final String WARN_IT_FILE_NOT_READ = "WARNING: COMPSs Properties file could not be read";

    private static final String WARN_FILE_EMPTY_DEFAULT =
        "WARNING: COMPSs Properties file is null." + " Setting default values";

    static {
        AGENT_NAME = COMPSsNode.getMasterName();
        setAgentProperties();
        LoggerManager.init();
        LOGGER.info("Initializing agent with name: " + AGENT_NAME);

        // Start tracing system
        boolean tracing = System.getProperty(COMPSsConstants.TRACING) != null
            && Boolean.parseBoolean(System.getProperty(COMPSsConstants.TRACING));
        boolean tracingTaskDep = Boolean.parseBoolean(System.getProperty(COMPSsConstants.TRACING_TASK_DEPENDENCIES));
        String installDir = System.getenv(COMPSsConstants.COMPSS_HOME);
        Tracer.init(tracing, 0, "master", installDir, tracingTaskDep);
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.STATIC_IT);
        }

        String dcConfigPath = System.getProperty(AgentConstants.DATACLAY_CONFIG_PATH);
        LOGGER.debug("DataClay configuration: " + dcConfigPath);
        if (dcConfigPath != null) {
            try {
                StorageItf.init(dcConfigPath);
            } catch (StorageException se) {
                se.printStackTrace(System.err);
                System.err.println("Continuing...");
            }
            Runtime.getRuntime().addShutdownHook(new Thread() {

                public void run() {
                    try {
                        StorageItf.finish();
                    } catch (StorageException se) {
                        se.printStackTrace(System.err);
                        System.err.println("Continuing...");
                    }
                }
            });
        }
        RUNTIME = new COMPSsRuntimeImpl();

        LOGGER.debug("Executing Agent");
        try {
            Thread.sleep(200);
        } catch (Exception e) {
            LOGGER.debug("");
        }

        ErrorHandler feh = new ErrorHandler() {

            @Override
            public boolean handleError() {
                LOGGER.info("Error raised. Please, check runtime.log");
                return false;
            }

            @Override
            public boolean handleFatalError() {
                LOGGER.info("Fatal error for an application raised. Please, check runtime.log");
                return false;
            }

        };
        ErrorManager.init(feh);
        RUNTIME.setObjectRegistry(new ObjectRegistry(RUNTIME));
        RUNTIME.setStreamRegistry(new StreamRegistry(RUNTIME));

        INTERFACES = new LinkedList<>();

    }


    private static void setAgentProperties() {
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
        setDefaultProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, COMPSsDefaults.CUSTOM_EXTRAE_FILE);
        setDefaultProperty(COMPSsConstants.TASK_EXECUTION, COMPSsConstants.TaskExecution.COMPSS.toString());
    }

    private static void setDefaultProperty(String propertyName, String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null || propertyValue.isEmpty()) {
            System.setProperty(propertyName, defaultValue);
        }
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

    /**
     * Start the runtime within the Agent and sets it up to allow the execution of COMPSs methods.
     */
    public static void start() {
        RUNTIME.startIT();
    }

    /**
     * Stops the runtime within the Agent.
     */
    public static void stop() {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.AGENT_STOP);
        }
        RUNTIME.stopIT(true);
        Iterator<AgentInterface<?>> itfs = INTERFACES.iterator();
        while (itfs.hasNext()) {
            AgentInterface<?> itf = itfs.next();
            itf.stop();
            itfs.remove();
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.AGENT_STOP);
        }
    }

    /**
     * Requests the execution of a method as a task.
     *
     * @param lang programming language of the method
     * @param ced Definition of the Core Element to execute
     * @param ceiClass Core Element interface to detect nested tasks in the code. If null, no nested parallelism will be
     *            detected
     * @param arguments parameter description of the task's arguments
     * @param target paramter description of the task's callee
     * @param results paramter description of the task's results
     * @param monitor monitor to notify changes on the method execution
     * @param onFailure behaviour in case of task execution failure
     * @return Identifier of the application associated to the task
     * @throws AgentException could not retrieve the value of some parameter
     */
    public static long runTask(Lang lang, CoreElementDefinition ced, String ceiClass, ApplicationParameter[] arguments,
        ApplicationParameter target, ApplicationParameter[] results, AppMonitor monitor, OnFailure onFailure)
        throws AgentException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.AGENT_RUN_TASK);
        }

        synchronized (RUNTIME) {
            // Making sure that the runtime has already been started
        }
        Long appId = RUNTIME.registerApplication(ceiClass, monitor);
        monitor.setAppId(appId);
        LOGGER.debug("New request to run as a " + lang + " task " + ced.getCeSignature());
        LOGGER.debug("appId: " + appId);
        LOGGER.debug("Core Element Description: " + ced.toString());
        LOGGER.debug("Parallelizing application according to " + ceiClass);
        LOGGER.debug("Parameters: ");
        for (ApplicationParameter param : arguments) {
            LOGGER.debug("\t* " + param);
        }

        try {
            // PREPARING PARAMETERS
            int paramsCount = arguments.length;
            if (target != null) {
                paramsCount++;
            }
            paramsCount += results.length;

            Object[] params = new Object[PARAM_LENGTH * paramsCount];
            int position = 0;
            LOGGER.debug("Handles parameters:");
            for (ApplicationParameter param : arguments) {
                LOGGER.debug("\t Parameter:" + param.getParamName());
                processParameter(appId, param, position, params);
                position += PARAM_LENGTH;
            }

            if (target != null) {
                LOGGER.debug("\t Target:" + target.getParamName());
                processParameter(appId, target, position, params);
                position += PARAM_LENGTH;
            }

            for (ApplicationParameter param : results) {
                Object value;
                if (DataType.FILE_T.equals(param.getType())) {
                    value = UUID.randomUUID().toString();
                } else {
                    value = new Object();
                }
                addTaskParameter(value, param, position, params);
                position += PARAM_LENGTH;
            }

            onFailure = OnFailure.FAIL;
            RUNTIME.registerCoreElement(ced);
            int numNodes = 1;
            RUNTIME.executeTask(appId, // APP ID
                lang, true, null, null, ced.getCeSignature(), // Method to call
                onFailure, // On failure behavior
                0, // Time out of the task
                false, // isPriority
                numNodes, // Number of nodes
                false, 0, // Reduce information
                false, false, // Scheduling information (isDistributed, isReplicated)
                target != null, results.length, paramsCount, // Parameter information
                params // Parameter values
            );

        } catch (Exception e) {
            LOGGER.error("Error submitting task", e);
            throw new AgentException(e);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TraceEvent.AGENT_RUN_TASK);
            }
        }
        return appId;
    }

    private static String processCollParamValue(ApplicationParameterCollection<ApplicationParameter> param, Long appId,
        String colName) throws Exception {

        int collSize = param.getCollectionParameters().size();
        StringBuilder sb = new StringBuilder();
        sb.append(colName).append(" ");
        sb.append(collSize).append(" ");
        sb.append(param.getContentType()).append(" ");
        List<ApplicationParameter> subParams = param.getCollectionParameters();
        for (int i = 0; i < subParams.size(); i++) {
            ApplicationParameter subParam = subParams.get(i);
            sb.append(subParam.getType().ordinal()).append(" ");
            String paramValue;
            if (subParam.getType() == DataType.COLLECTION_T) {
                String subParamName = colName + "_" + i;
                @SuppressWarnings("unchecked")
                ApplicationParameterCollection<ApplicationParameter> collSubParam =
                    (ApplicationParameterCollection<ApplicationParameter>) (subParam);
                paramValue = processCollParamValue(collSubParam, appId, subParamName);
            } else {
                paramValue = subParam.getValueContent().toString() + " " + subParam.getContentType();
            }
            sb.append(paramValue).append(" ");

            RemoteDataInformation remote = subParam.getRemoteData();
            if (remote != null) {
                Object stub;
                if (subParam.getType() == DataType.FILE_T) {
                    stub = subParam.getValueContent();
                } else {
                    stub = paramValue;
                }
                addRemoteData(remote);
                RUNTIME.registerData(appId, subParam.getType(), stub, remote.getRenaming());
            }
        }
        return sb.toString();
    }

    private static Object processParamValue(Long appId, int position, ApplicationParameter param) throws Exception {
        RemoteDataInformation remote = param.getRemoteData();
        Object stub;
        if (remote == null && param.getType() != DataType.COLLECTION_T) {
            LOGGER.debug("\t\tUsing value passed in as parameter");
            return param.getValueContent();
        } else {
            if (param.getType() == DataType.FILE_T) {
                stub = param.getValueContent();
            } else {
                stub = "app_" + appId + "_param" + position;
                if (param.getType() == DataType.COLLECTION_T) {
                    @SuppressWarnings("unchecked")
                    ApplicationParameterCollection<ApplicationParameter> collSubParam =
                        (ApplicationParameterCollection<ApplicationParameter>) (param);
                    stub = processCollParamValue(collSubParam, appId, (String) stub);
                }
            }

            if (remote != null) {
                addRemoteData(remote);
                RUNTIME.registerData(appId, param.getType(), stub, remote.getRenaming());
            }
        }
        return stub;
    }

    private static void processParameter(Long appId, ApplicationParameter param, int position, Object[] arguments)
        throws AgentException, Exception {

        Object value = processParamValue(appId, position, param);
        addTaskParameter(value, param, position, arguments);
    }

    private static void addTaskParameter(Object value, ApplicationParameter param, int position, Object[] arguments)
        throws AgentException, Exception {

        arguments[position] = value;
        arguments[position + 1] = param.getType();
        arguments[position + 2] = param.getDirection();
        arguments[position + 3] = param.getStdIOStream();
        arguments[position + 4] = param.getPrefix();
        arguments[position + 5] = param.getParamName();
        arguments[position + 6] = param.getContentType();
        arguments[position + 7] = Double.toString(param.getWeight());
        arguments[position + 8] = param.isKeepRename();
    }

    /**
     * Returns or creates the host from a remote Resource description.
     *
     * @param r remote resource description
     * @return Internal Resource corresponding to the resource description passed in as parameter
     * @throws AgentException Error while registering the remote node in the Agent
     */
    public static es.bsc.compss.types.resources.Resource getNodeForResource(Resource<?, ?> r) throws AgentException {
        if (r == null) {
            return null;
        }
        String workerName = r.getName();
        Worker<? extends WorkerResourceDescription> host = ResourceManager.getWorker(workerName);
        if (host == null) {
            MethodResourceDescription mrd = r.getDescription();
            String adaptor = r.getAdaptor();
            Map<String, Object> projectConf = new HashMap<>();
            projectConf.put("Properties", r.getProjectConf());
            Map<String, Object> resourcesConf = new HashMap<>();
            resourcesConf.put("Properties", r.getResourceConf());
            host = registerWorker(workerName, mrd, adaptor, projectConf, resourcesConf);
        }
        return host;

    }

    private static void addRemoteData(RemoteDataInformation remote) throws AgentException {
        int addedSources = 0;
        LogicalData ld = Comm.getData(remote.getRenaming());
        String otherDataNameInLocal = null;

        LinkedList<DataLocation> locations = new LinkedList<>();
        for (RemoteDataLocation loc : remote.getSources()) {
            if (loc != null) {
                try {
                    DataLocation dl = null;
                    if (loc.getType() == RemoteDataLocation.Type.SHARED) {
                        SharedRemoteDataLocation sloc = (SharedRemoteDataLocation) loc;
                        String diskName = sloc.getDiskName();
                        String pathOnDisk = sloc.getPathOnDisk();
                        for (SharedRemoteDataLocation.Mountpoint mp : sloc.getMountpoints()) {
                            Resource<?, ?> r = mp.getResource();
                            es.bsc.compss.types.resources.Resource host = getNodeForResource(r);
                            host.addSharedDisk(diskName, mp.getPath());
                        }
                        String sPath = ProtocolType.SHARED_URI.getSchema() + diskName + File.separator + pathOnDisk;
                        SimpleURI resultURI = new SimpleURI(sPath);
                        dl = DataLocation.createLocation(null, resultURI);
                    } else {
                        PrivateRemoteDataLocation ploc = (PrivateRemoteDataLocation) loc;
                        String path = ploc.getPath();
                        SimpleURI uri = new SimpleURI(path);
                        Resource<?, ?> r = ploc.getResource();
                        if (r != null) {
                            es.bsc.compss.types.resources.Resource host = getNodeForResource(r);
                            if (host == Comm.getAppHost()) {
                                String name = uri.getPath();
                                LogicalData localData = Comm.getData(name);
                                if (localData != null) {
                                    otherDataNameInLocal = name;
                                    addedSources++;
                                    continue;
                                }
                            }
                            dl = DataLocation.createLocation(host, uri);
                        }

                    }
                    if (dl != null) {
                        locations.add(dl);
                    }
                } catch (AgentException | IOException e) {
                    // Do nothing. Ignore location
                    LOGGER.warn("Exception adding remote data", e);
                }
            }
        }

        if (ld == null) {
            if (otherDataNameInLocal == null) {
                ld = Comm.registerData(remote.getRenaming());
            } else {
                try {
                    ld = Comm.linkData(otherDataNameInLocal, remote.getRenaming());
                } catch (CommException ce) {
                    ErrorManager.error("Could not link " + remote.getRenaming() + " and " + otherDataNameInLocal, ce);
                }
                addedSources++;
            }
        }
        for (DataLocation loc : locations) {
            ld.addLocation(loc);
            addedSources++;
        }
        if (addedSources == 0) {
            throw new AgentException("Could not add any source for data " + remote.getRenaming());
        }
    }

    /**
     * Adds new resources into the resource pool.
     *
     * @param r Description of the resources to add into the resource pool
     * @throws AgentException could not create a configuration to start using this resource
     */
    public static void addResources(Resource<?, ?> r) throws AgentException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.AGENT_ADD_RESOURCE);
        }
        String workerName = r.getName();
        MethodResourceDescription description = r.getDescription();

        DynamicMethodWorker worker = ResourceManager.getDynamicResource(workerName);
        if (worker != null) {
            ResourceManager.increasedDynamicWorker(worker, description);
        } else {
            String adaptor = r.getAdaptor();
            Map<String, Object> projectConf = new HashMap<>();
            projectConf.put("Properties", r.getProjectConf());
            Map<String, Object> resourcesConf = new HashMap<>();
            resourcesConf.put("Properties", r.getResourceConf());
            registerWorker(workerName, description, adaptor, projectConf, resourcesConf);
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.AGENT_ADD_RESOURCE);
        }
    }

    private static DynamicMethodWorker registerWorker(String workerName, MethodResourceDescription description,
        String adaptor, Map<String, Object> projectConf, Map<String, Object> resourcesConf) throws AgentException {
        if (description == null) {
            description = new MethodResourceDescription();
        }

        MethodConfiguration mc;
        try {
            mc = (MethodConfiguration) Comm.constructConfiguration(adaptor, projectConf, resourcesConf);
        } catch (ConstructConfigurationException e) {
            throw new AgentException(e.getMessage(), e);
        }
        int limitOfTasks = mc.getLimitOfTasks();
        int computingUnits = description.getTotalCPUComputingUnits();
        if (limitOfTasks < 0 && computingUnits < 0) {
            mc.setLimitOfTasks(0);
            mc.setTotalComputingUnits(0);
        } else {
            mc.setLimitOfTasks(Math.max(limitOfTasks, computingUnits));
            mc.setTotalComputingUnits(Math.max(limitOfTasks, computingUnits));
        }
        mc.setLimitOfGPUTasks(description.getTotalGPUComputingUnits());
        mc.setTotalGPUComputingUnits(description.getTotalGPUComputingUnits());
        mc.setLimitOfFPGATasks(description.getTotalFPGAComputingUnits());
        mc.setTotalFPGAComputingUnits(description.getTotalFPGAComputingUnits());
        mc.setLimitOfOTHERsTasks(description.getTotalOTHERComputingUnits());
        mc.setTotalOTHERComputingUnits(description.getTotalOTHERComputingUnits());

        mc.setHost(workerName);
        DynamicMethodWorker worker;
        worker = new DynamicMethodWorker(workerName, description, mc, new HashMap<>());
        ResourceManager.addDynamicWorker(worker, description);
        return worker;
    }

    /**
     * Requests the agent to stop using some resources from a node.
     *
     * @param workerName name of the worker to whom the resources belong.
     * @param reduction description of the resources to stop using.
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void removeResources(String workerName, MethodResourceDescription reduction) throws AgentException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.AGENT_REMOVE_RESOURCES);
        }
        DynamicMethodWorker worker = ResourceManager.getDynamicResource(workerName);
        if (worker != null) {
            ResourceManager.requestWorkerReduction(worker, reduction);
        } else {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TraceEvent.AGENT_REMOVE_RESOURCES);
        }
    }

    /**
     * Request the agent to stop using all the resources from a node.
     *
     * @param workerName name of the worker to stop using
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void removeNode(String workerName) throws AgentException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.AGENT_REMOVE_RESOURCES);
        }
        try {
            ResourceManager.requestWholeWorkerReduction(workerName);
        } catch (NullPointerException e) {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TraceEvent.AGENT_REMOVE_RESOURCES);
            }
        }
    }

    /**
     * Forces the agent to remove a node with which it has lost the connection.
     *
     * @param workerName name of the worker to stop using
     * @throws AgentException the worker was not set up for the agent.
     */
    public static void lostNode(String workerName) throws AgentException {
        try {
            ResourceManager.notifyWholeWorkerReduction(workerName);
        } catch (NullPointerException e) {
            throw new AgentException("Resource " + workerName + " was not set up for this agent. Ignoring request.");
        }
    }

    /**
     * Starts an agent interface.
     *
     * @param conf Agent Interface configuration parameters
     * @throws ClassNotFoundException Could not find the specify agent interface class
     * @throws InstantiationException Could not instantiate the agent interface
     * @throws IllegalAccessException Could not call the empty constructor because is private
     * @throws AgentException Error during the interface boot process
     */
    @SuppressWarnings("unchecked")
    public static final void startInterface(AgentInterfaceConfig conf)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, AgentException {

        AgentInterface<AgentInterfaceConfig> itf = (AgentInterface<AgentInterfaceConfig>) conf.getAgentInterface();
        itf.start(conf);
        INTERFACES.add(itf);
    }

    private static AgentInterfaceConfig getInterfaceConfig(String className, JSONObject arguments)
        throws ClassNotFoundException, InstantiationException, IllegalAccessException, AgentException {

        Class<?> agentClass = Class.forName(className);
        AgentInterface<?> itf = (AgentInterface<?>) agentClass.newInstance();
        return itf.configure(arguments);
    }

    /**
     * Main method to start a COMPSs agent. (Currently it only allows a REST agent)
     *
     * @param args Set of JSONObjects describing the AgentInterfaces to start and their configurations.
     * @throws Exception Could not create the configuration for the REST agent due to internal errors
     */
    public static final void main(String[] args) throws Exception {
        LinkedList<AgentInterfaceConfig> agents = new LinkedList<>();
        String agentConfig = System.getProperty(AgentConstants.AGENT_CONFIG_PATH);

        if (agentConfig != null && !agentConfig.isEmpty()) {
            LOGGER.info("Reading Agent config from " + agentConfig);
            File configFile = new File(agentConfig);
            if (configFile.exists()) {
                String configString = new String(Files.readAllBytes(configFile.toPath()));
                JSONArray array = new JSONArray(configString);
                for (int i = 0; i < array.length(); i++) {
                    JSONObject jo = array.getJSONObject(i);
                    try {
                        String interfaceClass = jo.getString("AGENT_IMPL");
                        JSONObject conf = jo.getJSONObject("CONF");
                        LOGGER.info("Loading " + interfaceClass + "'s agent interface");
                        AgentInterfaceConfig aic = getInterfaceConfig(interfaceClass, conf);
                        agents.add(aic);
                    } catch (Exception e) {
                        ErrorManager.warn("Unexpected format for agent config: " + jo);
                    }
                }
            } else {
                ErrorManager.warn("Could not find the agent configuration file " + agentConfig);
            }
        }
        synchronized (RUNTIME) {
            for (String arg : args) {
                try {
                    JSONObject jo = new JSONObject(arg);
                    String interfaceClass = jo.getString("AGENT_IMPL");
                    JSONObject conf = jo.getJSONObject("CONF");
                    LOGGER.info("Loading " + agentConfig + "'s agent interface");
                    AgentInterfaceConfig aic = getInterfaceConfig(interfaceClass, conf);
                    agents.add(aic);
                } catch (Exception e) {
                    ErrorManager.warn("Unexpected format for agent config: " + arg);
                }
            }

            for (AgentInterfaceConfig agent : agents) {
                try {
                    startInterface(agent);
                } catch (Exception e) {
                    ErrorManager.warn("Could not start Agent", e);
                }
            }
            if (INTERFACES.isEmpty()) {
                ErrorManager.fatal("Could not start any interface");
            }
            start();
        }
    }

    /**
     * Handles the notification of the end of an application.
     *
     * @param appId Identifier of the finished application
     */
    public static void finishedApplication(long appId) {
        synchronized (RUNTIME) {
            // Making sure that the runtime has already been started
        }
        // Remove all data bound to the application and remove app
        RUNTIME.deregisterApplication(appId);
    }
}
