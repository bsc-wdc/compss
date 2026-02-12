/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.COMPSsPaths;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.BuilderParameters;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;


/**
 * Loads the runtime configuration.
 */
public class RuntimeConfigManager {

    private static final String WARN_IT_FILE_NOT_READ = "WARNING: COMPSs Properties file could not be read";
    private static final String WARN_FILE_EMPTY_DEFAULT =
        "WARNING: COMPSs Properties file is null." + " Setting default values";

    private final PropertiesConfiguration config;


    /**
     * Sets up all the properties of the JVM that will be required during the runtime execution.
     */
    public static void setProperties() {

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

    private static InputStream findPropertiesConfigFile() {
        final Class<?> clazz = RuntimeConfigManager.class;
        // Try to get as resource from class
        InputStream stream = clazz.getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
        if (stream == null) {
            stream = clazz.getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
        }

        // From the loader that loaded this class
        ClassLoader clazzLoader = null;
        if (stream == null) {
            clazzLoader = clazz.getClassLoader();
            stream = clazzLoader.getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
            if (stream == null) {
                stream = clazzLoader.getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
            }
        }

        // IT properties file not found. Looking at parent ClassLoader
        ClassLoader clazzLoaderParent = null;
        if (stream == null) {
            clazzLoaderParent = clazzLoader.getParent();
            stream = clazzLoaderParent.getResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
            if (stream == null) {
                stream = clazzLoaderParent.getResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
            }
        }

        // IT properties file not found in classloader. Looking at system resources
        if (stream == null) {
            stream = ClassLoader.getSystemResourceAsStream(COMPSsConstants.COMPSS_CONFIG);
            if (stream == null) {
                stream = ClassLoader.getSystemResourceAsStream(File.separator + COMPSsConstants.COMPSS_CONFIG);
            }
        }

        return stream;
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

                if (System.getProperty(COMPSsConstants.SOCKET_MODE) == null
                    || System.getProperty(COMPSsConstants.SOCKET_MODE).isEmpty()) {
                    setDefaultProperty(COMPSsConstants.SOCKET_MODE, COMPSsDefaults.SOCKET_MODE);
                } else {
                    System.setProperty(COMPSsConstants.SOCKET_MODE, manager.getSocketMode());
                }
                if (System.getProperty(COMPSsConstants.SOCKET_PATH) == null
                    || System.getProperty(COMPSsConstants.SOCKET_PATH).isEmpty()) {
                    setDefaultProperty(COMPSsConstants.SOCKET_PATH, COMPSsDefaults.SOCKET_PATH);
                } else {
                    System.setProperty(COMPSsConstants.SOCKET_PATH, manager.getSocketPath());
                }
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
        setDefaultProperty(COMPSsConstants.SOCKET_MODE, COMPSsDefaults.SOCKET_MODE);
        setDefaultProperty(COMPSsConstants.SOCKET_PATH, COMPSsDefaults.SOCKET_PATH);
    }

    private static void setDefaultProperty(String propertyName, String defaultValue) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null || propertyValue.isEmpty()) {
            System.setProperty(propertyName, defaultValue);
        }
    }

    /**
     * Loads the runtime configuration found in path {@code pathToConfigFile}.
     *
     * @param pathToConfigFile Path to configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    private RuntimeConfigManager(String pathToConfigFile) throws ConfigurationException {
        this(new Parameters().properties().setFileName(pathToConfigFile));
    }

    /**
     * Loads the runtime configuration from an input stream {@code stream}.
     *
     * @param stream Stream to configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    private RuntimeConfigManager(InputStream stream) throws ConfigurationException {
        this(new Parameters().properties());
        FileHandler handler = new FileHandler(this.config);
        handler.load(stream);
    }

    /**
     * Loads the runtime configuration found in URL {@code pathToConfigFile}.
     *
     * @param pathToConfigFile URL path to configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    private RuntimeConfigManager(URL pathToConfigFile) throws ConfigurationException {
        this(new Parameters().properties().setURL(pathToConfigFile));
    }

    /**
     * Loads the runtime configuration from a file {@code file}.
     *
     * @param file File object pointing to the configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    private RuntimeConfigManager(File file) throws ConfigurationException {
        this(new Parameters().properties().setFile(file));
    }

    private RuntimeConfigManager(BuilderParameters... params) throws ConfigurationException {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
            new FileBasedConfigurationBuilder<>(PropertiesConfiguration.class).configure(params);
        this.config = builder.getConfiguration();
    }

    /**
     * Returns the deployment id.
     *
     * @return The deployment id.
     */
    public String getDeploymentId() {
        return config.getString(COMPSsConstants.DEPLOYMENT_ID, COMPSsDefaults.DEPLOYMENT_ID);
    }

    /**
     * Sets a new value for the deployment id.
     *
     * @param uuid New deployment id value.
     */
    public void setDeploymentId(String uuid) {
        config.setProperty(COMPSsConstants.DEPLOYMENT_ID, uuid);
    }

    /**
     * Returns the master node name.
     *
     * @return The master node name.
     */
    public String getMasterName() {
        return config.getString(COMPSsConstants.MASTER_NAME);
    }

    /**
     * Sets a new value for the master node name.
     *
     * @param name New value for the master node name.
     */
    public void setMasterName(String name) {
        config.setProperty(COMPSsConstants.MASTER_NAME, name);
    }

    /**
     * Returns the master node port.
     *
     * @return The master node port.
     */
    public String getMasterPort() {
        return config.getString(COMPSsConstants.MASTER_PORT);
    }

    /**
     * Sets a new value for the master node port.
     *
     * @param port New value for the master node port.
     */
    public void setMasterPort(String port) {
        config.setProperty(COMPSsConstants.MASTER_PORT, port);
    }

    /**
     * Returns the application name.
     *
     * @return The application name.
     */
    public String getAppName() {
        return config.getString(COMPSsConstants.APP_NAME);
    }

    /**
     * Returns the socket mode.
     * 
     * @return The socket mode.
     */
    public String getSocketMode() {
        return config.getString(COMPSsConstants.SOCKET_MODE);
    }

    /**
     * Returns the socket path.
     * 
     * @return The socket path.
     */
    public String getSocketPath() {
        return config.getString(COMPSsConstants.SOCKET_PATH);
    }

    /**
     * Sets a new name for the application.
     *
     * @param name New application name.
     */
    public void setAppName(String name) {
        config.setProperty(COMPSsConstants.APP_NAME, name);
    }

    /**
     * Returns the task summary flag value.
     *
     * @return The task summary flag value.
     */
    public String getTaskSummary() {
        return config.getString(COMPSsConstants.TASK_SUMMARY);
    }

    /**
     * Sets a new value for the task summary flag.
     *
     * @param value New value for the task summary flag.
     */
    public void setTaskSummary(String value) {
        config.setProperty(COMPSsConstants.TASK_SUMMARY, value);
    }

    /**
     * Returns the log directory.
     *
     * @return The log directory.
     */
    public String getLogDir() {
        return config.getString(COMPSsConstants.LOG_DIR);
    }

    /**
     * Returns the working directory used by the master process.
     *
     * @return The working directory for the master process.
     */
    public String getWorkingDir() {
        return config.getString(COMPSsConstants.WORKING_DIR);
    }

    /**
     * Returns the project file.
     *
     * @return The project file.
     */
    public String getProjectFile() {
        return config.getString(COMPSsConstants.PROJ_FILE);
    }

    /**
     * Sets a new value for the project file.
     *
     * @param location New value for the project file.
     */
    public void setProjectFile(String location) {
        config.setProperty(COMPSsConstants.PROJ_FILE, location);
    }

    /**
     * Returns the project schema.
     *
     * @return The project schema.
     */
    public String getProjectSchema() {
        return config.getString(COMPSsConstants.PROJ_SCHEMA);
    }

    /**
     * Sets a new value for the project schema.
     *
     * @param location New value for the project schema.
     */
    public void setProjectSchema(String location) {
        config.setProperty(COMPSsConstants.PROJ_SCHEMA, location);
    }

    /**
     * Returns the resources file.
     *
     * @return The resources file.
     */
    public String getResourcesFile() {
        return config.getString(COMPSsConstants.RES_FILE);
    }

    /**
     * Sets a new value for the resources file.
     *
     * @param location New value for the resources file.
     */
    public void setResourcesFile(String location) {
        config.setProperty(COMPSsConstants.RES_FILE, location);
    }

    /**
     * Returns the resources schema.
     *
     * @return The resources schema.
     */
    public String getResourcesSchema() {
        return config.getString(COMPSsConstants.RES_SCHEMA);
    }

    /**
     * Sets a new location for the resources schema.
     *
     * @param location New location for the resources schema.
     */
    public void setResourcesSchema(String location) {
        config.setProperty(COMPSsConstants.RES_SCHEMA, location);
    }

    /**
     * Returns the scheduler to load.
     *
     * @return The scheduler to load.
     */
    public String getScheduler() {
        return config.getString(COMPSsConstants.SCHEDULER);
    }

    /**
     * Sets a new implementing class for the scheduler.
     *
     * @param implementingClass New implementing class for the scheduler.
     */
    public void setScheduler(String implementingClass) {
        config.setProperty(COMPSsConstants.SCHEDULER, implementingClass);
    }

    /**
     * Returns the log4j configuration file location.
     *
     * @return The log4j configuration file location.
     */
    public String getLog4jConfiguration() {
        return config.getString(COMPSsConstants.LOG4J);
    }

    /**
     * Sets a new location for the log4j configuration file.
     *
     * @param location New location for the log4j configuration file.
     */
    public void setLog4jConfiguration(String location) {
        config.setProperty(COMPSsConstants.LOG4J, location);
    }

    /**
     * Returns the COMM Adaptor implementing class.
     *
     * @return The COMM Adaptor implementing class.
     */
    public String getCommAdaptor() {
        return config.getString(COMPSsConstants.COMM_ADAPTOR);
    }

    /**
     * Sets a new implementing class for the COMM adaptor.
     *
     * @param adaptor New implementing class for the COMM adaptor.
     */
    public void setCommAdaptor(String adaptor) {
        config.setProperty(COMPSsConstants.COMM_ADAPTOR, adaptor);
    }

    /**
     * Returns the CONN implementing class.
     *
     * @return The CONN implementing class.
     */
    public String getConn() {
        return config.getString(COMPSsConstants.CONN);
    }

    /**
     * Sets a new implementing class for the CONN.
     *
     * @param connector New implementing class for the CONN.
     */
    public void setConn(String connector) {
        config.setProperty(COMPSsConstants.CONN, connector);
    }

    /**
     * Returns the GAT context.
     *
     * @return The GAT context.
     */
    public String getContext() {
        return config.getString(COMPSsConstants.COMPSS_CONTEXT);
    }

    /**
     * Sets a new GAT context.
     *
     * @param context New GAT context.
     */
    public void setContext(String context) {
        config.setProperty(COMPSsConstants.COMPSS_CONTEXT, context);
    }

    /**
     * Returns the specific GAT adaptor path.
     *
     * @return The specific GAT adaptor path.
     */
    public String getGATAdaptor() {
        return config.getString(COMPSsConstants.GAT_ADAPTOR_PATH, COMPSsPaths.GAT_ADAPTOR_LOCATION);
    }

    /**
     * Sets a new location for the GAT Adaptor.
     *
     * @param adaptorPath New location for the GAT Adaptor.
     */
    public void setGATAdaptor(String adaptorPath) {
        config.setProperty(COMPSsConstants.GAT_ADAPTOR_PATH, adaptorPath);
    }

    /**
     * Returns if the GAT Adaptor is in debug mode or not.
     *
     * @return {@code true} if the GAT Adaptor is in debug mode, {@code false} otherwise.
     */
    public boolean isGATDebug() {
        return config.getBoolean(COMPSsConstants.GAT_DEBUG, false);
    }

    /**
     * Sets a new debug mode for the GAT Adaptor.
     *
     * @param debug New debug mode for the GAT Adaptor.
     */
    public void setGATDebug(boolean debug) {
        config.setProperty(COMPSsConstants.GAT_DEBUG, debug);
    }

    /**
     * Returns the GAT Broker Adaptor class.
     *
     * @return The GAT Broker Adaptor class.
     */
    public String getGATBrokerAdaptor() {
        return config.getString(COMPSsConstants.GAT_BROKER_ADAPTOR);
    }

    /**
     * Sets a new GAT Broker Adaptor class.
     *
     * @param adaptor New GAT Broker Adaptor class.
     */
    public void setGATBrokerAdaptor(String adaptor) {
        config.setProperty(COMPSsConstants.GAT_BROKER_ADAPTOR, adaptor);
    }

    /**
     * Returns the GAT File Adaptor.
     *
     * @return The GAT File Adaptor.
     */
    public String getGATFileAdaptor() {
        return config.getString(COMPSsConstants.GAT_FILE_ADAPTOR);
    }

    /**
     * Sets a new GAT File Adaptor class.
     *
     * @param adaptor New GAT File Adaptor class.
     */
    public void setGATFileAdaptor(String adaptor) {
        config.setProperty(COMPSsConstants.GAT_FILE_ADAPTOR, adaptor);
    }

    /**
     * Returns whether the resources assigned to an execution should be reused when it stalls.
     *
     * @return {@literal true} if the resources assigned to an execution should be reused when it stalls;
     *         {@literal false}, otherwise.
     */
    public boolean getReuseResourcesOnBlock() {
        return config.getBoolean(COMPSsConstants.REUSE_RESOURCES_ON_BLOCK);
    }

    /**
     * Sets whether the resources assigned to an execution should be reused when it stalls.
     *
     * @param reuse {@literal true} if the resources assigned to an execution should be reused when it stalls;
     *            {@literal false}, otherwise.
     */
    public void setReuseResourcesOnBlock(boolean reuse) {
        config.setProperty(COMPSsConstants.REUSE_RESOURCES_ON_BLOCK, reuse);
    }

    /**
     * Returns whether the detection of nested tasks is enabled during the execution of a tasks within the local
     * resources.
     *
     * @return {@literal true} if the detection of nested tasks is enabled during the execution of a tasks within the
     *         local resources is enabled; {@literal false}, otherwise.
     */
    public boolean isNestedDetectionEnabled() {
        return config.getBoolean(COMPSsConstants.ENABLED_NESTED_TASKS_DETECTION);
    }

    /**
     * Sets whether the detection of nested tasks is enabled during the execution of a tasks within the local resources.
     *
     * @param enable {@literal true} if the detection of nested tasks is enabled during the execution of a tasks within
     *            the local resources is enabled; {@literal false}, otherwise.
     */
    public void setNestedDetectionEnabled(boolean enable) {
        config.setProperty(COMPSsConstants.ENABLED_NESTED_TASKS_DETECTION, enable);
    }

    /**
     * Sets the new CPU Affinity for the Workers.
     *
     * @param isAffinityEnabled New CPU Affinity for the Workers.
     */
    public void setWorkerCPUAffinity(boolean isAffinityEnabled) {
        config.setProperty(COMPSsConstants.WORKER_CPU_AFFINITY, isAffinityEnabled);
    }

    /**
     * Returns the Workers CPU Affinity flag value.
     *
     * @return The Workers CPU Affinity flag value.
     */
    public boolean isWorkerCPUAffinityEnabled() {
        return config.getBoolean(COMPSsConstants.WORKER_CPU_AFFINITY, false);
    }

    /**
     * Sets the new GPU Affinity for the Workers.
     *
     * @param isAffinityEnabled New GPU Affinity for the Workers.
     */
    public void setWorkerGPUAffinity(boolean isAffinityEnabled) {
        config.setProperty(COMPSsConstants.WORKER_GPU_AFFINITY, isAffinityEnabled);
    }

    /**
     * Returns the Workers GPU Affinity flag value.
     *
     * @return The Workers GPU Affinity flag value.
     */
    public boolean isWorkerGPUAffinityEnabled() {
        return config.getBoolean(COMPSsConstants.WORKER_GPU_AFFINITY, false);
    }

    /**
     * Returns the graph flag value.
     *
     * @return The graph flag value.
     */
    public boolean isGraph() {
        return config.getBoolean(COMPSsConstants.GRAPH, false);
    }

    /**
     * Sets a new graph value.
     *
     * @param graph New graph value.
     */
    public void setGraph(boolean graph) {
        config.setProperty(COMPSsConstants.GRAPH, graph);
    }

    /**
     * Returns the tracing flag value.
     *
     * @return The tracing flag value.
     */
    public boolean getTracing() {
        return config.getBoolean(COMPSsConstants.TRACING, false);
    }

    /**
     * Sets a new tracing flag value.
     *
     * @param tracing New tracing flag value.
     */
    public void setTracing(boolean tracing) {
        config.setProperty(COMPSsConstants.TRACING, tracing);
    }

    /**
     * Returns Extrae's working directory path.
     *
     * @return Extrae's working directory path.
     */
    public String getExtraeWDir() {
        return config.getString(COMPSsConstants.EXTRAE_WORKING_DIR, ".");
    }

    /**
     * Sets a new path as Extrae's working directory.
     *
     * @param extraeWDir New Extrae's working directory path.
     */
    public void setExtraeWDir(String extraeWDir) {
        config.setProperty(COMPSsConstants.EXTRAE_WORKING_DIR, extraeWDir);
    }

    public boolean getTracingTaskDep() {
        return config.getBoolean(COMPSsConstants.TRACING_TASK_DEPENDENCIES, false);
    }

    /**
     * Returns the custom Extrae configuration file path.
     *
     * @return The custom Extrae configuration file path.
     */
    public String getCustomExtraeFile() {
        return config.getString(COMPSsConstants.EXTRAE_CONFIG_FILE, COMPSsDefaults.CUSTOM_EXTRAE_FILE);
    }

    /**
     * Sets a new custom Extrae configuration file path.
     *
     * @param extraeFilePath New custom Extrae configuration file path.
     */
    public void setCustomExtraeFile(String extraeFilePath) {
        config.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, extraeFilePath);
    }

    /**
     * Returns the custom Extrae configuration file path for python worker.
     *
     * @return The custom Extrae configuration file path for python worker.
     */
    public String getCustomExtraeFilePython() {
        return config.getString(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE, COMPSsDefaults.PYTHON_CUSTOM_EXTRAE_FILE);
    }

    /**
     * Sets a new custom Extrae configuration file path for python worker.
     *
     * @param extraeFilePathPython New custom Extrae configuration file path for python worker.
     */
    public void setCustomExtraeFilePython(String extraeFilePathPython) {
        config.setProperty(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE, extraeFilePathPython);
    }

    /**
     * Returns the monitor interval value.
     *
     * @return The monitor interval value.
     */
    public long getMonitorInterval() {
        return config.getLong(COMPSsConstants.MONITOR, COMPSsDefaults.MONITOR_INTERVAL);
    }

    /**
     * Sets a new monitor interval.
     *
     * @param seconds New monitor interval.
     */
    public void setMonitorInterval(long seconds) {
        config.setProperty(COMPSsConstants.MONITOR, seconds);
    }

    /**
     * Returns the lang value.
     *
     * @return The lang value.
     */
    public String getLang() {
        return config.getString(COMPSsConstants.LANG, COMPSsConstants.Lang.JAVA.name());
    }

    /**
     * Sets a new language value.
     *
     * @param lang New language value.
     */
    public void setLang(String lang) {
        config.setProperty(COMPSsConstants.LANG, lang);
    }

    /**
     * Returns the worker classpath.
     *
     * @return The worker classpath.
     */
    public String getWorkerCP() {
        return config.getString(COMPSsConstants.WORKER_CP);
    }

    /**
     * Sets a new worker classpath.
     *
     * @param classpath New worker classpath.
     */
    public void setWorkerCP(String classpath) {
        config.setProperty(COMPSsConstants.WORKER_CP, classpath);
    }

    /**
     * Returns the service name.
     *
     * @return The service name.
     */
    public String getServiceName() {
        return config.getString(COMPSsConstants.SERVICE_NAME);
    }

    /**
     * Sets a new service name.
     *
     * @param serviceName New service name.
     */
    public void setServiceName(String serviceName) {
        config.setProperty(COMPSsConstants.SERVICE_NAME, serviceName);
    }

    /**
     * Returns the service name.
     *
     * @return The service name.
     */
    public String getWorkerJVMOpts() {
        return config.getString(COMPSsConstants.WORKER_JVM_OPTS);
    }

    /**
     * Sets new JVM Options for the workers.
     *
     * @param jvmOpts New JVM options for the workers.
     */
    public void setWorkerJVMOpts(String jvmOpts) {
        config.setProperty(COMPSsConstants.WORKER_JVM_OPTS, jvmOpts);
    }

    /**
     * Saves the current configuration.
     *
     * @throws ConfigurationException Exception when configuration cannot be saved.
     */
    public void save() throws ConfigurationException {
        FileHandler handler = new FileHandler(this.config);
        handler.save();
    }

    /**
     * Returns if the configuration must be stored to file or not.
     *
     * @return {@code true} if the configuration must be stored to file, {@code false} otherwise.
     */
    public boolean isToFile() {
        return config.getBoolean(COMPSsConstants.COMPSS_TO_FILE, false);
    }

    /**
     * Returns the value of a generic property {@code propertyName} from the configuration.
     *
     * @param propertyName Name of a generic property.
     * @return The value associated with the given property name {@code propertyName}.
     */
    public String getProperty(String propertyName) {
        Object prop = config.getProperty(propertyName);
        if (prop != null) {
            return prop.toString();
        }

        return null;
    }

}
