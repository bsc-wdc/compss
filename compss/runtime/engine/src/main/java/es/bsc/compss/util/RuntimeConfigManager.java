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
package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.COMPSsPaths;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.configuration2.PropertiesConfiguration;
import org.apache.commons.configuration2.builder.FileBasedConfigurationBuilder;
import org.apache.commons.configuration2.builder.fluent.Parameters;
import org.apache.commons.configuration2.ex.ConfigurationException;
import org.apache.commons.configuration2.io.FileHandler;


/**
 * Loads the runtime configuration.
 */
public class RuntimeConfigManager {

    private final PropertiesConfiguration config;


    /**
     * Loads the runtime configuration found in path {@code pathToConfigFile}.
     * 
     * @param pathToConfigFile Path to configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    public RuntimeConfigManager(String pathToConfigFile) throws ConfigurationException {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
            new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class)
                .configure(new Parameters().properties().setFileName(pathToConfigFile));
        this.config = builder.getConfiguration();
    }

    /**
     * Loads the runtime configuration found in URL {@code pathToConfigFile}.
     * 
     * @param pathToConfigFile URL path to configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    public RuntimeConfigManager(URL pathToConfigFile) throws ConfigurationException {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
            new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class)
                .configure(new Parameters().properties().setURL(pathToConfigFile));
        this.config = builder.getConfiguration();
    }

    /**
     * Loads the runtime configuration from an input stream {@code stream}.
     * 
     * @param stream Stream to configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    public RuntimeConfigManager(InputStream stream) throws ConfigurationException {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
            new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class)
                .configure(new Parameters().properties());
        this.config = builder.getConfiguration();
        FileHandler handler = new FileHandler(this.config);
        handler.load(stream);
    }

    /**
     * Loads the runtime configuration from a file {@code file}.
     * 
     * @param file File object pointing to the configuration file.
     * @throws ConfigurationException Exception when parsing the configuration file.
     */
    public RuntimeConfigManager(File file) throws ConfigurationException {
        FileBasedConfigurationBuilder<PropertiesConfiguration> builder =
            new FileBasedConfigurationBuilder<PropertiesConfiguration>(PropertiesConfiguration.class)
                .configure(new Parameters().properties().setFile(file));
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
