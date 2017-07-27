package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


/**
 * Loads the runtime configuration
 * 
 */
public class RuntimeConfigManager {

    private final PropertiesConfiguration config;


    /**
     * Loads the runtime configuration found in path @pathToConfigFile
     * 
     * @param pathToConfigFile
     * @throws ConfigurationException
     */
    public RuntimeConfigManager(String pathToConfigFile) throws ConfigurationException {
        config = new PropertiesConfiguration(pathToConfigFile);
    }

    /**
     * Loads the runtime configuration found in URL @pathToConfigFile
     * 
     * @param pathToConfigFile
     * @throws ConfigurationException
     */
    public RuntimeConfigManager(URL pathToConfigFile) throws ConfigurationException {
        config = new PropertiesConfiguration(pathToConfigFile);
    }

    /**
     * Loads the runtime configuration from an input stream @stream
     * 
     * @param stream
     * @throws ConfigurationException
     */
    public RuntimeConfigManager(InputStream stream) throws ConfigurationException {
        config = new PropertiesConfiguration();
        config.load(stream);
    }

    /**
     * Loads the runtime configuration from a file @file
     * 
     * @param file
     * @throws ConfigurationException
     */
    public RuntimeConfigManager(File file) throws ConfigurationException {
        config = new PropertiesConfiguration(file);
    }

    /**
     * Returns the deployment id
     * 
     * @return
     */
    public String getDeploymentId() {
        return config.getString(COMPSsConstants.DEPLOYMENT_ID, COMPSsConstants.DEFAULT_DEPLOYMENT_ID);
    }

    /**
     * Sets a new value for the deployment id
     * 
     * @param uuid
     */
    public void setDeploymentId(String uuid) {
        config.setProperty(COMPSsConstants.DEPLOYMENT_ID, uuid);
    }

    /**
     * Returns the master node name
     * 
     * @return
     */
    public String getMasterName() {
        return config.getString(COMPSsConstants.MASTER_NAME);
    }

    /**
     * Sets a new value for the master node name
     * 
     * @param name
     */
    public void setMasterName(String name) {
        config.setProperty(COMPSsConstants.MASTER_NAME, name);
    }

    /**
     * Returns the master node port
     * 
     * @return
     */
    public String getMasterPort() {
        return config.getString(COMPSsConstants.MASTER_PORT);
    }

    /**
     * Sets a new value for the master node port
     * 
     * @param port
     */
    public void setMasterPort(String port) {
        config.setProperty(COMPSsConstants.MASTER_PORT, port);
    }

    /**
     * Returns the application name
     * 
     * @return
     */
    public String getAppName() {
        return config.getString(COMPSsConstants.APP_NAME);
    }

    /**
     * Sets a new name for the application
     * 
     * @param name
     */
    public void setAppName(String name) {
        config.setProperty(COMPSsConstants.APP_NAME, name);
    }

    /**
     * Returns the task summary flag value
     * 
     * @return
     */
    public String getTaskSummary() {
        return config.getString(COMPSsConstants.TASK_SUMMARY);
    }

    /**
     * Sets a new value for the task summary flag
     * 
     * @param value
     */
    public void setTaskSummary(String value) {
        config.setProperty(COMPSsConstants.TASK_SUMMARY, value);
    }

    /**
     * Returns the base log directory
     * 
     * @return
     */
    public String getCOMPSsBaseLogDir() {
        return config.getString(COMPSsConstants.BASE_LOG_DIR);
    }

    /**
     * Returns the specific application log directory
     * 
     * @return
     */
    public String getSpecificLogDir() {
        return config.getString(COMPSsConstants.SPECIFIC_LOG_DIR);
    }

    /**
     * Returns the project file
     * 
     * @return
     */
    public String getProjectFile() {
        return config.getString(COMPSsConstants.PROJ_FILE);
    }

    /**
     * Sets a new value for the project file
     * 
     * @param location
     */
    public void setProjectFile(String location) {
        config.setProperty(COMPSsConstants.PROJ_FILE, location);
    }

    /**
     * Returns the project schema
     * 
     * @return
     */
    public String getProjectSchema() {
        return config.getString(COMPSsConstants.PROJ_SCHEMA);
    }

    /**
     * Sets a new value for the project schema
     * 
     * @param location
     */
    public void setProjectSchema(String location) {
        config.setProperty(COMPSsConstants.PROJ_SCHEMA, location);
    }

    /**
     * Returns the resources file
     * 
     * @return
     */
    public String getResourcesFile() {
        return config.getString(COMPSsConstants.RES_FILE);
    }

    /**
     * Sets a new value for the resources file
     * 
     * @param location
     */
    public void setResourcesFile(String location) {
        config.setProperty(COMPSsConstants.RES_FILE, location);
    }

    /**
     * Returns the resources schema
     * 
     * @return
     */
    public String getResourcesSchema() {
        return config.getString(COMPSsConstants.RES_SCHEMA);
    }

    /**
     * Sets a new location for the resources schema
     * 
     * @param location
     */
    public void setResourcesSchema(String location) {
        config.setProperty(COMPSsConstants.RES_SCHEMA, location);
    }

    /**
     * Returns the scheduler to load
     * 
     * @return
     */
    public String getScheduler() {
        return config.getString(COMPSsConstants.SCHEDULER);
    }

    /**
     * Sets a new implementing class for the scheduler
     * 
     * @param implementingClass
     */
    public void setScheduler(String implementingClass) {
        config.setProperty(COMPSsConstants.SCHEDULER, implementingClass);
    }

    /**
     * Returns the log4j configuration file location
     * 
     * @return
     */
    public String getLog4jConfiguration() {
        return config.getString(COMPSsConstants.LOG4J);
    }

    /**
     * Sets a new location for the log4j configuration file
     * 
     * @param location
     */
    public void setLog4jConfiguration(String location) {
        config.setProperty(COMPSsConstants.LOG4J, location);
    }

    /**
     * Returns the COMM Adaptor implementing class
     * 
     * @return
     */
    public String getCommAdaptor() {
        return config.getString(COMPSsConstants.COMM_ADAPTOR);
    }

    /**
     * Sets a new implementing class for the COMM adaptor
     * 
     * @param adaptor
     */
    public void setCommAdaptor(String adaptor) {
        config.setProperty(COMPSsConstants.COMM_ADAPTOR, adaptor);
    }

    /**
     * Returns the CONN implementing class
     * 
     * @return
     */
    public String getConn() {
        return config.getString(COMPSsConstants.CONN);
    }

    /**
     * Sets a new implementing class for the CONN
     * 
     * @param connector
     */
    public void setConn(String connector) {
        config.setProperty(COMPSsConstants.CONN, connector);
    }

    /**
     * Returns the GAT context
     * 
     * @return
     */
    public String getContext() {
        return config.getString(COMPSsConstants.COMPSS_CONTEXT);
    }

    /**
     * Sets a new GAT context
     * 
     * @param context
     */
    public void setContext(String context) {
        config.setProperty(COMPSsConstants.COMPSS_CONTEXT, context);
    }

    /**
     * Returns the specific GAT adaptor path
     * 
     * @return
     */
    public String getGATAdaptor() {
        return config.getString(COMPSsConstants.GAT_ADAPTOR_PATH, COMPSsConstants.DEFAULT_GAT_ADAPTOR_LOCATION);
    }

    /**
     * Sets a new location for the GAT Adaptor
     * 
     * @param adaptorPath
     */
    public void setGATAdaptor(String adaptorPath) {
        config.setProperty(COMPSsConstants.GAT_ADAPTOR_PATH, adaptorPath);
    }

    /**
     * Returns if the GAT Adaptor is in debug mode or not
     * 
     * @return
     */
    public boolean isGATDebug() {
        return config.getBoolean(COMPSsConstants.GAT_DEBUG, false);
    }

    /**
     * Sets a new debug mode for the GAT Adaptor
     * 
     * @param debug
     */
    public void setGATDebug(boolean debug) {
        config.setProperty(COMPSsConstants.GAT_DEBUG, debug);
    }

    /**
     * Returns the GAT Broker Adaptor class
     * 
     * @return
     */
    public String getGATBrokerAdaptor() {
        return config.getString(COMPSsConstants.GAT_BROKER_ADAPTOR);
    }

    /**
     * Sets a new GAT Broker Adaptor class
     * 
     * @param adaptor
     */
    public void setGATBrokerAdaptor(String adaptor) {
        config.setProperty(COMPSsConstants.GAT_BROKER_ADAPTOR, adaptor);
    }

    /**
     * Returns the GAT File Adaptor
     * 
     * @return
     */
    public String getGATFileAdaptor() {
        return config.getString(COMPSsConstants.GAT_FILE_ADAPTOR);
    }

    /**
     * Sets a new GAT File Adaptor class
     * 
     * @param adaptor
     */
    public void setGATFileAdaptor(String adaptor) {
        config.setProperty(COMPSsConstants.GAT_FILE_ADAPTOR, adaptor);
    }

    /**
     * Sets the new CPU Affinity for the Workers
     * 
     * @param isAffinityEnabled
     */
    public void setWorkerCPUAffinity(boolean isAffinityEnabled) {
        config.setProperty(COMPSsConstants.WORKER_CPU_AFFINITY, isAffinityEnabled);
    }

    /**
     * Returns the Workers CPU Affinity flag value
     * 
     * @return
     */
    public boolean isWorkerCPUAffinityEnabled() {
        return config.getBoolean(COMPSsConstants.WORKER_CPU_AFFINITY, false);
    }
    
    /**
     * Sets the new GPU Affinity for the Workers
     * 
     * @param isAffinityEnabled
     */
    public void setWorkerGPUAffinity(boolean isAffinityEnabled) {
        config.setProperty(COMPSsConstants.WORKER_GPU_AFFINITY, isAffinityEnabled);
    }

    /**
     * Returns the Workers GPU Affinity flag value
     * 
     * @return
     */
    public boolean isWorkerGPUAffinityEnabled() {
        return config.getBoolean(COMPSsConstants.WORKER_GPU_AFFINITY, false);
    }

    /**
     * Returns the graph flag value
     * 
     * @return
     */
    public boolean isGraph() {
        return config.getBoolean(COMPSsConstants.GRAPH, false);
    }

    /**
     * Sets a new graph value
     * 
     * @param graph
     */
    public void setGraph(boolean graph) {
        config.setProperty(COMPSsConstants.GRAPH, graph);
    }

    /**
     * Returns the tracing flag value
     * 
     * @return
     */
    public int getTracing() {
        return config.getInt(COMPSsConstants.TRACING, 0);
    }

    /**
     * Sets a new tracing flag value
     * 
     * @param tracing
     */
    public void setTracing(int tracing) {
        config.setProperty(COMPSsConstants.TRACING, tracing);
    }

    /**
     * Returns the custom Extrae configuration file path
     * 
     * @return
     */
    public String getCustomExtraeFile() {
        return config.getString(COMPSsConstants.EXTRAE_CONFIG_FILE, COMPSsConstants.DEFAULT_CUSTOM_EXTRAE_FILE);
    }

    /**
     * Sets a new custom Extrae configuration file path
     * 
     * @param extraeFilePath
     */
    public void setCustomExtraeFile(String extraeFilePath) {
        config.setProperty(COMPSsConstants.EXTRAE_CONFIG_FILE, extraeFilePath);
    }

    /**
     * Returns the monitor interval value
     * 
     * @return
     */
    public long getMonitorInterval() {
        return config.getLong(COMPSsConstants.MONITOR, COMPSsConstants.DEFAULT_MONITOR_INTERVAL);
    }

    /**
     * Sets a new monitor interval
     * 
     * @param seconds
     */
    public void setMonitorInterval(long seconds) {
        config.setProperty(COMPSsConstants.MONITOR, seconds);
    }

    /**
     * Returns the lang value
     * 
     * @return
     */
    public String getLang() {
        return config.getString(COMPSsConstants.LANG, COMPSsConstants.Lang.JAVA.name());
    }

    /**
     * Sets a new lang value
     * 
     * @param lang
     */
    public void setLang(String lang) {
        config.setProperty(COMPSsConstants.LANG, lang);
    }

    /**
     * Returns the worker classpath
     * 
     * @return
     */
    public String getWorkerCP() {
        return config.getString(COMPSsConstants.WORKER_CP);
    }

    /**
     * Sets a new worker classpath
     * 
     * @param classpath
     */
    public void setWorkerCP(String classpath) {
        config.setProperty(COMPSsConstants.WORKER_CP, classpath);
    }

    /**
     * Returns the service name
     * 
     * @return
     */
    public String getServiceName() {
        return config.getString(COMPSsConstants.SERVICE_NAME);
    }

    /**
     * Sets a new service name
     * 
     * @param serviceName
     */
    public void setServiceName(String serviceName) {
        config.setProperty(COMPSsConstants.SERVICE_NAME, serviceName);
    }

    /**
     * Returns the service name
     * 
     * @return
     */
    public String getWorkerJVMOpts() {
        return config.getString(COMPSsConstants.WORKER_JVM_OPTS);
    }

    /**
     * Sets a new service name
     * 
     * @param serviceName
     */
    public void setWorkerJVMOpts(String jvmOpts) {
        config.setProperty(COMPSsConstants.WORKER_JVM_OPTS, jvmOpts);
    }

    /**
     * Saves the current configuration
     * 
     * @throws ConfigurationException
     */
    public void save() throws ConfigurationException {
        config.save();
    }

    /**
     * Returns if the configuration is toFile or not
     * 
     * @return
     */
    public boolean isToFile() {
        return config.getBoolean(COMPSsConstants.COMPSS_TO_FILE, false);
    }

    /**
     * Returns the value of a generic property @propertyName from the configuration
     * 
     * @param propertyName
     * @return
     */
    public String getProperty(String propertyName) {
        Object prop = config.getProperty(propertyName);
        if (prop != null) {
            return prop.toString();
        } else
            return null;
    }

}
