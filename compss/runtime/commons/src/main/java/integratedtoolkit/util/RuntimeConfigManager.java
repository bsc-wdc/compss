package integratedtoolkit.util;

import integratedtoolkit.ITConstants;

import java.io.File;
import java.io.InputStream;
import java.net.URL;

import org.apache.commons.configuration.ConfigurationException;
import org.apache.commons.configuration.PropertiesConfiguration;


public class RuntimeConfigManager {

    private final PropertiesConfiguration config;

    public RuntimeConfigManager(String pathToConfigFile) throws ConfigurationException {
        config = new PropertiesConfiguration(pathToConfigFile);
    }

    public RuntimeConfigManager(URL pathToConfigFile) throws ConfigurationException {
        config = new PropertiesConfiguration(pathToConfigFile);
    }

    public RuntimeConfigManager(InputStream stream) throws ConfigurationException {
        config = new PropertiesConfiguration();
        config.load(stream);
    }

    public RuntimeConfigManager(File file) throws ConfigurationException {
        config = new PropertiesConfiguration(file);
    }
    
    public String getDeploymentId(){
        return config.getString(ITConstants.IT_DEPLOYMENT_ID, ITConstants.DEFAULT_DEPLOYMENT_ID);
    }
        
    public void setDeploymentId(String uuid) {
        config.setProperty(ITConstants.IT_DEPLOYMENT_ID, uuid);
    }
    
    public String getMasterPort(){
        return config.getString(ITConstants.IT_MASTER_PORT);
    }
        
    public void setMasterPort(String port) {
        config.setProperty(ITConstants.IT_MASTER_PORT, port);
    }
    
    public String getAppName() {
        return config.getString(ITConstants.IT_APP_NAME);
    }
    
    public String getCOMPSsBaseLogDir() {
        return config.getString(ITConstants.IT_BASE_LOG_DIR);
    }
    
    public String getSpecificLogDir() {
    	return config.getString(ITConstants.IT_SPECIFIC_LOG_DIR);
    }

    public void setAppName(String name) {
        config.setProperty(ITConstants.IT_APP_NAME, name);
    }

    public String getProjectFile() {
        return config.getString(ITConstants.IT_PROJ_FILE);
    }

    public void setProjectFile(String location) {
        config.setProperty(ITConstants.IT_PROJ_FILE, location);
    }

    public String getProjectSchema() {
        return config.getString(ITConstants.IT_PROJ_SCHEMA);
    }

    public void setProjectSchema(String location) {
        config.setProperty(ITConstants.IT_PROJ_SCHEMA, location);
    }

    public String getResourcesFile() {
        return config.getString(ITConstants.IT_RES_FILE);
    }

    public void setResourcesFile(String location) {
        config.setProperty(ITConstants.IT_RES_FILE, location);
    }

    public String getResourcesSchema() {
        return config.getString(ITConstants.IT_RES_SCHEMA);
    }

    public void setResourcesSchema(String location) {
        config.setProperty(ITConstants.IT_RES_SCHEMA, location);
    }

    public String getScheduler() {
        return config.getString(ITConstants.IT_SCHEDULER);
    }

    public void setScheduler(String implementingClass) {
        config.setProperty(ITConstants.IT_SCHEDULER, implementingClass);
    }

    public String getLog4jConfiguration() {
        return config.getString(ITConstants.LOG4J);
    }

    public void setLog4jConfiguration(String location) {
        config.setProperty(ITConstants.LOG4J, location);
    }

    public void setCommAdaptor(String adaptor) {
        config.setProperty(ITConstants.COMM_ADAPTOR, adaptor);
    }

    public String getCommAdaptor() {
        return config.getString(ITConstants.COMM_ADAPTOR);
    }

    public String getGATBrokerAdaptor() {
        return config.getString(ITConstants.GAT_BROKER_ADAPTOR);
    }

    public void setGATBrokerAdaptor(String adaptor) {
        config.setProperty(ITConstants.GAT_BROKER_ADAPTOR, adaptor);
    }

    public String getGATFileAdaptor() {
        return config.getString(ITConstants.GAT_FILE_ADAPTOR);
    }

    public void setGATFileAdaptor(String adaptor) {
        config.setProperty(ITConstants.GAT_FILE_ADAPTOR, adaptor);
    }

    public void setGraph(boolean graph) {
        config.setProperty(ITConstants.IT_GRAPH, graph);
    }

    public boolean isGraph() {
        return config.getBoolean(ITConstants.IT_GRAPH, false);
    }

    public void setTracing(int tracing) {
        config.setProperty(ITConstants.IT_TRACING, tracing);
    }

    public int getTracing() {
        return config.getInt(ITConstants.IT_TRACING, 0);
    }

    public boolean isPresched() {
        return config.getBoolean(ITConstants.IT_PRESCHED, false);
    }

    public void setPresched(boolean presched) {
        config.setProperty(ITConstants.IT_PRESCHED, presched);
    }

    public void setMonitorInterval(long seconds) {
        config.setProperty(ITConstants.IT_MONITOR, seconds);
    }

    public long getMonitorInterval() {
        return config.getLong(ITConstants.IT_MONITOR, ITConstants.DEFAULT_MONITOR_INTERVAL);
    }

    public String getLang() {
        return config.getString(ITConstants.IT_LANG, "java");
    }

    public void setLang(String lang) {
        config.setProperty(ITConstants.IT_LANG, lang);
    }

    public String getWorkerCP() {
        return config.getString(ITConstants.IT_WORKER_CP);
    }

    public void setWorkerCP(String classpath) {
        config.setProperty(ITConstants.IT_WORKER_CP, classpath);
    }

    public String getContext() {
        return config.getString(ITConstants.IT_CONTEXT);
    }

    public void setContext(String context) {
        config.setProperty(ITConstants.IT_CONTEXT, context);
    }

    public String getGATAdaptor() {
        return config.getString(ITConstants.GAT_ADAPTOR_PATH, System.getenv("GAT_LOCATION") + "/lib/adaptors");
    }

    public void setGATAdaptor(String adaptorPath) {
        config.setProperty(ITConstants.GAT_ADAPTOR_PATH, adaptorPath);
    }

    public boolean isGATDebug() {
        return config.getBoolean(ITConstants.GAT_DEBUG, false);
    }

    public void setGATDebug(boolean debug) {
        config.setProperty(ITConstants.GAT_DEBUG, debug);
    }

    public void setServiceName(String serviceName) {
        config.setProperty(ITConstants.IT_SERVICE_NAME, serviceName);
    }

    public String getServiceName() {
        return config.getString(ITConstants.IT_SERVICE_NAME);
    }

    public void save() throws ConfigurationException {
        config.save();
    }

    public boolean isToFile() {
        return config.getBoolean(ITConstants.IT_TO_FILE, false);
    }
    
    public String getProperty(String propertyName){
    	Object prop = config.getProperty(propertyName);
    	if (prop != null){
    		return prop.toString();
    	}else
    		return null;
    }

}
