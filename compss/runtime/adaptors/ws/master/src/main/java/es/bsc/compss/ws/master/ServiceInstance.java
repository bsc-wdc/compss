package es.bsc.compss.ws.master;

import java.util.List;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.types.COMPSsNode;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.location.DataLocation.Protocol;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.ws.master.configuration.WSConfiguration;


public class ServiceInstance extends COMPSsWorker {

    private WSConfiguration config;


    public ServiceInstance(String name, WSConfiguration config) {
        super(name, config);
        this.config = config;
    }

    @Override
    public void start() throws InitNodeException {
        // Do nothing
    }

    public String getWsdl() {
        return this.config.getWsdl();
    }

    public void setServiceName(String serviceName) {
        this.config.setServiceName(serviceName);
    }

    public String getServiceName() {
        return this.config.getServiceName();
    }

    public void setNamespace(String namespace) {
        this.config.setNamespace(namespace);
    }

    public String getNamespace() {
        return this.config.getNamespace();
    }

    public void setPort(String port) {
        this.config.setPort(port);
    }

    public String getPort() {
        return this.config.getPort();
    }

    @Override
    public String getName() {
        return this.config.getWsdl();
    }

    @Override
    public void setInternalURI(MultiURI uri) {

    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, 
            List<String> slaveWorkersNodeNames, JobListener listener) {
        
        return new WSJob(taskId, taskParams, impl, res, listener);
    }

    @Override
    public void stop(ShutdownListener sl) {
        // No need to do anything
        sl.notifyEnd();
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {
        // Never sends Data
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {
        
        // Delegate on the master to obtain the data value
        String path = target.getProtocol().getSchema() + target.getPath();
        DataLocation tgtLoc = null;
        try {
            SimpleURI uri = new SimpleURI(path);
            tgtLoc = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + path, e);
        }

        COMPSsNode node = Comm.getAppHost().getNode();
        node.obtainData(ld, source, tgtLoc, tgtData, reason, listener);
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
        // No need to do anything
    }

    @Override
    public void announceDestruction() {
        // No need to do anything
    }

    @Override
    public void announceCreation() {
        // No need to do anything
    }

    @Override
    public String getUser() {
        return "";
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        // The path of the data is the same than in the master
        String path = null;
        switch (type) {
            case FILE_T:
                path = Protocol.FILE_URI.getSchema() + Comm.getAppHost().getTempDirPath() + name;
                break;
            case OBJECT_T:
                path = Protocol.OBJECT_URI.getSchema() + name;
                break;
            case PSCO_T:
                path = Protocol.PERSISTENT_URI.getSchema() + name;
                break;
            case EXTERNAL_OBJECT_T:
                path = Protocol.PERSISTENT_URI.getSchema() + name;
                break;
            default:
                return null;
        }

        // Switch path to URI
        return new SimpleURI(path);
    }

    @Override
    public void deleteTemporary() {
    }

    @Override
    public boolean generatePackage() {
    	return false;
    }

    @Override
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
        // No executor for services
    }

    @Override
    public boolean generateWorkersDebugInfo() {
    	return false;
    }

    @Override
    public String getClasspath() {
        // No classpath for services
        return "";
    }

    @Override
    public String getPythonpath() {
        // No pythonpath for services
        return "";
    }

}
