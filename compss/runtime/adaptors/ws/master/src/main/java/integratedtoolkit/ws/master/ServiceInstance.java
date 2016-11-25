package integratedtoolkit.ws.master;

import java.util.List;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.exceptions.InitNodeException;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.DataLocation.Protocol;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.ws.master.configuration.WSConfiguration;


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
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation<?> impl, Resource res, 
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
            case EXTERNAL_PSCO_T:
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
