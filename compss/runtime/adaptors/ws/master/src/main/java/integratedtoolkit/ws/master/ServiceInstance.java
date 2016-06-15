package integratedtoolkit.ws.master;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.operation.DataOperation.EventListener;
import integratedtoolkit.types.job.Job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.ws.master.configuration.WSConfiguration;


public class ServiceInstance extends COMPSsWorker {

    private WSConfiguration config;

    
    public ServiceInstance(String name, WSConfiguration config) {
        super(name, config);
        this.config = config;
    }

    public void start() throws Exception {
        //Do nothing
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
    public void setInternalURI(URI uri) {

    }

    @Override
    public Job<?> newJob(int taskId, TaskParams taskParams, Implementation<?> impl, Resource res, JobListener listener) {
        return new WSJob<COMPSsWorker>(taskId, taskParams, impl, res, listener);
    }

    @Override
    public void stop(ShutdownListener sl) {
        //No need to do anything
        sl.notifyEnd();
    }

    @Override
    public void sendData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        //Never sends Data
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {
        //Delegate on the master to obtain the data value
        DataLocation tgtLoc = DataLocation.getLocation(Comm.appHost, target.getPath());
        COMPSsNode node = null;
        node = Comm.appHost.getNode();
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
    public String getCompletePath(DataType type, String name) {
        switch (type) {
            case FILE_T:
                return Comm.appHost.getTempDirPath() + name;
            case OBJECT_T:
            case SCO_T:
            case PSCO_T:
                return name;
            default:
                return null;
        }
    }

    @Override
    public void deleteTemporary() {
    }

    @Override
    public void generatePackage() {
    }

    @Override
    public void generateWorkersDebugInfo() {
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
