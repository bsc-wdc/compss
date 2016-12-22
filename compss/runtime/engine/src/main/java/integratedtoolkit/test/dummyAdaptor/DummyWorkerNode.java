package integratedtoolkit.test.dummyAdaptor;

import java.util.List;

import integratedtoolkit.exceptions.AnnounceException;
import integratedtoolkit.exceptions.InitNodeException;

import integratedtoolkit.types.annotations.parameter.DataType;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;


/**
 * Dummy Worker Node for integration tests
 *
 */
public class DummyWorkerNode extends COMPSsWorker {

    private final String name;


    /**
     * New DummyWorker node with name @name and configuration @config
     * 
     * @param name
     * @param config
     */
    public DummyWorkerNode(String name, MethodConfiguration config) {
        super(name, config);
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void start() throws InitNodeException {
    }

    @Override
    public String getUser() {
        return this.name;
    }

    @Override
    public String getClasspath() {
        return this.name;
    }

    @Override
    public String getPythonpath() {
        return this.name;
    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation<?> impl, Resource res, 
            List<String> slaveWorkersNodeNames, JobListener listener) {
        
        return null;
    }

    @Override
    public void setInternalURI(MultiURI uri) {
    }

    @Override
    public void stop(ShutdownListener sl) {
    }

    @Override
    public void sendData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason,
            EventListener listener) {
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {
    }

    @Override
    public void announceCreation() throws AnnounceException {
    }

    @Override
    public void announceDestruction() throws AnnounceException {
    }

    @Override
    public SimpleURI getCompletePath(DataType type, String name) {
        return null;
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

}
