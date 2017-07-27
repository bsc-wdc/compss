package es.bsc.compss.test.dummyAdaptor;

import java.util.List;

import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.exceptions.InitNodeException;

import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.Transferable;
import es.bsc.compss.types.data.listener.EventListener;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.job.Job;
import es.bsc.compss.types.job.JobListener;
import es.bsc.compss.types.resources.ExecutorShutdownListener;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ShutdownListener;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;


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
    public Job<?> newJob(int taskId, TaskDescription taskParams, Implementation impl, Resource res, 
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
    public void shutdownExecutionManager(ExecutorShutdownListener sl) {
    }

    @Override
    public boolean generateWorkersDebugInfo() {
        return false;
    }

}
