package es.bsc.compss.types.fake;

import es.bsc.compss.exceptions.AnnounceException;
import es.bsc.compss.exceptions.InitNodeException;
import es.bsc.compss.exceptions.UnstartedNodeException;
import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.annotations.parameter.DataType;
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
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import java.util.List;

public class FakeNode extends COMPSsWorker {

    private final String name;

    public FakeNode(String name) {
        super(name, null);
        this.name = name;
    }

    @Override
    public String getUser() {
        return "";
    }

    @Override
    public String getClasspath() {
        return "";
    }

    @Override
    public String getPythonpath() {
        return "";
    }

    @Override
    public void updateTaskCount(int processorCoreCount) {

    }

    @Override
    public void announceDestruction() throws AnnounceException {

    }

    @Override
    public void announceCreation() throws AnnounceException {

    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void start() throws InitNodeException {

    }

    @Override
    public void setInternalURI(MultiURI u) throws UnstartedNodeException {

    }

    @Override
    public Job<?> newJob(int taskId, TaskDescription taskparams, Implementation impl, Resource res, List<String> slaveWorkersNodeNames, JobListener listener) {
        return null;
    }

    @Override
    public void sendData(LogicalData srcData, DataLocation loc, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {

    }

    @Override
    public void obtainData(LogicalData srcData, DataLocation source, DataLocation target, LogicalData tgtData, Transferable reason, EventListener listener) {

    }

    @Override
    public void stop(ShutdownListener sl) {

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
