package es.bsc.compss.types.fake;

import java.util.List;

import es.bsc.compss.types.COMPSsNode;
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
import es.bsc.compss.types.uri.MultiURI;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.types.annotations.parameter.DataType;


public class FakeNode extends COMPSsNode {

    @Override
    public String getName() {
        return "a";
    }

    @Override
    public void setInternalURI(MultiURI uri) {

    }

    @Override
    public void sendData(LogicalData ld, DataLocation dl, DataLocation dl1, LogicalData ld1, Transferable t, EventListener el) {
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation dl, DataLocation dl1, LogicalData ld1, Transferable t, EventListener el) {
    }

    @Override
    public Job<?> newJob(int i, TaskDescription tp, Implementation i1, Resource rsrc, List<String> slaveWorkersNodeNames,
            JobListener jl) {

        return null;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop(ShutdownListener sl) {
    }

    @Override
    public SimpleURI getCompletePath(DataType pt, String string) {
        return new SimpleURI("");
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
