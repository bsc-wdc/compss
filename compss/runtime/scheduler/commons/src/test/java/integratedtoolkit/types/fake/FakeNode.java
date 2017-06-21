package integratedtoolkit.types.fake;

import java.util.List;

import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.job.JobListener;
import integratedtoolkit.types.resources.ExecutorShutdownListener;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;
import integratedtoolkit.types.annotations.parameter.DataType;


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
