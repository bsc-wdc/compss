package integratedtoolkit.types.fake;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.listener.EventListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.uri.MultiURI;
import integratedtoolkit.types.uri.SimpleURI;


public class FakeNode extends COMPSsNode {

    private final String name;


    public FakeNode(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
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
    public Job<?> newJob(int i, TaskDescription tp, Implementation<?> i1, Resource rsrc, Job.JobListener jl) {
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
    public void generatePackage() {

    }

    @Override
    public void generateWorkersDebugInfo() {

    }

}
