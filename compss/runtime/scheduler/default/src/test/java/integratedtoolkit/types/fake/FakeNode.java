package integratedtoolkit.types.fake;

import integratedtoolkit.api.COMPSsRuntime.DataType;
import integratedtoolkit.types.COMPSsNode;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.Transferable;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.location.URI;
import integratedtoolkit.types.data.operation.DataOperation;
import integratedtoolkit.types.job.Job;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ShutdownListener;


public class FakeNode extends COMPSsNode {

    final String name;

    FakeNode(String name) {
        this.name = name;
    }

    @Override
    public String getName() {
        return name;
    }

    @Override
    public void setInternalURI(URI uri) {

    }

    @Override
    public void sendData(LogicalData ld, DataLocation dl, DataLocation dl1, LogicalData ld1, Transferable t, DataOperation.EventListener el) {
    }

    @Override
    public void obtainData(LogicalData ld, DataLocation dl, DataLocation dl1, LogicalData ld1, Transferable t, DataOperation.EventListener el) {
    }

    @Override
    public Job<?> newJob(int i, TaskParams tp, Implementation<?> i1, Resource rsrc, Job.JobListener jl) {
        return null;
    }

    @Override
    public void start() {
    }

    @Override
    public void stop(ShutdownListener sl) {
    }

    @Override
    public String getCompletePath(DataType pt, String string) {
        return "";
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
