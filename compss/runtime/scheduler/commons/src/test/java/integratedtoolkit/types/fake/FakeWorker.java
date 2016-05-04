package integratedtoolkit.types.fake;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.MethodResourceDescription;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

public class FakeWorker extends Worker {

    public FakeWorker(MethodResourceDescription description ) {
        super("a", description, new FakeNode());
    }

    @Override
    public Resource.Type getType() {
        return Resource.Type.WORKER;
    }

    @Override
    public int compareTo(Resource rsrc) {
        return 0;
    }

    @Override
    public String getMonitoringData(String prefix) {
        return "";
    }

    @Override
    public boolean canRun(Implementation implementation) {
        return true;
    }

    @Override
    public Integer fitCount(Implementation impl) {
        return 10;
    }

    @Override
    public boolean hasAvailable(WorkerResourceDescription consumption) {
        return true;
    }

    @Override
    public boolean reserveResource(WorkerResourceDescription consumption) {
        return true;
    }

    @Override
    public void releaseResource(WorkerResourceDescription consumption) {

    }

    @Override
    public Worker getSchedulingCopy() {
        throw new UnsupportedOperationException("Not supported yet.");
    }

}
