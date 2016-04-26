package integratedtoolkit.types.fake;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.Worker;

public class FakeWorker extends Worker {

    public FakeWorker() {
        super("a", null, new FakeNode(), 3);
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
    public boolean hasAvailable(ResourceDescription consumption) {
        return true;
    }

    @Override
    public boolean reserveResource(ResourceDescription consumption) {
        return true;
    }

    @Override
    public void releaseResource(ResourceDescription consumption) {

    }

    @Override
    public Worker getSchedulingCopy() {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

}
