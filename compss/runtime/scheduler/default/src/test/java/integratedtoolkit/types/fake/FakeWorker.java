package integratedtoolkit.types.fake;

import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.Worker;

public class FakeWorker extends Worker<FakeResourceDescription> {

    private final FakeResourceDescription available;

    public FakeWorker(String name, FakeResourceDescription description) {
        super(name, description, new FakeNode(name), 3);
        available = (FakeResourceDescription) description.copy();
    }

    public FakeWorker(FakeWorker fw) {
        super(fw);
        available = (FakeResourceDescription) fw.available.copy();
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
    public boolean canRun(Implementation<?> implementation) {
        return true;
    }

    @Override
    public boolean reserveResource(FakeResourceDescription consumption) {
        synchronized (available) {
            if (hasAvailable(consumption)) {
                available.reduce(consumption);
                return true;
            } else {
                return false;
            }
        }
    }

    @Override
    public void releaseResource(FakeResourceDescription consumption) {
        synchronized (available) {
            available.increase(consumption);
        }
    }

    @Override
    public boolean hasAvailable(FakeResourceDescription consumption) {
        synchronized (available) {
            return available.canHost(consumption);
        }
    }

    @Override
    public Integer fitCount(Implementation<?> impl) {
        return 10;
    }

    @Override
    public Worker<?> getSchedulingCopy() {
        return new FakeWorker(this);
    }

}
