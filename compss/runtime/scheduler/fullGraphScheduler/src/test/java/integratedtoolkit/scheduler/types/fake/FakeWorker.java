package integratedtoolkit.scheduler.types.fake;

import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.Worker;


public class FakeWorker extends Worker<FakeResourceDescription, FakeImplementation> {

    private final FakeResourceDescription available;


    public FakeWorker(String name, FakeResourceDescription description, int limitOfTasks) {
        super(name, description, new FakeNode(name), limitOfTasks, null);
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
    public boolean canRun(FakeImplementation implementation) {
        return true;
    }

    @Override
    public FakeResourceDescription reserveResource(FakeResourceDescription consumption) {
        synchronized (available) {
            if (this.hasAvailable(consumption)) {
                return (FakeResourceDescription) available.reduceDynamic(consumption);
            } else {
                return null;
            }
        }
    }

    @Override
    public void releaseResource(FakeResourceDescription consumption) {
        synchronized (available) {
            available.increaseDynamic(consumption);
        }
    }

    @Override
    public void releaseAllResources() {
        synchronized (available) {
            super.resetUsedTaskCount();
            available.reduceDynamic(available);
            available.increaseDynamic(description);
        }
    }

    @Override
    public boolean hasAvailable(FakeResourceDescription consumption) {
        synchronized (available) {
            return available.canHost(consumption);
        }
    }

    @Override
    public boolean usesGPU(FakeResourceDescription consumption) {
        LOGGER.debug("fake worker");
        return false;
    }

    @Override
    public boolean usesFPGA(FakeResourceDescription consumption) {
        return false;
    }

    @Override
    public boolean usesOthers(FakeResourceDescription consumption) {
        return false;
    }

    @Override
    public Integer fitCount(FakeImplementation impl) {
        return 10;
    }

    @Override
    public Worker<FakeResourceDescription, FakeImplementation> getSchedulingCopy() {
        return new FakeWorker(this);
    }
}
