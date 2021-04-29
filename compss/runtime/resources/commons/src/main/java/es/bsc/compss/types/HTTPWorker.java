package es.bsc.compss.types;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.HTTPResourceDescription;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.ResourceType;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.configuration.HTTPConfiguration;


public class HTTPWorker extends Worker<HTTPResourceDescription> {

    public HTTPWorker(String httpWorker, HTTPResourceDescription description, HTTPConfiguration config) {
        super(httpWorker, description, config, null);
    }

    public HTTPWorker(HTTPWorker httpWorker) {
        super(httpWorker);
    }

    @Override
    public ResourceType getType() {
        return ResourceType.HTTP;
    }

    @Override
    public boolean canRun(Implementation implementation) {
        return implementation.getTaskType().equals(TaskType.HTTP);
    }

    @Override
    public String getMonitoringData(String prefix) {
        return prefix + "<TotalComputingUnits></TotalComputingUnits>" + "\n";
    }

    @Override
    public Integer fitCount(Implementation impl) {
        return Integer.MAX_VALUE;
    }

    @Override
    public boolean hasAvailable(HTTPResourceDescription consumption) {
        return true;
    }

    @Override
    public boolean hasAvailableSlots() {
        return true;
    }

    @Override
    public HTTPResourceDescription reserveResource(HTTPResourceDescription consumption) {
        return consumption;
    }

    @Override
    public void releaseResource(HTTPResourceDescription consumption) {
        // does nothing
    }

    @Override
    public void releaseAllResources() {
        super.resetUsedTaskCounts();
    }

    @Override
    public Worker<HTTPResourceDescription> getSchedulingCopy() {
        return new HTTPWorker(this);
    }

    @Override
    public int compareTo(Resource t) {
        if (t == null) {
            throw new NullPointerException();
        }

        if (t.getType() == ResourceType.HTTP) {
            return 0;
        }

        return -1;
    }
}
