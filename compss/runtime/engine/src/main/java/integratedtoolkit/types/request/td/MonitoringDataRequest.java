package integratedtoolkit.types.request.td;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.ResourceManager;
import java.util.concurrent.Semaphore;

/**
 * The MonitoringDataRequest class represents a request to obtain the current
 * resources and cores that can be run
 */
public class MonitoringDataRequest extends TDRequest {

    /**
     * Semaphore where to synchronize until the operation is done
     */
    private Semaphore sem;
    /**
     * Applications progress description
     */
    private String response;

    /**
     * Constructs a new TaskStateRequest
     *
     * @param sem semaphore where to synchronize until the current state is
     * described
     */
    public MonitoringDataRequest(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the current state is
     * described
     *
     * @return the semaphore where to synchronize until the current state is
     * described
     */
    public Semaphore getSemaphore() {
        return sem;
    }

    /**
     * Sets the semaphore where to synchronize until the current state is
     * described
     *
     * @param sem the semaphore where to synchronize until the current state is
     * described
     */
    public void setSemaphore(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the progress description in an xml format string
     *
     * @return progress description in an xml format string
     */
    public String getResponse() {
        return response;
    }

    /**
     * Sets the current state description
     *
     * @param response current state description
     */
    public void setResponse(String response) {
        this.response = response;
    }

    @Override
    public void process(TaskScheduler ts) {
        String prefix = "\t";
        StringBuilder monitorData = new StringBuilder();
        monitorData.append(ts.getCoresMonitoringData(prefix));

        monitorData.append(prefix).append("<ResourceInfo>").append("\n");
        monitorData.append(ResourceManager.getPendingRequestsMonitorData(prefix + "\t"));
        for (Worker r : ResourceManager.getAllWorkers()) {
            monitorData.append(prefix + "\t").append("<Resource id=\"" + r.getName() + "\">").append("\n");
            //CPU, Core, Memory, Disk, Provider, Image --> Inside resource
            monitorData.append(r.getMonitoringData(prefix + "\t\t"));
            String runnningActions = ts.getRunningActionMonitorData(r, prefix + "\t\t\t");
            if (runnningActions != null) {
                //Resource state = running
                monitorData.append(prefix + "\t\t").append("<Status>").append("Running").append("</Status>").append("\n");
                monitorData.append(prefix + "\t\t").append("<Actions>").append(runnningActions).append("</Actions>").append("\n");
            } else {
                //Resource state = on destroy
                monitorData.append(prefix + "\t\t").append("<Status>").append("On Destroy").append("</Status>").append("\n");
                monitorData.append(prefix + "\t\t").append("<Actions>").append("</Actions>").append("\n");
            }
            monitorData.append(prefix + "\t").append("</Resource>").append("\n");
        }
        monitorData.append(prefix).append("</ResourceInfo>").append("\n");

        monitorData.append(prefix).append("<AccumulatedCost>" + ResourceManager.getTotalCost() + "</AccumulatedCost>").append("\n");
        response = monitorData.toString();
        sem.release();
    }

}
