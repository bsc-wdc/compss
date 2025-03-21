/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ResourceManager;

import java.util.concurrent.Semaphore;


/**
 * The MonitoringDataRequest class represents a request to obtain the current resources and cores that can be run.
 */
public class MonitoringDataRequest extends TDRequest {

    private final Semaphore sem;

    private String response;


    /**
     * Constructs a new TaskStateRequest.
     *
     * @param sem semaphore where to synchronize until the current state is described.
     */
    public MonitoringDataRequest(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the current state is described.
     *
     * @return the semaphore where to synchronize until the current state is described.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the progress description in an xml format string.
     *
     * @return progress description in an xml format string.
     */
    public String getResponse() {
        return this.response;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        String prefix = "\t";
        StringBuilder monitorData = new StringBuilder();
        monitorData.append(ts.getCoresMonitoringData(prefix));

        monitorData.append(prefix).append("<ResourceInfo>").append("\n");
        monitorData.append(ResourceManager.getPendingRequestsMonitorData(prefix + "\t"));
        for (Worker<? extends WorkerResourceDescription> worker : ResourceManager.getAllWorkers()) {
            monitorData.append(prefix).append("\t").append("<Resource id=\"").append(worker.getName()).append("\">")
                .append("\n");
            // CPU, Core, Memory, Disk, Provider, Image --> Inside resource
            monitorData.append(worker.getMonitoringData(prefix + "\t\t"));
            String runnningActions = ts.getRunningActionMonitorData(worker, prefix + "\t\t\t");
            if (runnningActions != null) {
                // Resource state = running
                monitorData.append(prefix).append("\t\t").append("<Status>").append("Running").append("</Status>")
                    .append("\n");
                monitorData.append(prefix).append("\t\t").append("<Actions>").append("\n");
                monitorData.append(runnningActions);
                monitorData.append(prefix).append("\t\t").append("</Actions>").append("\n");
            } else {
                // Resource state = on destroy
                monitorData.append(prefix).append("\t\t").append("<Status>").append("On Destroy").append("</Status>")
                    .append("\n");
                monitorData.append(prefix).append("\t\t").append("<Actions>").append("</Actions>").append("\n");
            }
            monitorData.append(prefix).append("\t").append("</Resource>").append("\n");
        }
        monitorData.append(prefix).append("</ResourceInfo>").append("\n");

        monitorData.append(prefix).append("<Statistics>").append("\n");
        monitorData.append(prefix).append("\t").append("<Statistic>").append("\n");
        monitorData.append(prefix).append("\t\t").append("<Key>").append("Accumulated Cost").append("</Key>")
            .append("\n");
        monitorData.append(prefix).append("\t\t").append("<Value>").append(ResourceManager.getTotalCost())
            .append("</Value>").append("\n");
        monitorData.append(prefix).append("\t").append("</Statistic>").append("\n");
        monitorData.append(prefix).append("</Statistics>").append("\n");

        this.response = monitorData.toString();
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.MONITORING_DATA;
    }

}
