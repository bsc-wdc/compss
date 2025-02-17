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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.MethodResourceDescription.MethodResourceType;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;


public class ClusterWorker extends Worker<MethodResourceDescription> {

    // Available resource capabilities
    // Task count
    private final int limitOfTasks;
    private AtomicInteger sharedRunningTasks;

    private int numNodes = 0;
    private int usedCPUs = 0;
    private int usedGPUs = 0;
    private int usedFPGAs = 0;
    private int usedOthers = 0;

    private final int maxCPUs;
    private final int maxGPUs;
    private final int maxFPGAs;
    private final int maxOthers;


    /**
     * Instantiates a new Cluster worker.
     */
    public ClusterWorker(String hostname, String clusterNode, ClusterMethodResourceDescription md,
        MethodConfiguration config, Map<String, String> sharedDisks, AtomicInteger sharedLimitTask) {
        super(hostname, md, config, sharedDisks);
        this.numNodes = md.getNumClusters();
        this.limitOfTasks = md.getLimitOfTasks();
        this.sharedRunningTasks = sharedLimitTask;
        this.maxCPUs = numNodes * description.totalCPUComputingUnits;
        this.maxGPUs = numNodes * description.totalGPUComputingUnits;
        this.maxFPGAs = numNodes * description.totalFPGAComputingUnits;
        this.maxOthers = numNodes * description.totalOtherComputingUnits;
    }

    /**
     * Instantiates a new Cluster worker that is a copy of the given one.
     *
     * @param cw the cw
     */
    public ClusterWorker(ClusterWorker cw) {
        super(cw);
        this.limitOfTasks = cw.limitOfTasks;
        this.sharedRunningTasks = cw.sharedRunningTasks;
        this.maxCPUs = cw.maxCPUs;
        this.maxGPUs = cw.maxGPUs;
        this.maxFPGAs = cw.maxFPGAs;
        this.maxOthers = cw.maxOthers;
    }

    @Override
    protected int limitIdealSimultaneousTasks(int ideal) {
        int internalLimit = Math.min(this.maxCPUs * numNodes, limitOfTasks);
        return Math.min(internalLimit, ideal);
    }

    @Override
    public Integer simultaneousCapacity(Implementation impl) {
        return Math.min(limitOfTasks, numNodes * fitCount(impl));
    }

    @Override
    public ResourceType getType() {
        return ResourceType.WORKER;
    }

    @Override
    public boolean canRun(Implementation implementation) {

        if (this.isLost()) {
            return false;
        }
        if (implementation.getTaskType() == TaskType.METHOD) {
            LOGGER.debug("******* Executiong Can Run in " + this.name);
            LOGGER.debug("******* Description: " + this.description);
            MethodResourceDescription requirements = (MethodResourceDescription) implementation.getRequirements();
            boolean res = this.description.contains(requirements);
            LOGGER.debug("******** Result in Can Run in " + this.name + " is " + res);
            return res;

        }
        return false;

    }

    @Override
    public String getMonitoringData(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("<LimitOfTasks>").append(this.limitOfTasks).append("</LimitOfTasks>").append("\n");
        sb.append(prefix).append("<TotalCPUComputingUnits>").append(this.description.getTotalCPUComputingUnits())
            .append("</TotalCPUComputingUnits>").append("\n");
        sb.append(prefix).append("<TotalGPUComputingUnits>").append(this.description.getTotalGPUComputingUnits())
            .append("</TotalGPUComputingUnits>").append("\n");
        sb.append(prefix).append("<TotalFPGAComputingUnits>").append(this.description.getTotalFPGAComputingUnits())
            .append("</TotalFPGAComputingUnits>").append("\n");
        sb.append(prefix).append("<TotalOTHERComputingUnits>").append(this.description.getTotalOTHERComputingUnits())
            .append("</TotalOTHERComputingUnits>").append("\n");
        sb.append(prefix).append("<Memory>").append(this.description.getMemorySize()).append("</Memory>").append("\n");
        sb.append(prefix).append("<Disk>").append(this.description.getStorageSize()).append("</Disk>").append("\n");
        return sb.toString();
    }

    @Override
    public Integer fitCount(Implementation impl) {

        if (impl.getTaskType() != TaskType.METHOD) {
            return null;
        }
        MethodResourceDescription mrd = (MethodResourceDescription) impl.getRequirements();
        int res = Math.min(limitOfTasks, numNodes * this.description.canHostSimultaneously(mrd));
        return res;
    }

    @Override
    public boolean hasAvailable(MethodResourceDescription consumption) {
        synchronized (this.description) {
            int nCPUS = consumption.totalCPUComputingUnits;
            int nGPUS = consumption.totalGPUComputingUnits;
            int nFPGAs = consumption.totalFPGAComputingUnits;
            final int nOthers = consumption.totalOtherComputingUnits;
            boolean canRun = sharedRunningTasks.get() < limitOfTasks;
            canRun = canRun && (nCPUS <= (maxCPUs - usedCPUs));
            canRun = canRun && (nGPUS <= (maxGPUs - usedGPUs));
            canRun = canRun && (nFPGAs <= (maxFPGAs - usedFPGAs));
            canRun = canRun && (nOthers <= (maxOthers - usedOthers));

            if (consumption.getMethodType().equals(MethodResourceType.CLUSTER)) {
                ClusterMethodResourceDescription cmrd;
                cmrd = (ClusterMethodResourceDescription) consumption;
                canRun = canRun && (numNodes <= cmrd.getNumClusters());
            }
            return canRun;
        }
    }

    @Override
    public boolean hasAvailableSlots() {
        boolean available = sharedRunningTasks.get() < limitOfTasks;
        available = available && usedCPUs < maxCPUs;
        available = available && usedGPUs < maxGPUs;
        available = available && usedFPGAs < maxFPGAs;
        available = available && usedOthers < maxOthers;
        return available;
    }

    private MethodResourceDescription reserveResourceMethod(MethodResourceDescription consumption) {
        sharedRunningTasks.incrementAndGet();
        usedCPUs += consumption.totalCPUComputingUnits;
        usedGPUs += consumption.totalGPUComputingUnits;
        usedFPGAs += consumption.totalFPGAComputingUnits;
        usedOthers += consumption.totalOtherComputingUnits;

        return consumption;
    }

    private MethodResourceDescription reserveResourceCluster(ClusterMethodResourceDescription consumption) {

        int nNodes = consumption.getNumClusters();
        sharedRunningTasks.getAndAdd(nNodes);
        usedCPUs += nNodes * consumption.totalCPUComputingUnits;
        usedGPUs += nNodes * consumption.totalGPUComputingUnits;
        usedFPGAs += nNodes * consumption.totalFPGAComputingUnits;
        usedOthers += nNodes * consumption.totalOtherComputingUnits;
        // debug();
        return consumption;
    }

    @Override
    public MethodResourceDescription reserveResource(MethodResourceDescription consumption) {
        if (consumption == null || !hasAvailable(consumption)) {
            return null;
        }
        synchronized (this) {
            if (consumption.getMethodType().equals(MethodResourceType.CLUSTER)) {
                return reserveResourceCluster((ClusterMethodResourceDescription) consumption);
            } else {
                return reserveResourceMethod(consumption);
            }
        }
    }

    private void releaseResourceMethod(MethodResourceDescription consumption) {
        sharedRunningTasks.decrementAndGet();
        usedCPUs -= consumption.totalCPUComputingUnits;
        usedGPUs -= consumption.totalGPUComputingUnits;
        usedFPGAs -= consumption.totalFPGAComputingUnits;
        usedOthers -= consumption.totalOtherComputingUnits;
    }

    private void releaseResourceCluster(ClusterMethodResourceDescription consumption) {
        int nNodes = consumption.getNumClusters();
        sharedRunningTasks.addAndGet(-nNodes);
        usedCPUs -= nNodes * consumption.totalCPUComputingUnits;
        usedGPUs -= nNodes * consumption.totalGPUComputingUnits;
        usedFPGAs -= nNodes * consumption.totalFPGAComputingUnits;
        usedOthers -= nNodes * consumption.totalOtherComputingUnits;
    }

    @Override
    public void releaseResource(MethodResourceDescription consumption) {
        if (consumption == null) {
            return;
        }

        synchronized (this) {
            if (consumption.getMethodType().equals(MethodResourceType.CLUSTER)) {
                releaseResourceCluster((ClusterMethodResourceDescription) consumption);
            } else {
                releaseResourceMethod(consumption);
            }
        }
    }

    @Override
    public void releaseAllResources() {
        super.resetUsedTaskCounts();
        this.usedCPUs = 0;
        this.usedGPUs = 0;
        this.usedFPGAs = 0;
        this.usedOthers = 0;
    }

    @Override
    public ClusterWorker getSchedulingCopy() {
        return new ClusterWorker(this);
    }

    private Float getValue() {
        return this.description.value;
    }

    @Override
    public int compareTo(Resource t) {
        if (t == null) {
            throw new NullPointerException();
        }
        switch (t.getType()) {
            case HTTP:
            case WORKER:
                ClusterWorker w = (ClusterWorker) t;
                if (this.description.getValue() == null) {
                    if (w.getValue() == null) {
                        return w.getName().compareTo(getName());
                    }
                    return 1;
                }
                if (w.getValue() == null) {
                    return -1;
                }
                float dif = w.getValue() - this.description.getValue();
                if (dif > 0) {
                    return -1;
                }
                if (dif < 0) {
                    return 1;
                }
                return getName().compareTo(w.getName());
            case MASTER:
                return -1;
            default:
                return getName().compareTo(t.getName());
        }
    }
}
