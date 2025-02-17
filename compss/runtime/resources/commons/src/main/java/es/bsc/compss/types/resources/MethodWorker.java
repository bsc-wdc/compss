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

import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.util.ResourceManager;

import java.util.Map;


public class MethodWorker extends Worker<MethodResourceDescription> {

    private String name;

    // Available resource capabilities
    protected final MethodResourceDescription available;

    // Task count
    private int usedCPUtaskCount = 0;
    private int maxCPUtaskCount;
    private int usedGPUtaskCount = 0;
    private int maxGPUtaskCount;
    private int usedFPGAtaskCount = 0;
    private int maxFPGAtaskCount;
    private int usedOthersTaskCount = 0;
    private int maxOthersTaskCount;


    /**
     * Creates a new MethodWorker instance.
     *
     * @param name Worker name.
     * @param description Worker description.
     * @param worker COMPSs worker.
     * @param limitOfTasks Limit of CPU tasks.
     * @param limitGPUTasks Limit of GPU tasks.
     * @param limitFPGATasks Limit of FPGA tasks.
     * @param limitOTHERTasks Limit of OTHER tasks.
     * @param sharedDisks Mounted shared disks.
     */
    public MethodWorker(String name, MethodResourceDescription description, COMPSsWorker worker, int limitOfTasks,
        int limitGPUTasks, int limitFPGATasks, int limitOTHERTasks, Map<String, String> sharedDisks) {

        super(name, description, worker, limitOfTasks, sharedDisks);
        this.name = name;
        this.available = new MethodResourceDescription(this.getDescription());

        this.maxCPUtaskCount = limitOfTasks;
        this.maxGPUtaskCount = limitGPUTasks;
        this.maxFPGAtaskCount = limitFPGATasks;
        this.maxOthersTaskCount = limitOTHERTasks;
    }

    /**
     * Creates a new MethodWorker instance.
     *
     * @param name Worker name.
     * @param description Worker description.
     * @param conf Worker configuration.
     * @param sharedDisks Mounted shared disks.
     */
    public MethodWorker(String name, MethodResourceDescription description, MethodConfiguration conf,
        Map<String, String> sharedDisks) {
        super(name, description, conf, sharedDisks);
        this.name = name;
        this.available = new MethodResourceDescription(this.getDescription()); // clone
        this.maxCPUtaskCount = conf.getLimitOfTasks();
        this.maxGPUtaskCount = conf.getLimitOfGPUTasks();
        this.maxFPGAtaskCount = conf.getLimitOfFPGATasks();
        this.maxOthersTaskCount = conf.getLimitOfOTHERsTasks();
    }

    /**
     * Clones the given MethodWorker.
     *
     * @param mw MethodWorker to clone.
     */
    public MethodWorker(MethodWorker mw) {
        super(mw);
        this.available = mw.available.copy();

        this.maxCPUtaskCount = mw.maxCPUtaskCount;
        this.usedCPUtaskCount = mw.usedCPUtaskCount;
        this.maxGPUtaskCount = mw.maxGPUtaskCount;
        this.usedGPUtaskCount = mw.usedGPUtaskCount;
        this.maxFPGAtaskCount = mw.maxFPGAtaskCount;
        this.usedFPGAtaskCount = mw.usedFPGAtaskCount;
        this.maxOthersTaskCount = mw.maxOthersTaskCount;
        this.usedOthersTaskCount = mw.usedOthersTaskCount;
    }

    @Override
    public String getName() {
        return this.name;
    }

    @Override
    protected int limitIdealSimultaneousTasks(int ideal) {
        return Math.min(this.getMaxCPUTaskCount(), ideal);
    }

    /**
     * Returns the available resources in the current worker.
     *
     * @return The available resources in the current worker.
     */
    public MethodResourceDescription getAvailable() {
        return this.available;
    }

    @Override
    public MethodResourceDescription reserveResource(MethodResourceDescription consumption) {
        synchronized (this.available) {
            if (this.hasAvailable(consumption)) {
                return (MethodResourceDescription) this.available.reduceDynamic(consumption);
            } else {
                return null;
            }
        }
    }

    @Override
    public void releaseResource(MethodResourceDescription consumption) {
        synchronized (this.available) {
            this.available.increaseDynamic(consumption);
        }
    }

    @Override
    public void releaseAllResources() {
        synchronized (this.available) {
            super.resetUsedTaskCounts();
            this.available.reduceDynamic(this.available);
            this.available.increaseDynamic(this.description);
        }
    }

    @Override
    public Integer fitCount(Implementation impl) {
        if (impl.getTaskType() != TaskType.METHOD) {
            return null;
        }
        MethodResourceDescription ctrs = (MethodResourceDescription) impl.getRequirements();
        return this.description.canHostSimultaneously(ctrs);
    }

    @Override
    public boolean hasAvailable(MethodResourceDescription consumption) {
        synchronized (this.available) {
            return this.available.containsDynamic(consumption);
        }
    }

    @Override
    public boolean hasAvailableSlots() {
        return ((this.usedCPUtaskCount < this.maxCPUtaskCount) || (this.usedGPUtaskCount < this.maxGPUtaskCount)
            || (this.usedFPGAtaskCount < this.maxFPGAtaskCount)
            || (this.usedOthersTaskCount < this.maxOthersTaskCount));
    }

    /**
     * Sets a new number of maximum CPU tasks.
     *
     * @param maxCPUTaskCount New number of maximum CPU tasks.
     */
    public void setMaxCPUTaskCount(int maxCPUTaskCount) {
        this.maxCPUtaskCount = maxCPUTaskCount;
    }

    /**
     * Returns the maximum number of CPU tasks.
     *
     * @return The maximum number of CPU tasks.
     */
    public int getMaxCPUTaskCount() {
        return this.maxCPUtaskCount;
    }

    /**
     * Returns the current number of CPU tasks.
     *
     * @return The current number of CPU tasks.
     */
    public int getUsedCPUTaskCount() {
        return this.usedCPUtaskCount;
    }

    /**
     * Decreases the current number of CPU tasks.
     */
    private void decreaseUsedCPUTaskCount() {
        this.usedCPUtaskCount--;
    }

    /**
     * Increases the current number of CPU tasks.
     */
    private void increaseUsedCPUTaskCount() {
        this.usedCPUtaskCount++;
    }

    /**
     * Sets a new number of maximum GPU tasks.
     *
     * @param maxGPUTaskCount New number of maximum GPU tasks.
     */
    public void setMaxGPUTaskCount(int maxGPUTaskCount) {
        this.maxGPUtaskCount = maxGPUTaskCount;
    }

    /**
     * Returns the maximum number of GPU tasks.
     *
     * @return The maximum number of GPU tasks.
     */
    public int getMaxGPUTaskCount() {
        return this.maxGPUtaskCount;
    }

    /**
     * Returns the current number of GPU tasks.
     *
     * @return The current number of GPU tasks.
     */
    public int getUsedGPUTaskCount() {
        return this.usedGPUtaskCount;
    }

    /**
     * Decreases the current number of GPU tasks.
     */
    private void decreaseUsedGPUTaskCount() {
        this.usedGPUtaskCount--;
    }

    /**
     * Increases the current number of GPU tasks.
     */
    private void increaseUsedGPUTaskCount() {
        this.usedGPUtaskCount++;
    }

    /**
     * Sets a new number of maximum FPGA tasks.
     *
     * @param maxFPGATaskCount New number of maximum FPGA tasks.
     */
    public void setMaxFPGATaskCount(int maxFPGATaskCount) {
        this.maxFPGAtaskCount = maxFPGATaskCount;
    }

    /**
     * Returns the maximum number of FPGA tasks.
     *
     * @return The maximum number of FPGA tasks.
     */
    public int getMaxFPGATaskCount() {
        return this.maxFPGAtaskCount;
    }

    /**
     * Returns the current number of FPGA tasks.
     *
     * @return The current number of FPGA tasks.
     */
    public int getUsedFPGATaskCount() {
        return this.usedFPGAtaskCount;
    }

    /**
     * Decreases the current number of FPGA tasks.
     */
    private void decreaseUsedFPGATaskCount() {
        this.usedFPGAtaskCount--;
    }

    /**
     * Increases the current number of FPGA tasks.
     */
    private void increaseUsedFPGATaskCount() {
        this.usedFPGAtaskCount++;
    }

    /**
     * Sets a new number of maximum OTHER tasks.
     *
     * @param maxOthersTaskCount New number of maximum OTHER tasks.
     */
    public void setMaxOthersTaskCount(int maxOthersTaskCount) {
        this.maxOthersTaskCount = maxOthersTaskCount;
    }

    /**
     * Returns the maximum number of OTHER tasks.
     *
     * @return The maximum number of OTHER tasks.
     */
    public int getMaxOthersTaskCount() {
        return this.maxOthersTaskCount;
    }

    /**
     * Returns the current number of OTHER tasks.
     *
     * @return The current number of OTHER tasks.
     */
    public int getUsedOthersTaskCount() {
        return this.usedOthersTaskCount;
    }

    /**
     * Decreases the current number of OTHER tasks.
     */
    private void decreaseUsedOthersTaskCount() {
        this.usedOthersTaskCount--;
    }

    /**
     * Increases the current number of OTHER tasks.
     */
    private void increaseUsedOthersTaskCount() {
        this.usedOthersTaskCount++;
    }

    @Override
    public void resetUsedTaskCounts() {
        super.resetUsedTaskCounts();
        this.usedCPUtaskCount = 0;
        this.usedGPUtaskCount = 0;
        this.usedFPGAtaskCount = 0;
        this.usedOthersTaskCount = 0;
    }

    @Override
    public Integer simultaneousCapacity(Implementation impl) {
        return Math.min(super.simultaneousCapacity(impl), this.getMaxCPUTaskCount());
    }

    @Override
    public ResourceType getType() {
        return ResourceType.WORKER;
    }

    @Override
    public String getMonitoringData(String prefix) {
        // TODO: Add full information about description (mem type, each processor information, etc)
        StringBuilder sb = new StringBuilder();
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

    /**
     * Returns the description value.
     *
     * @return The description value.
     */
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
                return 1;
            case WORKER:
                MethodWorker w = (MethodWorker) t;
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

    @Override
    public boolean canRun(Implementation implementation) {
        if (this.isLost()) {
            return false;
        }
        switch (implementation.getTaskType()) {
            case METHOD:
                MethodResourceDescription ctrs = (MethodResourceDescription) implementation.getRequirements();
                return this.description.contains(ctrs);
            default:
                return false;
        }
    }

    @Override
    public boolean canRunNow(MethodResourceDescription consumption) {
        boolean canRun = super.canRunNow(consumption);

        // Available slots
        canRun = canRun && (this.getUsedCPUTaskCount() < this.getMaxCPUTaskCount() || !consumption.containsCPU());
        canRun = canRun && ((this.getUsedGPUTaskCount() < this.getMaxGPUTaskCount()) || !consumption.containsGPU());
        canRun = canRun && ((this.getUsedFPGATaskCount() < this.getMaxFPGATaskCount()) || !consumption.containsFPGA());
        canRun =
            canRun && ((this.getUsedOthersTaskCount() < this.getMaxOthersTaskCount()) || !consumption.containsOthers());
        canRun = canRun && this.hasAvailable(consumption);
        return canRun;
    }

    @Override
    public void endTask(MethodResourceDescription consumption) {
        if (DEBUG) {
            LOGGER.debug(
                "End task received. Releasing resource " + consumption.getDynamicDescription() + " on " + getName());
        }
        if (consumption.containsCPU()) {
            this.decreaseUsedCPUTaskCount();
        }
        if (consumption.containsGPU()) {
            this.decreaseUsedGPUTaskCount();
        }
        if (consumption.containsFPGA()) {
            this.decreaseUsedFPGATaskCount();
        }
        if (consumption.containsOthers()) {
            this.decreaseUsedOthersTaskCount();
        }
        super.endTask(consumption);
    }

    @Override
    public MethodResourceDescription runTask(MethodResourceDescription consumption) {
        MethodResourceDescription reserved = super.runTask(consumption);
        if (DEBUG) {
            LOGGER.debug(
                "Run task received. Reserving resource " + consumption.getDynamicDescription() + " on " + getName());
        }
        if (reserved != null) {
            // Consumption can be hosted
            if (consumption.containsCPU()) {
                this.increaseUsedCPUTaskCount();
            }
            if (consumption.containsGPU()) {
                this.increaseUsedGPUTaskCount();
            }
            if (consumption.containsFPGA()) {
                this.increaseUsedFPGATaskCount();
            }
            if (consumption.containsOthers()) {
                this.increaseUsedOthersTaskCount();
            }
            return reserved;
        }
        return reserved;
    }

    @Override
    public void idleReservedResourcesDetected(ResourceDescription resources) {
        // Should notify the resource user that such resources are available again
        ResourceManager.notifyIdleResources((MethodWorker) this, (MethodResourceDescription) resources);
    }

    @Override
    public void reactivatedReservedResourcesDetected(ResourceDescription resources) {
        // Should notify the resource user that such resources are no longer available
        ResourceManager.notifyResourcesReacquisition((MethodWorker) this, (MethodResourceDescription) resources);
    }

    @Override
    public String getResourceLinks(String prefix) {
        StringBuilder sb = new StringBuilder(super.getResourceLinks(prefix));
        sb.append(prefix).append("TYPE = WORKER").append("\n");
        sb.append(prefix).append("CPU_COMPUTING_UNITS = ").append(this.description.getTotalCPUComputingUnits())
            .append("\n");
        sb.append(prefix).append("GPU_COMPUTING_UNITS = ").append(this.description.getTotalGPUComputingUnits())
            .append("\n");
        sb.append(prefix).append("FPGA_COMPUTING_UNITS = ").append(this.description.getTotalFPGAComputingUnits())
            .append("\n");
        sb.append(prefix).append("OTHER_COMPUTING_UNITS = ").append(this.description.getTotalFPGAComputingUnits())
            .append("\n");
        sb.append(prefix).append("MEMORY = ").append(this.description.getMemorySize()).append("\n");
        return sb.toString();
    }

    @Override
    public MethodWorker getSchedulingCopy() {
        return new MethodWorker(this);
    }

    @Override
    public String toString() {
        return "Worker " + this.name + " with usedTaskCount = " + this.usedCPUtaskCount + " and maxTaskCount = "
            + this.maxCPUtaskCount + " with the following description " + this.description;
    }
}
