package integratedtoolkit.types.resources;

import java.util.Map;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.implementations.Implementation.TaskType;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;

public class MethodWorker extends Worker<MethodResourceDescription> {

    private String name;

    // Available resource capabilities
    protected final MethodResourceDescription available;

    // Task count
    private int usedCPUTaskCount = 0;
    private final int maxCPUTaskCount;
    private int usedGPUTaskCount = 0;
    private final int maxGPUTaskCount;
    private int usedFPGATaskCount = 0;
    private final int maxFPGATaskCount;
    private int usedOthersTaskCount = 0;
    private final int maxOthersTaskCount;

    public MethodWorker(String name, MethodResourceDescription description, COMPSsWorker worker, int limitOfTasks,
            Map<String, String> sharedDisks) {

        super(name, description, worker, limitOfTasks, sharedDisks);
        this.name = name;
        available = new MethodResourceDescription(description);

        this.maxCPUTaskCount = limitOfTasks;
        this.maxGPUTaskCount = 0;
        this.maxFPGATaskCount = 0;
        this.maxOthersTaskCount = 0;
    }

    public MethodWorker(String name, MethodResourceDescription description, MethodConfiguration conf, Map<String, String> sharedDisks) {

        super(name, description, conf, sharedDisks);
        this.name = name;
        this.available = new MethodResourceDescription(description); // clone
        this.maxCPUTaskCount = conf.getLimitOfTasks();
        this.maxGPUTaskCount = conf.getLimitOfGPUTasks();
        this.maxFPGATaskCount = conf.getLimitOfFPGATasks();
        this.maxOthersTaskCount = conf.getLimitOfOTHERSTasks();
    }

    public MethodWorker(MethodWorker mw) {
        super(mw);
        this.available = mw.available.copy();

        this.maxCPUTaskCount = mw.maxCPUTaskCount;
        this.usedCPUTaskCount = mw.usedCPUTaskCount;
        this.maxGPUTaskCount = mw.maxGPUTaskCount;
        this.usedGPUTaskCount = mw.usedGPUTaskCount;
        this.maxFPGATaskCount = mw.maxFPGATaskCount;
        this.usedFPGATaskCount = mw.usedFPGATaskCount;
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

    public MethodResourceDescription getAvailable() {
        return this.available;
    }

    @Override
    public MethodResourceDescription reserveResource(MethodResourceDescription consumption) {
        synchronized (available) {
            if (this.hasAvailable(consumption)) {
                return (MethodResourceDescription) available.reduceDynamic(consumption);
            } else {
                return null;
            }
        }
    }

    @Override
    public void releaseResource(MethodResourceDescription consumption) {
        synchronized (available) {
            available.increaseDynamic(consumption);
        }
    }

    @Override
    public void releaseAllResources() {
        synchronized (available) {
            super.resetUsedTaskCounts();
            available.reduceDynamic(available);
            available.increaseDynamic(description);
        }
    }

    @Override
    public Integer fitCount(Implementation impl) {
        if (impl.getTaskType() == TaskType.SERVICE) {
            return null;
        }
        MethodResourceDescription ctrs = (MethodResourceDescription) impl.getRequirements();
        return description.canHostSimultaneously(ctrs);
    }

    @Override
    public boolean hasAvailable(MethodResourceDescription consumption) {
        synchronized (available) {
            return available.containsDynamic(consumption);
        }
    }

    public int getMaxCPUTaskCount() {
        return this.maxCPUTaskCount;
    }

    public int getUsedCPUTaskCount() {
        return this.usedCPUTaskCount;
    }

    public int getMaxGPUTaskCount() {
        return this.maxGPUTaskCount;
    }

    public int getUsedGPUTaskCount() {
        return this.usedGPUTaskCount;
    }

    public int getMaxFPGATaskCount() {
        return this.maxFPGATaskCount;
    }

    public int getUsedFPGATaskCount() {
        return this.usedFPGATaskCount;
    }

    public int getMaxOthersTaskCount() {
        return this.maxOthersTaskCount;
    }

    public int getUsedOthersTaskCount() {
        return this.usedOthersTaskCount;
    }

    private void decreaseUsedCPUTaskCount() {
        this.usedCPUTaskCount--;
    }

    private void increaseUsedCPUTaskCount() {
        this.usedCPUTaskCount++;
    }

    private void decreaseUsedGPUTaskCount() {
        this.usedGPUTaskCount--;
    }

    private void increaseUsedGPUTaskCount() {
        this.usedGPUTaskCount++;
    }

    private void decreaseUsedFPGATaskCount() {
        this.usedFPGATaskCount--;
    }

    private void increaseUsedFPGATaskCount() {
        this.usedFPGATaskCount++;
    }

    private void decreaseUsedOthersTaskCount() {
        this.usedOthersTaskCount--;
    }

    private void increaseUsedOthersTaskCount() {
        this.usedOthersTaskCount++;
    }

    @Override
    public void resetUsedTaskCounts() {
        super.resetUsedTaskCounts();
        usedCPUTaskCount = 0;
        usedGPUTaskCount = 0;
        usedFPGATaskCount = 0;
        usedOthersTaskCount = 0;
    }

    @Override
    public Integer simultaneousCapacity(Implementation impl) {
        return Math.min(super.simultaneousCapacity(impl), this.getMaxCPUTaskCount());
    }

    @Override
    public Type getType() {
        return Type.WORKER;
    }

    @Override
    public String getMonitoringData(String prefix) {
        // TODO: Add full information about description (mem type, each processor information, etc)
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("<TotalCPUComputingUnits>").append(description.getTotalCPUComputingUnits())
                .append("</TotalCPUComputingUnits>").append("\n");
        sb.append(prefix).append("<TotalGPUComputingUnits>").append(description.getTotalGPUComputingUnits())
                .append("</TotalGPUComputingUnits>").append("\n");
        sb.append(prefix).append("<TotalFPGAComputingUnits>").append(description.getTotalFPGAComputingUnits())
                .append("</TotalFPGAComputingUnits>").append("\n");
        sb.append(prefix).append("<TotalOTHERComputingUnits>").append(description.getTotalOTHERComputingUnits())
                .append("</TotalOTHERComputingUnits>").append("\n");
        sb.append(prefix).append("<Memory>").append(description.getMemorySize()).append("</Memory>").append("\n");
        sb.append(prefix).append("<Disk>").append(description.getStorageSize()).append("</Disk>").append("\n");
        return sb.toString();
    }

    private Float getValue() {
        return description.value;
    }

    @Override
    public int compareTo(Resource t) {
        if (t == null) {
            throw new NullPointerException();
        }
        switch (t.getType()) {
            case SERVICE:
                return 1;
            case WORKER:
                MethodWorker w = (MethodWorker) t;
                if (description.getValue() == null) {
                    if (w.getValue() == null) {
                        return w.getName().compareTo(getName());
                    }
                    return 1;
                }
                if (w.getValue() == null) {
                    return -1;
                }
                float dif = w.getValue() - description.getValue();
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
        switch (implementation.getTaskType()) {
            case METHOD:
                MethodResourceDescription ctrs = (MethodResourceDescription) implementation.getRequirements();
                return description.contains(ctrs);
            default:
                return false;
        }
    }

    @Override
    public boolean canRunNow(MethodResourceDescription consumption) {
        boolean canRun = super.canRunNow(consumption);

        // Available slots
        canRun = canRun && this.getUsedCPUTaskCount() < this.getMaxCPUTaskCount() || !consumption.containsCPU();
        canRun = canRun && ((this.getUsedGPUTaskCount() < this.getMaxGPUTaskCount()) || !consumption.containsGPU());
        canRun = canRun && ((this.getUsedFPGATaskCount() < this.getMaxFPGATaskCount()) || !consumption.containsFPGA());
        canRun = canRun && ((this.getUsedOthersTaskCount() < this.getMaxOthersTaskCount()) || !consumption.containsOthers());
        canRun = canRun && this.hasAvailable(consumption);
        return canRun;
    }

    @Override
    public void endTask(MethodResourceDescription consumption) {
        if (DEBUG) {
            LOGGER.debug("End task received. Releasing resource " + getName());
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
    public String getResourceLinks(String prefix) {
        StringBuilder sb = new StringBuilder(super.getResourceLinks(prefix));
        sb.append(prefix).append("TYPE = WORKER").append("\n");
        sb.append(prefix).append("CPU_COMPUTING_UNITS = ").append(description.getTotalCPUComputingUnits()).append("\n");
        sb.append(prefix).append("GPU_COMPUTING_UNITS = ").append(description.getTotalGPUComputingUnits()).append("\n");
        sb.append(prefix).append("FPGA_COMPUTING_UNITS = ").append(description.getTotalFPGAComputingUnits()).append("\n");
        sb.append(prefix).append("OTHER_COMPUTING_UNITS = ").append(description.getTotalFPGAComputingUnits()).append("\n");
        sb.append(prefix).append("MEMORY = ").append(description.getMemorySize()).append("\n");
        return sb.toString();
    }

    @Override
    public MethodWorker getSchedulingCopy() {
        return new MethodWorker(this);
    }

    @Override
    public String toString() {
        return "Worker " + description + " with usedTaskCount = " + usedCPUTaskCount + " and maxTaskCount = " + maxCPUTaskCount
                + " with the following description " + description;
    }
}
