package integratedtoolkit.scheduler.multiobjective.types;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.ResourceDescription;

public class Gap {

    private final long initialTime;
    private long endTime;
    private final AllocatableAction origin;
    private final ResourceDescription resources;
    private final int capacity;

    public Gap(long start, long endTime, AllocatableAction origin, ResourceDescription resources, int capacity) {
        this.initialTime = start;
        this.endTime = endTime;
        this.origin = origin;
        this.resources = resources;
        this.capacity = capacity;
    }

    public long getInitialTime() {
        return initialTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public AllocatableAction getOrigin() {
        return origin;
    }

    public ResourceDescription getResources() {
        return resources;
    }

    public void setEndTime(long endTime) {
        this.endTime = endTime;
    }

    public int getCapacity() {
        return capacity;
    }

    @Override
    public String toString() {
        return "<" + initialTime + "->" + endTime + ", " + origin + ", " + resources.getDynamicDescription() + ", with " + capacity + " slots >";
    }
}
