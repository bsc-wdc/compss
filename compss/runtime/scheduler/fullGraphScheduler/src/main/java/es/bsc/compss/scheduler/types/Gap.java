package es.bsc.compss.scheduler.types;

import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;


public class Gap<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    private final long initialTime;
    private long endTime;
    private final AllocatableAction<P, T, I> origin;
    private final ResourceDescription resources;
    private final int capacity;


    public Gap(long start, AllocatableAction<P, T, I> origin, ResourceDescription resources, int capacity) {
        this.initialTime = start;
        this.origin = origin;
        this.resources = resources.copy();
        this.capacity = capacity;
    }

    public Gap(long start, long endTime, AllocatableAction<P, T, I> origin, ResourceDescription resources, int capacity) {
        this.initialTime = start;
        this.endTime = endTime;
        this.origin = origin;
        this.resources = resources.copy();
        this.capacity = capacity;
    }

    public long getInitialTime() {
        return initialTime;
    }

    public long getEndTime() {
        return endTime;
    }

    public AllocatableAction<P, T, I> getOrigin() {
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
        return "<" + initialTime + "->" + endTime + ", " + origin + ", " + resources + ", amb " + capacity + " slots >";
    }

}
