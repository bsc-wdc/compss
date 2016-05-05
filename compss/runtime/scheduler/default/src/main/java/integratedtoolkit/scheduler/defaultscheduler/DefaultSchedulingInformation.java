package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.LinkedList;

public class DefaultSchedulingInformation<P extends Profile, T extends WorkerResourceDescription> extends SchedulingInformation<P,T> {

    private long lastUpdate;
    private long expectedStart;
    private long expectedEnd;

    //Allocatable actions that the action depends on due to resource availability
    private final LinkedList<AllocatableAction<P,T>> resourcePredecessors;

    //Allocatable actions depending on the allocatable action due to resource availability
    private final LinkedList<AllocatableAction<P,T>> resourceSuccessors;

    public DefaultSchedulingInformation() {
        resourcePredecessors = new LinkedList<AllocatableAction<P,T>>();
        resourceSuccessors = new LinkedList<AllocatableAction<P,T>>();

        lastUpdate = System.currentTimeMillis();
        expectedStart = 0;
        expectedEnd = 0;
    }

    public void addPredecessor(AllocatableAction<P,T> predecessor) {
        resourcePredecessors.add(predecessor);
    }

    public boolean hasPredecessors() {
        return !resourcePredecessors.isEmpty();
    }

    @Override
    public final boolean isExecutable() {
        return resourcePredecessors.isEmpty();
    }

    public LinkedList<AllocatableAction<P,T>> getPredecessors() {
        return resourcePredecessors;
    }

    public void removePredecessor(AllocatableAction<P,T> successor) {
        resourcePredecessors.remove(successor);
    }

    public void clearPredecessors() {
        resourcePredecessors.clear();
    }

    public void addSuccessor(AllocatableAction<P,T> successor) {
        resourceSuccessors.add(successor);
    }

    public LinkedList<AllocatableAction<P,T>> getSuccessors() {
        return resourceSuccessors;
    }

    public void removeSuccessor(AllocatableAction<P,T> successor) {
        resourceSuccessors.remove(successor);
    }

    public void clearSuccessors() {
        resourceSuccessors.clear();
    }

    public void setExpectedStart(long expectedStart) {
        this.expectedStart = expectedStart;
    }

    public long getExpectedStart() {
        return expectedStart;
    }

    public void setExpectedEnd(long expectedEnd) {
        this.expectedEnd = expectedEnd;
    }

    public long getExpectedEnd() {
        return expectedEnd;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    public long getLastUpdate() {
        return lastUpdate;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("lastUpdate: " + lastUpdate + " expectedStart: " + expectedStart + " expectedEnd:" + expectedEnd);
        sb.append("\t").append("schedPredecessors: ");
        for (AllocatableAction<P,T> aa : getPredecessors()) {
            sb.append(" ").append(aa.hashCode());
        }
        sb.append("\n");
        sb.append("\t").append("schedSuccessors: ");
        for (AllocatableAction<P,T> aa : getSuccessors()) {
            sb.append(" ").append(aa.hashCode());
        }
        return sb.toString();
    }

}
