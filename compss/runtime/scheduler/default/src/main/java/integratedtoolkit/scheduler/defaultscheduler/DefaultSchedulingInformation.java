package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.SchedulingInformation;
import java.util.LinkedList;

public class DefaultSchedulingInformation extends SchedulingInformation {

    private long lastUpdate;
    private long expectedStart;
    private long expectedEnd;

    //Allocatable actions that the action depends on due to resource availability
    private final LinkedList<AllocatableAction> resourcePredecessors;

    //Allocatable actions depending on the allocatable action due to resource availability
    private final LinkedList<AllocatableAction> resourceSuccessors;

    public DefaultSchedulingInformation() {
        resourcePredecessors = new LinkedList();
        resourceSuccessors = new LinkedList();

        lastUpdate = System.currentTimeMillis();
        expectedStart = 0;
        expectedEnd = 0;
    }

    public void addPredecessor(AllocatableAction predecessor) {
        resourcePredecessors.add(predecessor);
    }

    public boolean hasPredecessors() {
        return !resourcePredecessors.isEmpty();
    }

    @Override
    public final boolean isExecutable() {
        return resourcePredecessors.isEmpty();
    }

    public LinkedList<AllocatableAction> getPredecessors() {
        return resourcePredecessors;
    }

    public void removePredecessor(AllocatableAction successor) {
        resourcePredecessors.remove(successor);
    }

    public void clearPredecessors() {
        resourcePredecessors.clear();
    }

    public void addSuccessor(AllocatableAction successor) {
        resourceSuccessors.add(successor);
    }

    public LinkedList<AllocatableAction> getSuccessors() {
        return resourceSuccessors;
    }

    public void removeSuccessor(AllocatableAction successor) {
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
        sb.append("\tschedPredecessors: ");
        for (AllocatableAction aa : getPredecessors()) {
            sb.append(" ").append(aa.hashCode());
        }
        sb.append("\n");
        sb.append("\tschedSuccessors: ");
        for (AllocatableAction aa : getSuccessors()) {
            sb.append(" ").append(aa.hashCode());
        }
        return sb.toString();
    }

}
