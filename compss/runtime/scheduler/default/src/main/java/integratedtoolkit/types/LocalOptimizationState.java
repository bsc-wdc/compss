package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.ResourceDescription;
import java.util.LinkedList;

public class LocalOptimizationState {

    private final long updateId;

    private final LinkedList<Gap> gaps = new LinkedList<Gap>();

    private AllocatableAction topAction = null;
    private ResourceDescription topMissingResources;
    private long topStartTime;

    public LocalOptimizationState(long updateId, ResourceDescription rd) {
        this.updateId = updateId;
        Gap g = new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, rd.copy(), 0);
        gaps.add(g);
    }

    public long getId() {
        return updateId;
    }

    public void setTopAction(AllocatableAction action) {
        topAction = action;
        if (topAction != null) {
            topMissingResources = topAction.getAssignedImplementation().getRequirements().copy();
            //Check if the new peek can run in the already freed resources.
            for (Gap gap : gaps) {
                updatedResources(gap);
                if (topMissingResources.isDynamicUseless()) {
                    break;
                }
            }
        } else {
            topMissingResources = null;
            topStartTime = 0l;
        }
    }

    public void updatedResources(Gap gap) {
        if (topMissingResources != null) {
            ResourceDescription empty = gap.getResources().copy();
            topStartTime = gap.getInitialTime();
            ResourceDescription.reduceCommonDynamics(empty, topMissingResources);
        }
    }

    public AllocatableAction getTopAction() {
        return topAction;
    }

    public long getTopStartTime() {
        return Math.max(topStartTime, ((DefaultSchedulingInformation) topAction.getSchedulingInfo()).getExpectedStart());
    }

    public boolean canTopRun() {
        if (topMissingResources != null) {
            return topMissingResources.isDynamicUseless();
        } else {
            return false;
        }
    }

    public boolean areGaps() {
        return !gaps.isEmpty();
    }

    public void addGap(Gap g) {
        gaps.add(g);
    }

    public Gap peekFirstGap() {
        return gaps.peekFirst();
    }

    public void pollGap() {
        gaps.removeFirst();
    }

    public LinkedList<Gap> getGaps() {
        return gaps;
    }

}
