package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.ResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;


public class LocalOptimizationState {

    private final long updateId;

    private final LinkedList<Gap> gaps = new LinkedList<>();

    private AllocatableAction action = null;
    private ResourceDescription missingResources;
    private long topStartTime;


    public LocalOptimizationState(long updateId, ResourceDescription rd) {
        this.updateId = updateId;
        Gap g = new Gap(0, Long.MAX_VALUE, null, rd.copy(), 0);
        gaps.add(g);
    }

    public long getId() {
        return updateId;
    }

    public LinkedList<Gap> reserveResources(ResourceDescription resources, long startTime) {

        LinkedList<Gap> previousGaps = new LinkedList<>();
        // Remove requirements from resource description
        ResourceDescription requirements = resources.copy();
        Iterator<Gap> gapIt = gaps.iterator();
        while (gapIt.hasNext() && !requirements.isDynamicUseless()) {
            Gap g = gapIt.next();
            if (checkGapForReserve(g, requirements, startTime, previousGaps)) {
                gapIt.remove();
            }
        }

        return previousGaps;
    }

    private boolean checkGapForReserve(Gap g, ResourceDescription requirements, long reserveStart, LinkedList<Gap> previousGaps) {
        boolean remove = false;
        AllocatableAction gapAction = g.getOrigin();
        ResourceDescription rd = g.getResources();
        ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(rd, requirements);
        Gap tmpGap = new Gap(g.getInitialTime(), reserveStart, g.getOrigin(), reduction, 0);
        previousGaps.add(tmpGap);

        if (gapAction != null) {
            FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
            // Remove resources from the first gap
            gapDSI.addGap();
        }

        // If the gap has been fully used
        if (rd.isDynamicUseless()) {
            // Remove the gap
            remove = true;
            if (gapAction != null) {
                FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
                gapDSI.removeGap();
            }
        }

        return remove;
    }

    public void releaseResources(long expectedStart, AllocatableAction action) {
        Gap gap = new Gap(expectedStart, Long.MAX_VALUE, action, action.getAssignedImplementation().getRequirements(), 0);
        FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        dsi.addGap();
        gaps.add(gap);
        if (missingResources != null) {
            ResourceDescription empty = gap.getResources().copy();
            topStartTime = gap.getInitialTime();
            ResourceDescription.reduceCommonDynamics(empty, missingResources);
        }
    }

    public void replaceAction(AllocatableAction action) {
        this.action = action;
        if (this.action != null) {
            missingResources = this.action.getAssignedImplementation().getRequirements().copy();
            // Check if the new peek can run in the already freed resources.
            for (Gap gap : gaps) {
                ResourceDescription empty = gap.getResources().copy();
                topStartTime = gap.getInitialTime();
                ResourceDescription.reduceCommonDynamics(empty, missingResources);
                if (missingResources.isDynamicUseless()) {
                    break;
                }
            }
        } else {
            missingResources = null;
            topStartTime = 0l;
        }
    }

    public void addTmpGap(Gap g) {
        AllocatableAction gapAction = g.getOrigin();
        FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
        gapDSI.addGap();
    }

    public void replaceTmpGap(Gap gap, Gap previousGap) {

    }

    public void removeTmpGap(Gap g) {
        AllocatableAction gapAction = g.getOrigin();
        if (gapAction != null) {
            FullGraphSchedulingInformation gapDSI = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
            gapDSI.removeGap();
            if (!gapDSI.hasGaps()) {
                gapDSI.unlock();
            }
        }
    }

    public AllocatableAction getAction() {
        return action;
    }

    public long getActionStartTime() {
        return Math.max(topStartTime, ((FullGraphSchedulingInformation) action.getSchedulingInfo()).getExpectedStart());
    }

    public boolean canActionRun() {
        if (missingResources != null) {
            return missingResources.isDynamicUseless();
        } else {
            return false;
        }
    }

    public boolean areGaps() {
        return !gaps.isEmpty();
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Optimization State at " + updateId + "\n");
        sb.append("\tGaps:\n");
        for (Gap gap : gaps) {
            sb.append("\t\t").append(gap).append("\n");
        }
        sb.append("\tTopAction:").append(action).append("\n");
        sb.append("\tMissing To Run:").append(missingResources).append("\n");
        sb.append("\tExpected Start:").append(topStartTime).append("\n");
        return sb.toString();
    }

}
