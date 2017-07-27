package es.bsc.compss.scheduler.types;

import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;


public class LocalOptimizationState<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    private final long updateId;

    private final LinkedList<Gap<P, T, I>> gaps = new LinkedList<>();

    private AllocatableAction<P, T, I> action = null;
    private ResourceDescription missingResources;
    private long topStartTime;


    public LocalOptimizationState(long updateId, ResourceDescription rd) {
        this.updateId = updateId;
        Gap<P, T, I> g = new Gap<>(0, Long.MAX_VALUE, null, rd.copy(), 0);
        gaps.add(g);
    }

    public long getId() {
        return updateId;
    }

    public LinkedList<Gap<P, T, I>> reserveResources(ResourceDescription resources, long startTime) {

        LinkedList<Gap<P, T, I>> previousGaps = new LinkedList<>();
        // Remove requirements from resource description
        ResourceDescription requirements = resources.copy();
        Iterator<Gap<P, T, I>> gapIt = gaps.iterator();
        while (gapIt.hasNext() && !requirements.isDynamicUseless()) {
            Gap<P,T,I> g = gapIt.next();
            if (checkGapForReserve(g, requirements, startTime, previousGaps)) {
                gapIt.remove();
            }
        }

        return previousGaps;
    }

    private boolean checkGapForReserve(Gap<P, T, I> g, ResourceDescription requirements, long reserveStart,
            LinkedList<Gap<P, T, I>> previousGaps) {
        boolean remove = false;
        AllocatableAction<P, T, I> gapAction = g.getOrigin();
        ResourceDescription rd = g.getResources();
        ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(rd, requirements);
        Gap<P, T, I> tmpGap = new Gap<>(g.getInitialTime(), reserveStart, g.getOrigin(), reduction, 0);
        previousGaps.add(tmpGap);

        if (gapAction != null) {
            FullGraphSchedulingInformation<P, T, I> gapDSI = (FullGraphSchedulingInformation<P, T, I>) gapAction.getSchedulingInfo();
            // Remove resources from the first gap
            gapDSI.addGap();
        }

        // If the gap has been fully used
        if (rd.isDynamicUseless()) {
            // Remove the gap
            remove = true;
            if (gapAction != null) {
                FullGraphSchedulingInformation<P, T, I> gapDSI = (FullGraphSchedulingInformation<P, T, I>) gapAction.getSchedulingInfo();
                gapDSI.removeGap();
            }
        }

        return remove;
    }

    public void releaseResources(long expectedStart, AllocatableAction<P, T, I> action) {
        Gap<P, T, I> gap = new Gap<>(expectedStart, Long.MAX_VALUE, action, action.getAssignedImplementation().getRequirements(), 0);
        FullGraphSchedulingInformation<P, T, I> dsi = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
        dsi.addGap();
        gaps.add(gap);
        if (missingResources != null) {
            ResourceDescription empty = gap.getResources().copy();
            topStartTime = gap.getInitialTime();
            ResourceDescription.reduceCommonDynamics(empty, missingResources);
        }
    }

    public void replaceAction(AllocatableAction<P, T, I> action) {
        this.action = action;
        if (this.action != null) {
            missingResources = this.action.getAssignedImplementation().getRequirements().copy();
            // Check if the new peek can run in the already freed resources.
            for (Gap<P, T, I> gap : gaps) {
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

    public void addTmpGap(Gap<P, T, I> g) {
        AllocatableAction<P, T, I> gapAction = g.getOrigin();
        FullGraphSchedulingInformation<P, T, I> gapDSI = (FullGraphSchedulingInformation<P, T, I>) gapAction.getSchedulingInfo();
        gapDSI.addGap();
    }

    public void replaceTmpGap(Gap<P, T, I> gap, Gap<P, T, I> previousGap) {

    }

    public void removeTmpGap(Gap<P, T, I> g) {
        AllocatableAction<P, T, I> gapAction = g.getOrigin();
        if (gapAction != null) {
            FullGraphSchedulingInformation<P, T, I> gapDSI = (FullGraphSchedulingInformation<P, T, I>) gapAction.getSchedulingInfo();
            gapDSI.removeGap();
            if (!gapDSI.hasGaps()) {
                gapDSI.unlock();
            }
        }
    }

    public AllocatableAction<P, T, I> getAction() {
        return action;
    }

    public long getActionStartTime() {
        return Math.max(topStartTime, ((FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo()).getExpectedStart());
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

    public Gap<P, T, I> peekFirstGap() {
        return gaps.peekFirst();
    }

    public void pollGap() {
        gaps.removeFirst();
    }

    public LinkedList<Gap<P, T, I>> getGaps() {
        return gaps;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Optimization State at " + updateId + "\n");
        sb.append("\tGaps:\n");
        for (Gap<P, T, I> gap : gaps) {
            sb.append("\t\t").append(gap).append("\n");
        }
        sb.append("\tTopAction:").append(action).append("\n");
        sb.append("\tMissing To Run:").append(missingResources).append("\n");
        sb.append("\tExpected Start:").append(topStartTime).append("\n");
        return sb.toString();
    }

}
