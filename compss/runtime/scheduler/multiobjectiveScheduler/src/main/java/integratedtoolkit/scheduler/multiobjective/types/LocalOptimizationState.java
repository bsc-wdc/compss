package integratedtoolkit.scheduler.multiobjective.types;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.multiobjective.MOSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;


public class LocalOptimizationState {

    private final long updateId;
    private final ResourceScheduler<WorkerResourceDescription> worker;

    private final LinkedList<Gap> gaps = new LinkedList<>();
    private double runningCost;
    private double totalCost;
    private double runningEnergy;
    private double totalEnergy;

    private AllocatableAction action = null;
    private ResourceDescription missingResources;
    private long topStartTime;
    private int[][] implementationCount;
    private int[][] runningImplementationsCount;
    private long endRunningActions;

    // Actions considered to be running
    private final List<AllocatableAction> runningActions;

    // Actions not depending on other actions scheduled on the same resource
    // Sorted by data dependencies release
    private final PriorityQueue<AllocatableAction> readyActions;
    // Actions that can be selected to be scheduled on the node
    // Sorted by data dependencies release
    private final PriorityActionSet selectableActions;

    private AllocatableAction resourceBlockingAction;
    private AllocatableAction dataBlockingAction;


    public LocalOptimizationState(long updateId, ResourceScheduler<WorkerResourceDescription> rs,
            Comparator<AllocatableAction> readyComparator, Comparator<AllocatableAction> selectionComparator) {

        this.updateId = updateId;
        this.worker = rs;
        ResourceDescription rd = rs.getResource().getDescription();
        totalCost = 0;
        runningCost = 0;
        totalEnergy = 0;
        runningEnergy = 0;
        Gap g = new Gap(0, Long.MAX_VALUE, null, rd.copy(), 0);
        gaps.add(g);
        implementationCount = new int[CoreManager.getCoreCount()][];
        runningImplementationsCount = new int[CoreManager.getCoreCount()][];
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            implementationCount[coreId] = new int[CoreManager.getCoreImplementations(coreId).size()];
            runningImplementationsCount[coreId] = new int[CoreManager.getCoreImplementations(coreId).size()];
        }
        endRunningActions = 0;
        resourceBlockingAction = new OptimizationAction();
        resourceBlockingAction.assignResource(rs);
        dataBlockingAction = new OptimizationAction();
        dataBlockingAction.assignResource(rs);

        runningActions = new LinkedList<AllocatableAction>();
        readyActions = new PriorityQueue<AllocatableAction>(1, readyComparator);
        selectableActions = new PriorityActionSet(selectionComparator);
    }

    public long getId() {
        return updateId;
    }

    public List<Gap> reserveResources(ResourceDescription resources, long startTime) {
        List<Gap> previousGaps = new LinkedList<>();
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

    private boolean checkGapForReserve(Gap g, ResourceDescription requirements, long reserveStart, List<Gap> previousGaps) {
        boolean remove = false;
        AllocatableAction gapAction = g.getOrigin();
        ResourceDescription rd = g.getResources();
        ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(rd, requirements);
        if (!reduction.isDynamicUseless()) {
            Gap tmpGap = new Gap(g.getInitialTime(), reserveStart, g.getOrigin(), reduction, 0);
            previousGaps.add(tmpGap);

            if (gapAction != null) {
                MOSchedulingInformation gapDSI = (MOSchedulingInformation) gapAction.getSchedulingInfo();
                // Remove resources from the first gap
                gapDSI.addGap();
            }

            // If the gap has been fully used
            if (rd.isDynamicUseless()) {
                // Remove the gap
                remove = true;
                if (gapAction != null) {
                    MOSchedulingInformation gapDSI = (MOSchedulingInformation) gapAction.getSchedulingInfo();
                    gapDSI.removeGap();
                }
            }
        }
        return remove;
    }

    public void releaseResources(long expectedStart, AllocatableAction action) {
        if (action.getAssignedImplementation() != null) {
            Gap gap;
            gap = new Gap(expectedStart, Long.MAX_VALUE, action, action.getAssignedImplementation().getRequirements().copy(), 0);
            MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
            dsi.addGap();
            gaps.add(gap);
            if (missingResources != null) {
                ResourceDescription empty = gap.getResources().copy();
                topStartTime = gap.getInitialTime();
                ResourceDescription.reduceCommonDynamics(empty, missingResources);
            }
        } else {
            System.out.println("**** Action has null implementation. Nothing done at release resources *** ");
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
        MOSchedulingInformation gapDSI = (MOSchedulingInformation) gapAction.getSchedulingInfo();
        gapDSI.addGap();
    }

    public void replaceTmpGap(Gap gap, Gap previousGap) {

    }

    public void removeTmpGap(Gap g) {
        AllocatableAction gapAction = g.getOrigin();
        if (gapAction != null) {
            MOSchedulingInformation gapDSI = (MOSchedulingInformation) gapAction.getSchedulingInfo();
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
        return Math.max(topStartTime, ((MOSchedulingInformation) action.getSchedulingInfo()).getExpectedStart());
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

    public List<Gap> getGaps() {
        return gaps;
    }

    public String toString() {
        StringBuilder sb = new StringBuilder("Optimization State at " + updateId + "\n");
        sb.append("\tGaps:\n");
        for (Gap gap : gaps) {
            sb.append("\t\t").append(gap).append("\n");
        }
        sb.append("\tTopAction:").append(action).append("\n");
        sb.append("\tMissing To Run:").append(missingResources).append("\n");
        sb.append("\tExpected Start:").append(topStartTime).append("\n");
        sb.append("\tPending Executions:\n");
        for (int coreId = 0; coreId < implementationCount.length; coreId++) {
            sb.append("\t\tCore " + coreId + ":\n");
            for (int implId = 0; implId < implementationCount[coreId].length; implId++) {
                sb.append("\t\t\tImplementation " + implId + ":" + implementationCount[coreId][implId] + "\n");
            }
        }
        return sb.toString();
    }

    public void runningAction(Implementation impl, MOProfile p, long pendingTime) {
        if (impl != null) {
            reserveResources(impl.getRequirements(), 0);
            if (impl.getCoreId() != null && impl.getImplementationId() != null) {
                runningImplementationsCount[impl.getCoreId()][impl.getImplementationId()]++;
                endRunningActions = Math.max(endRunningActions, pendingTime);
                double power = p.getPower();
                runningEnergy += pendingTime * power;
                runningCost += p.getPrice();
            }
        } else {
            System.out.println("**** Action has a null implementation. Nothing done for reserving resources ***");
        }
    }

    public long getEndRunningTime() {
        return endRunningActions;
    }

    public int[][] getRunningImplementations() {
        return runningImplementationsCount;
    }

    public double getRunningCost() {
        return this.runningCost;
    }

    public double getRunningEnergy() {
        return this.runningEnergy;
    }

    public AllocatableAction pollActionForGap(Gap gap) {
        AllocatableAction gapAction = null;
        PriorityQueue<AllocatableAction> peeks = selectableActions.peekAll();
        // Get Main action to fill the gap
        while (!peeks.isEmpty() && gapAction == null) {
            AllocatableAction candidate = peeks.poll();
            // Check times
            MOSchedulingInformation candidateDSI = (MOSchedulingInformation) candidate.getSchedulingInfo();
            long start = candidateDSI.getExpectedStart();
            if (start > gap.getEndTime()) {
                continue;
            }
            Implementation impl = candidate.getAssignedImplementation();
            Profile p = worker.getProfile(impl);
            long expectedLength = p.getAverageExecutionTime();
            if ((gap.getEndTime() - gap.getInitialTime()) < expectedLength) {
                continue;
            }
            if ((start + expectedLength) > gap.getEndTime()) {
                continue;
            }

            // Check description
            if (gap.getResources().canHostDynamic(impl)) {
                selectableActions.removeFirst(candidate.getCoreId());
                gapAction = candidate;
            }
        }
        return gapAction;
    }

    public void resourceBlockedAction(AllocatableAction action) {
        MOSchedulingInformation aDSI = (MOSchedulingInformation) action.getSchedulingInfo();
        MOSchedulingInformation rbaDSI = (MOSchedulingInformation) resourceBlockingAction.getSchedulingInfo();
        rbaDSI.lock();
        rbaDSI.addSuccessor(action);
        Gap opActionGap = new Gap(0, 0, resourceBlockingAction, action.getAssignedImplementation().getRequirements().copy(), 0);
        aDSI.addPredecessor(opActionGap);
        rbaDSI.unlock();
        updateConsumptions(action);
    }

    public void dataBlockedAction(AllocatableAction action) {
        MOSchedulingInformation aDSI = (MOSchedulingInformation) action.getSchedulingInfo();
        MOSchedulingInformation dbaDSI = (MOSchedulingInformation) dataBlockingAction.getSchedulingInfo();
        dbaDSI.lock();
        dbaDSI.addSuccessor(action);
        Gap opActionGap = new Gap(0, 0, dataBlockingAction, action.getAssignedImplementation().getRequirements().copy(), 0);
        aDSI.addPredecessor(opActionGap);
        dbaDSI.unlock();
        updateConsumptions(action);
    }

    public AllocatableAction getResourceBlockingAction() {
        return resourceBlockingAction;
    }

    public AllocatableAction getDataBlockingAction() {
        return dataBlockingAction;
    }

    public void classifyAction(AllocatableAction action, boolean hasInternal, boolean hasExternal, boolean hasResourcePredecessors,
            long startTime) {
        if (!hasInternal) { // Not needs to wait for some blocked action to end
            if (hasExternal) {
                if (startTime == 0) {
                    selectableActions.offer(action);
                } else if (startTime == Long.MAX_VALUE) {
                    dataBlockedAction(action);
                } else {
                    readyActions.add(action);
                }
            } else // has no dependencies
            {
                if (hasResourcePredecessors) {
                    selectableActions.offer(action);
                } else {
                    runningActions.add(action);
                }
            }
        }
    }

    public List<AllocatableAction> getRunningActions() {
        return runningActions;
    }

    public boolean areRunnableActions() {
        return !selectableActions.isEmpty();
    }

    public AllocatableAction getMostPrioritaryRunnableAction() {
        return selectableActions.peek();
    }

    public void removeMostPrioritaryRunnableAction() {
        selectableActions.poll();
    }

    public void removeMostPrioritaryRunnableAction(Integer coreId) {
        selectableActions.removeFirst(coreId);
    }

    public boolean areActionsToBeRescheduled() {
        return !readyActions.isEmpty();
    }

    public AllocatableAction getEarliestActionToBeRescheduled() {
        return readyActions.poll();
    }

    public void progressOnTime(long time) {
        while (readyActions.size() > 0) {
            AllocatableAction top = readyActions.peek();
            MOSchedulingInformation topDSI = (MOSchedulingInformation) top.getSchedulingInfo();
            long start = topDSI.getExpectedStart();
            if (start > time) {
                break;
            }
            readyActions.poll();
            selectableActions.offer(top);
        }
    }

    public void releaseDataSuccessors(MOSchedulingInformation dsi, long timeLimit) {
        List<AllocatableAction> successors = dsi.getOptimizingSuccessors();
        for (AllocatableAction successor : successors) {
            MOSchedulingInformation successorDSI = (MOSchedulingInformation) successor.getSchedulingInfo();
            int missingParams = 0;
            long startTime = 0;
            boolean retry = true;
            while (retry) {
                try {
                    List<AllocatableAction> predecessors = successor.getDataPredecessors();
                    for (AllocatableAction predecessor : predecessors) {
                        MOSchedulingInformation predDSI = ((MOSchedulingInformation) predecessor.getSchedulingInfo());
                        if (predecessor.getAssignedResource() != worker) {
                            startTime = Math.max(startTime, predDSI.getExpectedEnd());
                        } else if (predDSI.isOnOptimization()) {
                            missingParams++;
                        } else {
                            startTime = Math.max(startTime, predDSI.getExpectedEnd());
                        }
                    }
                    retry = false;
                } catch (ConcurrentModificationException cme) {
                    missingParams = 0;
                    startTime = 0;
                }
            }
            successorDSI.setExpectedStart(startTime);
            if (missingParams == 0) {
                if (successorDSI.getExpectedStart() <= timeLimit) {
                    selectableActions.offer(successor);
                } else {
                    readyActions.add(successor);
                }
            }
        }
        dsi.clearOptimizingSuccessors();
    }

    public void blockDataSuccessors(MOSchedulingInformation dsi) {
        List<AllocatableAction> successors = dsi.getOptimizingSuccessors();
        for (AllocatableAction successor : successors) {
            MOSchedulingInformation sucDSI = (MOSchedulingInformation) successor.getSchedulingInfo();
            sucDSI.lock();
            if (sucDSI.isOnOptimization()) {
                sucDSI.clearPredecessors();
                sucDSI.clearSuccessors();
                dataBlockedAction(successor);
                blockDataSuccessors(sucDSI);
                sucDSI.setExpectedStart(Long.MAX_VALUE);
                sucDSI.setExpectedEnd(Long.MAX_VALUE);
                sucDSI.setOnOptimization(false);
            }
            sucDSI.unlock();
        }
    }

    // CONSUMPTIONS
    public void updateConsumptions(AllocatableAction action) {
        Implementation impl = action.getAssignedImplementation();
        MOProfile p = (MOProfile) worker.getProfile(impl);
        if (p != null) {
            MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
            long length = dsi.getExpectedEnd() - (dsi.getExpectedStart() < 0 ? 0 : dsi.getExpectedStart());
            implementationCount[impl.getCoreId()][impl.getImplementationId()]++;
            totalEnergy += p.getPower() * length;
            totalCost += p.getPrice();
        }
    }

    public double getTotalEnergy() {
        return totalEnergy;
    }

    public double getTotalCost() {
        return totalCost;
    }

    public int[][] getImplementationsCount() {
        return this.implementationCount;
    }

}
