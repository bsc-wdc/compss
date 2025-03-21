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
package es.bsc.compss.scheduler.fullgraph.multiobjective.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.fullgraph.multiobjective.MOSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.CoreManager;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class LocalOptimizationState {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean IS_DEBUG = LOGGER.isDebugEnabled();
    protected static final String LOG_PREFIX = "[MOLocalOptimizationState] ";

    private final long updateId;
    private final ResourceScheduler<WorkerResourceDescription> worker;

    private final LinkedList<Gap> gaps;
    private double runningCost;
    private double totalCost;
    private double runningEnergy;
    private double totalEnergy;

    private AllocatableAction action;
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


    /**
     * Creates a new LocalOptimizationState instance.
     * 
     * @param updateId Update Id.
     * @param rs Associated Resource scheduler.
     * @param readyComparator Ready action comparator.
     * @param selectionComparator Selection action comparator.
     */
    public LocalOptimizationState(long updateId, ResourceScheduler<WorkerResourceDescription> rs,
        Comparator<AllocatableAction> readyComparator, Comparator<AllocatableAction> selectionComparator) {

        this.action = null;
        this.updateId = updateId;
        this.worker = rs;
        this.totalCost = 0;
        this.runningCost = 0;
        this.totalEnergy = 0;
        this.runningEnergy = 0;

        this.gaps = new LinkedList<>();
        ResourceDescription rd = rs.getResource().getDescription();
        Gap g = new Gap(0, Long.MAX_VALUE, null, rd.copy(), 0);
        this.gaps.add(g);
        this.implementationCount = new int[CoreManager.getCoreCount()][];
        this.runningImplementationsCount = new int[CoreManager.getCoreCount()][];
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            this.implementationCount[coreId] = new int[CoreManager.getCoreImplementations(coreId).size()];
            this.runningImplementationsCount[coreId] = new int[CoreManager.getCoreImplementations(coreId).size()];
        }
        this.endRunningActions = 0;
        this.resourceBlockingAction = new OptimizationAction();
        this.resourceBlockingAction.assignResource(rs);
        this.dataBlockingAction = new OptimizationAction();
        this.dataBlockingAction.assignResource(rs);

        this.runningActions = new LinkedList<AllocatableAction>();
        this.readyActions = new PriorityQueue<AllocatableAction>(1, readyComparator);
        this.selectableActions = new PriorityActionSet(selectionComparator);
    }

    /**
     * Returns the LocalOptimizationState id.
     * 
     * @return The LocalOptimizationState id.
     */
    public long getId() {
        return this.updateId;
    }

    /**
     * Reserves the given resources at the given start time.
     * 
     * @param resources Resources to reserve.
     * @param startTime Expected start time.
     * @return Previous gap.
     */
    public List<Gap> reserveResources(ResourceDescription resources, long startTime) {
        List<Gap> previousGaps = new LinkedList<>();
        // Remove requirements from resource description
        ResourceDescription requirements = resources.copy();
        Iterator<Gap> gapIt = this.gaps.iterator();
        while (gapIt.hasNext() && !requirements.isDynamicUseless()) {
            Gap g = gapIt.next();
            if (checkGapForReserve(g, requirements, startTime, previousGaps)) {
                gapIt.remove();
            }
        }

        return previousGaps;
    }

    private boolean checkGapForReserve(Gap g, ResourceDescription requirements, long reserveStart,
        List<Gap> previousGaps) {

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

    /**
     * Releases the resources.
     * 
     * @param expectedStart Expected start.
     * @param action Action to release.
     */
    public void releaseResources(long expectedStart, AllocatableAction action) {
        if (action.getAssignedImplementation() != null) {
            Gap gap;
            gap = new Gap(expectedStart, Long.MAX_VALUE, action,
                action.getAssignedImplementation().getRequirements().copy(), 0);
            MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
            dsi.addGap();
            this.gaps.add(gap);
            if (this.missingResources != null) {
                ResourceDescription empty = gap.getResources().copy();
                this.topStartTime = gap.getInitialTime();
                ResourceDescription.reduceCommonDynamics(empty, this.missingResources);
            }
        } else {
            LOGGER.debug(LOG_PREFIX + "Action has null implementation. Nothing done at release resources *** ");
        }
    }

    /**
     * Replaces the current action by the new one.
     * 
     * @param action New action.
     */
    public void replaceAction(AllocatableAction action) {
        this.action = action;
        if (this.action != null) {
            this.missingResources = this.action.getAssignedImplementation().getRequirements().copy();
            // Check if the new peek can run in the already freed resources.
            for (Gap gap : this.gaps) {
                ResourceDescription empty = gap.getResources().copy();
                this.topStartTime = gap.getInitialTime();
                ResourceDescription.reduceCommonDynamics(empty, this.missingResources);
                if (this.missingResources.isDynamicUseless()) {
                    break;
                }
            }
        } else {
            this.missingResources = null;
            this.topStartTime = 0L;
        }
    }

    /**
     * Add temporary gap.
     * 
     * @param g Gap to add.
     */
    public void addTmpGap(Gap g) {
        AllocatableAction gapAction = g.getOrigin();
        MOSchedulingInformation gapDSI = (MOSchedulingInformation) gapAction.getSchedulingInfo();
        gapDSI.addGap();
    }

    /**
     * Replace temporary gap.
     * 
     * @param gap New gap.
     * @param previousGap Previous gap
     */
    public void replaceTmpGap(Gap gap, Gap previousGap) {
        // TODO: Implement tmp replacement
    }

    /**
     * Remove temporary gap.
     * 
     * @param g Gap to remove.
     */
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

    /**
     * Returns the associated action.
     * 
     * @return The associated action.
     */
    public AllocatableAction getAction() {
        return this.action;
    }

    /**
     * Returns the action start time.
     * 
     * @return The action start time.
     */
    public long getActionStartTime() {
        return Math.max(this.topStartTime,
            ((MOSchedulingInformation) this.action.getSchedulingInfo()).getExpectedStart());
    }

    /**
     * Returns whether the action can run or not.
     * 
     * @return {@literal true} if the action can run, {@literal false} otherwise.
     */
    public boolean canActionRun() {
        if (this.missingResources != null) {
            return this.missingResources.isDynamicUseless();
        } else {
            return false;
        }
    }

    /**
     * Return whether there are gaps or not.
     * 
     * @return {@literal true} if there are gaps, {@literal false} otherwise.
     */
    public boolean areGaps() {
        return !this.gaps.isEmpty();
    }

    /**
     * Returns the first available gap.
     * 
     * @return The first available gap.
     */
    public Gap peekFirstGap() {
        return this.gaps.peekFirst();
    }

    /**
     * Removes the first available gap.
     */
    public void pollGap() {
        this.gaps.removeFirst();
    }

    /**
     * Returns all the current gaps.
     * 
     * @return A list containing all the current gaps.
     */
    public List<Gap> getGaps() {
        return this.gaps;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("Optimization State at " + this.updateId + "\n");
        sb.append("\tGaps:\n");
        for (Gap gap : this.gaps) {
            sb.append("\t\t").append(gap).append("\n");
        }
        sb.append("\tTopAction:").append(this.action).append("\n");
        sb.append("\tMissing To Run:").append(this.missingResources).append("\n");
        sb.append("\tExpected Start:").append(this.topStartTime).append("\n");
        sb.append("\tPending Executions:\n");
        for (int coreId = 0; coreId < this.implementationCount.length; coreId++) {
            sb.append("\t\tCore " + coreId + ":\n");
            for (int implId = 0; implId < this.implementationCount[coreId].length; implId++) {
                sb.append("\t\t\tImplementation " + implId + ":" + this.implementationCount[coreId][implId] + "\n");
            }
        }
        return sb.toString();
    }

    /**
     * Marks the action with the given implementation as running.
     * 
     * @param impl Action's implementation.
     * @param p Action's profile.
     * @param pendingTime Pending execution time.
     */
    public void runningAction(Implementation impl, MOProfile p, long pendingTime) {
        if (impl != null) {
            reserveResources(impl.getRequirements(), 0);
            if (impl.getCoreId() != null && impl.getImplementationId() != null) {
                this.runningImplementationsCount[impl.getCoreId()][impl.getImplementationId()]++;
                this.endRunningActions = Math.max(this.endRunningActions, pendingTime);
                this.runningEnergy += p.getPower() * pendingTime;
                this.runningCost += p.getPrice() * pendingTime;
            }
        } else {
            LOGGER.debug(LOG_PREFIX + "Action has a null implementation. Nothing done for reserving resources ***");
        }
    }

    /**
     * Returns the end running time.
     * 
     * @return The end running time.
     */
    public long getEndRunningTime() {
        return this.endRunningActions;
    }

    /**
     * Returns the number of running implementations per implementation per core element.
     * 
     * @return The number of running implementations per implementation per core element.
     */
    public int[][] getRunningImplementations() {
        return this.runningImplementationsCount;
    }

    /**
     * Returns the running cost.
     * 
     * @return The running cost.
     */
    public double getRunningCost() {
        return this.runningCost;
    }

    /**
     * Returns the running energy consumption.
     * 
     * @return The running energy consumption.
     */
    public double getRunningEnergy() {
        return this.runningEnergy;
    }

    /**
     * Polls a new action for the given gap.
     * 
     * @param gap Gap to fill.
     * @return Action to fill the given gap (can be null if there are not any matches).
     */
    public AllocatableAction pollActionForGap(Gap gap) {
        AllocatableAction gapAction = null;
        PriorityQueue<AllocatableAction> peeks = this.selectableActions.peekAll();
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
            Profile p = this.worker.getProfile(impl);
            long expectedLength = p.getAverageExecutionTime();
            if ((gap.getEndTime() - gap.getInitialTime()) < expectedLength) {
                continue;
            }
            if ((start + expectedLength) > gap.getEndTime()) {
                continue;
            }

            // Check description
            if (gap.getResources().canHostDynamic(impl)) {
                this.selectableActions.removeFirst(candidate.getCoreId());
                gapAction = candidate;
            }
        }
        return gapAction;
    }

    /**
     * Marks the action as resource blocked by the given action.
     * 
     * @param action Blocking action.
     */
    public void resourceBlockedAction(AllocatableAction action) {
        MOSchedulingInformation aDSI = (MOSchedulingInformation) action.getSchedulingInfo();
        MOSchedulingInformation rbaDSI = (MOSchedulingInformation) this.resourceBlockingAction.getSchedulingInfo();
        rbaDSI.lock();
        rbaDSI.addSuccessor(action);
        Gap opActionGap =
            new Gap(0, 0, this.resourceBlockingAction, action.getAssignedImplementation().getRequirements().copy(), 0);
        aDSI.addPredecessor(opActionGap);
        rbaDSI.unlock();
        updateConsumptions(action);
    }

    /**
     * Marks the action as data blocked by the given action.
     * 
     * @param action Blocking action.
     */
    public void dataBlockedAction(AllocatableAction action) {
        MOSchedulingInformation aDSI = (MOSchedulingInformation) action.getSchedulingInfo();
        MOSchedulingInformation dbaDSI = (MOSchedulingInformation) this.dataBlockingAction.getSchedulingInfo();
        dbaDSI.lock();
        dbaDSI.addSuccessor(action);
        Gap opActionGap =
            new Gap(0, 0, this.dataBlockingAction, action.getAssignedImplementation().getRequirements().copy(), 0);
        aDSI.addPredecessor(opActionGap);
        dbaDSI.unlock();
        updateConsumptions(action);
    }

    /**
     * Returns the resource blocking action.
     * 
     * @return The resource blocking action.
     */
    public AllocatableAction getResourceBlockingAction() {
        return this.resourceBlockingAction;
    }

    /**
     * Returns the data blocking action.
     * 
     * @return The data blocking action.
     */
    public AllocatableAction getDataBlockingAction() {
        return this.dataBlockingAction;
    }

    /**
     * Classifies an action.
     * 
     * @param action Action to classify.
     * @param hasInternal Whether it has internal or not.
     * @param hasExternal Whether it has external or not.
     * @param hasResourcePredecessors Whether it has resource predecessors or not.
     * @param startTime Expected start time.
     */
    public void classifyAction(AllocatableAction action, boolean hasInternal, boolean hasExternal,
        boolean hasResourcePredecessors, long startTime) {

        if (!hasInternal) {
            // Not needs to wait for some blocked action to end
            if (hasExternal) {
                if (startTime == 0) {
                    // System.out.println("Action added to selectable");
                    this.selectableActions.offer(action);
                } else if (startTime == Long.MAX_VALUE) {
                    // System.out.println("Action added to blocked");
                    dataBlockedAction(action);
                } else {
                    // System.out.println("Action added to ready");
                    this.readyActions.add(action);
                }
            } else {
                // has no dependencies
                if (hasResourcePredecessors) {
                    // System.out.println("Action added to selectable");
                    this.selectableActions.offer(action);
                } else {
                    // System.out.println("Action added to running");
                    this.runningActions.add(action);
                }
            }
        } else {
            // System.out.println("Action not classified.");
        }
    }

    /**
     * Returns the list of running actions.
     * 
     * @return The list of running actions.
     */
    public List<AllocatableAction> getRunningActions() {
        return this.runningActions;
    }

    /**
     * Returns whether there are selectable actions or not.
     * 
     * @return {@literal true} if there are selectable actions, {@literal false} otherwise.
     */
    public boolean areRunnableActions() {
        return !this.selectableActions.isEmpty();
    }

    /**
     * Returns the most prioritary runnable action.
     * 
     * @return The most prioritary runnable action.
     */
    public AllocatableAction getMostPrioritaryRunnableAction() {
        return this.selectableActions.peek();
    }

    /**
     * Removes the most prioritary runnable action.
     */
    public void removeMostPrioritaryRunnableAction() {
        this.selectableActions.poll();
    }

    /**
     * Removes the most prioritary runnable action of the given core Id.
     * 
     * @param coreId Core Id.
     */
    public void removeMostPrioritaryRunnableAction(Integer coreId) {
        this.selectableActions.removeFirst(coreId);
    }

    /**
     * Returns whether there are actions to be rescheduled or not.
     * 
     * @return {@literal true} if there are actions to be re-scheduled, {@literal false} otherwise.
     */
    public boolean areActionsToBeRescheduled() {
        return !this.readyActions.isEmpty();
    }

    /**
     * Returns the earliest action to be re-scheduled.
     * 
     * @return The earliest action to be re-scheduled.
     */
    public AllocatableAction getEarliestActionToBeRescheduled() {
        return this.readyActions.poll();
    }

    /**
     * Updates the time progress.
     * 
     * @param time New time.
     */
    public void progressOnTime(long time) {
        while (this.readyActions.size() > 0) {
            AllocatableAction top = this.readyActions.peek();
            MOSchedulingInformation topDSI = (MOSchedulingInformation) top.getSchedulingInfo();
            long start = topDSI.getExpectedStart();
            if (start > time) {
                break;
            }
            this.readyActions.poll();
            this.selectableActions.offer(top);
        }
    }

    /**
     * Releases the data successors.
     * 
     * @param dsi MOSchedulingInformation.
     * @param timeLimit Time limit.
     */
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
                        if (predecessor.getAssignedResource() != this.worker) {
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
                    this.selectableActions.offer(successor);
                } else {
                    this.readyActions.add(successor);
                }
            }
        }
        dsi.clearOptimizingSuccessors();
    }

    /**
     * Blocks the data successors.
     * 
     * @param dsi MOSchedulingInformation.
     */
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

    /**
     * Updates the action consumptions.
     * 
     * @param action Action to update.
     */
    public void updateConsumptions(AllocatableAction action) {
        Implementation impl = action.getAssignedImplementation();
        MOProfile p = (MOProfile) this.worker.getProfile(impl);
        if (p != null) {
            MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
            long length = dsi.getExpectedEnd() - (dsi.getExpectedStart() < 0 ? 0 : dsi.getExpectedStart());
            this.implementationCount[impl.getCoreId()][impl.getImplementationId()]++;
            this.totalEnergy += p.getPower() * length;
            this.totalCost += p.getPrice() * length;
        }
    }

    /**
     * Returns the total energy.
     * 
     * @return The total energy.
     */
    public double getTotalEnergy() {
        return this.totalEnergy;
    }

    /**
     * Returns the total cost.
     * 
     * @return The total cost.
     */
    public double getTotalCost() {
        return this.totalCost;
    }

    /**
     * Returns the number of executions per implementation per core.
     * 
     * @return The number of executions per implementation per core.
     */
    public int[][] getImplementationsCount() {
        return this.implementationCount;
    }

}
