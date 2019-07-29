/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.fullgraph;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;

import java.util.LinkedList;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


public class FullGraphSchedulingInformation extends SchedulingInformation {

    // Lock to avoid multiple threads to modify the content at the same time
    private final ReentrantLock l = new ReentrantLock();

    private boolean scheduled = false;
    private long lastUpdate;
    private long expectedStart;
    private long expectedEnd;

    private int openGaps = 0;

    // Allocatable actions that the action depends on due to resource availability
    private final LinkedList<AllocatableAction> resourcePredecessors;

    // Allocatable actions depending on the allocatable action due to resource availability
    private LinkedList<AllocatableAction> resourceSuccessors;

    // Action Scheduling is being optimized locally
    private boolean onOptimization = false;
    private boolean toReschedule = false;
    private final LinkedList<AllocatableAction> optimizingSuccessors;


    /**
     * Creates a new FullGraphSchedulingInformation instance.
     * 
     * @param enforcedTargetResource Enforced resource.
     */
    public FullGraphSchedulingInformation(ResourceScheduler<?> enforcedTargetResource) {
        super(enforcedTargetResource);

        this.resourcePredecessors = new LinkedList<>();
        this.resourceSuccessors = new LinkedList<>();

        this.lastUpdate = System.currentTimeMillis();
        this.expectedStart = 0;
        this.expectedEnd = 0;

        this.optimizingSuccessors = new LinkedList<>();
    }

    /**
     * Adds a new predecessor.
     * 
     * @param predecessor New predecessor.
     */
    public void addPredecessor(AllocatableAction predecessor) {
        this.resourcePredecessors.add(predecessor);
    }

    /**
     * Returns whether the action has resource predecessors or not.
     * 
     * @return {@literal true} if the action has resource predecessors, {@literal false} otherwise.
     */
    public boolean hasPredecessors() {
        return !this.resourcePredecessors.isEmpty();
    }

    @Override
    public final boolean isExecutable() {
        boolean b;
        lock();
        b = this.resourcePredecessors.isEmpty();
        unlock();
        return b;
    }

    /**
     * Returns the list of resource predecessors.
     * 
     * @return The list of resource predecessors.
     */
    public LinkedList<AllocatableAction> getPredecessors() {
        return this.resourcePredecessors;
    }

    /**
     * Removes the given predecessor.
     * 
     * @param successor Predecessor to remove.
     */
    public void removePredecessor(AllocatableAction successor) {
        this.resourcePredecessors.remove(successor);
    }

    /**
     * Clears all the predecessors.
     */
    public void clearPredecessors() {
        this.resourcePredecessors.clear();
    }

    /**
     * Adds a new resource successor.
     * 
     * @param successor New resource successor.
     */
    public void addSuccessor(AllocatableAction successor) {
        this.resourceSuccessors.add(successor);
    }

    /**
     * Returns the list of resource successors.
     * 
     * @return The list of resource successors.
     */
    public LinkedList<AllocatableAction> getSuccessors() {
        return this.resourceSuccessors;
    }

    /**
     * Removes the given successor.
     * 
     * @param successor Successor to remove.
     */
    public void removeSuccessor(AllocatableAction successor) {
        this.resourceSuccessors.remove(successor);
    }

    /**
     * Clears the list of successors.
     */
    public void clearSuccessors() {
        this.resourceSuccessors.clear();
    }

    /**
     * Replaces the list of successors.
     * 
     * @param newSuccessors New successors' list.
     * @return The old list of successors.
     */
    public LinkedList<AllocatableAction> replaceSuccessors(LinkedList<AllocatableAction> newSuccessors) {
        LinkedList<AllocatableAction> oldSuccessors = this.resourceSuccessors;
        this.resourceSuccessors = newSuccessors;
        return oldSuccessors;
    }

    /**
     * Sets a new expected start time.
     * 
     * @param expectedStart New expected start time.
     */
    public void setExpectedStart(long expectedStart) {
        this.expectedStart = expectedStart;
    }

    /**
     * Returns the expected start time.
     * 
     * @return The expected start time.
     */
    public long getExpectedStart() {
        return this.expectedStart;
    }

    /**
     * Sets a new expected end time.
     * 
     * @param expectedEnd New expected end time.
     */
    public void setExpectedEnd(long expectedEnd) {
        this.expectedEnd = expectedEnd;
    }

    /**
     * Returns the expected end time.
     * 
     * @return The expected end time.
     */
    public long getExpectedEnd() {
        return this.expectedEnd;
    }

    /**
     * Sets a new last update time.
     * 
     * @param lastUpdate New last update time.
     */
    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

    /**
     * Returns the last update time.
     * 
     * @return The last update time.
     */
    public long getLastUpdate() {
        return this.lastUpdate;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder("\tlastUpdate: " + this.lastUpdate + "\n" + "\texpectedStart: "
                + this.expectedStart + "\n" + "\texpectedEnd:" + this.expectedEnd + "\n");
        sb.append("\t").append("schedPredecessors: ");
        for (AllocatableAction aa : getPredecessors()) {
            sb.append(" ").append(aa);
        }
        sb.append("\n");
        sb.append("\t").append("schedSuccessors: ");
        for (AllocatableAction aa : getSuccessors()) {
            sb.append(" ").append(aa);
        }
        sb.append("\n");
        sb.append("\tOptimization Successors").append(optimizingSuccessors);
        return sb.toString();
    }

    /**
     * Tries to lock the action for scheduling.
     * 
     * @return {@literal true} if the action has been locked, {@literal false} otherwise.
     */
    public boolean tryToLock() {
        try {
            return this.l.tryLock(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            return false;
        }
    }

    /**
     * Locks the action.
     */
    public void lock() {
        this.l.lock();
    }

    /**
     * Unlocks the action.
     */
    public void unlock() {
        this.l.unlock();
    }

    /**
     * Completely unlocks the action.
     */
    public void unlockCompletely() {
        while (this.l.getHoldCount() > 1) {
            this.l.unlock();
        }
    }

    /**
     * Returns the number of locks.
     * 
     * @return The number of locks.
     */
    public int getLockCount() {
        return this.l.getHoldCount();
    }

    /**
     * Marks the action as scheduled.
     */
    public void scheduled() {
        this.scheduled = true;
    }

    /**
     * Marks the action as unscheduled.
     */
    public void unscheduled() {
        this.scheduled = false;
        this.resourcePredecessors.clear();
        this.resourceSuccessors.clear();
    }

    /**
     * Returns whether the action is scheduled or not.
     * 
     * @return {@literal true} of the action has been scheduled, {@literal false} otherwise.
     */
    boolean isScheduled() {
        return this.scheduled;
    }

    /**
     * Marks the action as on optimization.
     * 
     * @param b Optimization value.
     */
    public void setOnOptimization(boolean b) {
        this.onOptimization = b;
    }

    /**
     * Returns whether the task is being in optimized or not.
     * 
     * @return {@literal true} if the task is being optimized, {@literal false} otherwise.
     */
    public boolean isOnOptimization() {
        return this.onOptimization;
    }

    /**
     * Marks the action with the to re-schedule value.
     * 
     * @param b Re-schedule value.
     */
    public void setToReschedule(boolean b) {
        this.toReschedule = b;
    }

    /**
     * Returns whether the task must be re-scheduled or not.
     * 
     * @return {@literal true} if the task must be re-scheduled, {@literal false} otherwise.
     */
    public boolean isToReschedule() {
        return this.toReschedule;
    }

    /**
     * Sets a new optimizing successor.
     * 
     * @param action New optimizing successor.
     */
    public void optimizingSuccessor(AllocatableAction action) {
        this.optimizingSuccessors.add(action);
    }

    /**
     * Returns the optimizing successors.
     * 
     * @return The optimizing successors.
     */
    public LinkedList<AllocatableAction> getOptimizingSuccessors() {
        return this.optimizingSuccessors;
    }

    /**
     * Clears the optimizing successors.
     */
    public void clearOptimizingSuccessors() {
        this.optimizingSuccessors.clear();
    }

    /**
     * Adds a new gap.
     */
    public void addGap() {
        this.openGaps++;
    }

    /**
     * Removes a gap.
     */
    public void removeGap() {
        this.openGaps--;
    }

    /**
     * Clears all the registered gaps.
     */
    public void clearGaps() {
        this.openGaps = 0;
    }

    /**
     * Returns whether the action has gaps or not.
     * 
     * @return {@literal true} if the action has gaps, {@literal false} otherwise.
     */
    public boolean hasGaps() {
        return this.openGaps > 0;
    }

    /**
     * Returns the number of gaps.
     * 
     * @return The number of gaps.
     */
    public int getGapCount() {
        return this.openGaps;
    }

}
