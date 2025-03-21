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
package es.bsc.compss.scheduler.fullgraph.multiobjective;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.fullgraph.multiobjective.types.Gap;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.locks.ReentrantLock;


public class MOSchedulingInformation extends SchedulingInformation {

    // Lock to avoid multiple threads to modify the content at the same time
    private final ReentrantLock l = new ReentrantLock();

    private boolean scheduled = false;
    private long lastUpdate;
    private long expectedStart;
    private long expectedEnd;

    private int openGaps = 0;

    // Allocatable actions that the action depends on due to resource availability
    private final List<Gap> resourcePredecessors;

    // Allocatable actions depending on the allocatable action due to resource availability
    private List<AllocatableAction> resourceSuccessors;

    // Action Scheduling is being optimized locally
    private boolean onOptimization = false;
    private boolean toReschedule = false;
    private final List<AllocatableAction> optimizingSuccessors;


    /**
     * Creates a new MOSchedulingInformation instance.
     * 
     * @param enforcedTargetResource Enforced target resource.
     */
    public <T extends WorkerResourceDescription> MOSchedulingInformation(ResourceScheduler<T> enforcedTargetResource) {
        super(enforcedTargetResource);
        this.resourcePredecessors = new LinkedList<>();
        this.resourceSuccessors = new LinkedList<>();

        this.lastUpdate = System.currentTimeMillis();
        this.expectedStart = 0;
        this.expectedEnd = 0;

        this.optimizingSuccessors = new LinkedList<>();
    }

    @Override
    public final boolean isExecutable() {
        boolean b = true;
        lock();
        for (Gap g : this.resourcePredecessors) {
            b = b && (g.getOrigin() == null);
        }
        unlock();
        return b;
    }

    /**
     * Adds a predecessor.
     * 
     * @param predecessor Predecessor.
     */
    public void addPredecessor(Gap predecessor) {
        this.resourcePredecessors.add(predecessor);
    }

    /**
     * Returns whether it has predecessors or not.
     * 
     * @return {@literal true} if there are precessors, {@literal false} otherwise.
     */
    public boolean hasPredecessors() {
        return !this.resourcePredecessors.isEmpty();
    }

    /**
     * Returns the list of predecessors.
     * 
     * @return The list of predecessors.
     */
    public List<Gap> getPredecessors() {
        return this.resourcePredecessors;
    }

    /**
     * Removes the given action from the predecessors.
     * 
     * @param successor Predecessor to remove.
     * @return Predecessor removed.
     */
    public Gap removePredecessor(AllocatableAction successor) {
        Iterator<Gap> it = this.resourcePredecessors.iterator();
        Gap g = null;
        while (it.hasNext()) {
            g = it.next();
            if (g.getOrigin() == successor) {
                it.remove();
            }
        }
        return g;
    }

    /**
     * Clears all the predecessors.
     */
    public void clearPredecessors() {
        this.resourcePredecessors.clear();
    }

    /**
     * Adds a new successor.
     * 
     * @param successor Successor to add.
     */
    public void addSuccessor(AllocatableAction successor) {
        this.resourceSuccessors.add(successor);
    }

    /**
     * Returns a list of successor actions.
     * 
     * @return A list of successor actions.
     */
    public List<AllocatableAction> getSuccessors() {
        return this.resourceSuccessors;
    }

    /**
     * Removes the given successor.
     * 
     * @param successor Successor action to remove.
     */
    public void removeSuccessor(AllocatableAction successor) {
        this.resourceSuccessors.remove(successor);
    }

    /**
     * Removes all successors.
     */
    public void clearSuccessors() {
        this.resourceSuccessors.clear();
    }

    /**
     * Replaces the current list of successors.
     * 
     * @param newSuccessors New list of successors.
     * @return Old successors.
     */
    public List<AllocatableAction> replaceSuccessors(List<AllocatableAction> newSuccessors) {
        List<AllocatableAction> oldSuccessors = this.resourceSuccessors;
        this.resourceSuccessors = newSuccessors;
        return oldSuccessors;
    }

    /**
     * Sets a new expected start.
     * 
     * @param expectedStart New expected start.
     */
    public void setExpectedStart(long expectedStart) {
        this.expectedStart = expectedStart;
    }

    /**
     * Returns the expected start.
     * 
     * @return The expected start.
     */
    public long getExpectedStart() {
        return this.expectedStart;
    }

    /**
     * Sets a new expected end.
     * 
     * @param expectedEnd New expected end.
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
     * Sets a new update time.
     * 
     * @param lastUpdate New update time.
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
        for (Gap g : getPredecessors()) {
            sb.append(" ").append(g.getOrigin());
        }
        sb.append("\n");
        sb.append("\t").append("schedSuccessors: ");
        for (AllocatableAction aa : getSuccessors()) {
            sb.append(" ").append(aa);
        }
        sb.append("\n");
        sb.append("\tOptimization Successors").append(this.optimizingSuccessors);
        return sb.toString();
    }

    /**
     * Tries to lock the current MOSI.
     * 
     * @return Whether the MOSI has been locked or not.
     */
    public boolean tryToLock() {
        // System.out.println("[MOSI] trylock "+ this.hashCode() + "("+l.getHoldCount()+")");
        try {
            return this.l.tryLock(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            return false;
        }
    }

    /**
     * Locks.
     */
    public void lock() {
        // System.out.println("[MOSI] Aquiring lock "+ this.hashCode() + "("+l.getHoldCount()+")");
        this.l.lock();
    }

    /**
     * Unlocks.
     */
    public void unlock() {
        this.l.unlock();
        // System.out.println("[MOSI] Unlock "+ this.hashCode()+ "("+l.getHoldCount()+")");
    }

    /**
     * Returns the number of locks.
     * 
     * @return Number of locks.
     */
    public int getLockCount() {
        return this.l.getHoldCount();
    }

    /**
     * Completely unlocks.
     */
    public void unlockCompletely() {
        // System.out.println("[MOSI] Unlock compleately "+ this.hashCode());
        while (this.l.getHoldCount() > 1) {
            this.l.unlock();
        }
    }

    /**
     * Marks as scheduled.
     */
    public void scheduled() {
        this.scheduled = true;
    }

    /**
     * Unschedules the MOSI.
     */
    public void unscheduled() {
        this.scheduled = false;
        this.resourcePredecessors.clear();
        this.resourceSuccessors.clear();
    }

    /**
     * Returns whether the MOSI has been scheduled or not.
     * 
     * @return {@literal true} if the MOSI has been scheduled, {@literal false} otherwise.
     */
    boolean isScheduled() {
        return this.scheduled;
    }

    /**
     * Marks the MOSI as in optimization state.
     * 
     * @param b Whether the MOSI is in optimization state or not.
     */
    public void setOnOptimization(boolean b) {
        this.onOptimization = b;
    }

    /**
     * Returns whether the MOSI is in optimization state or not.
     * 
     * @return {@literal true} if the MOSI is in optimization state, {@literal false} otherwise.
     */
    public boolean isOnOptimization() {
        return this.onOptimization;
    }

    /**
     * Marks the MOSI to re-schedule.
     * 
     * @param b Whether the MOSI must be re-scheduled or not.
     */
    public void setToReschedule(boolean b) {
        this.toReschedule = b;
    }

    /**
     * Returns whether the MOSI has to be re-scheduled or not.
     * 
     * @return {@literal true} if the MOSI has to be re-scheduled, {@literal false} otherwise.
     */
    public boolean isToReschedule() {
        return this.toReschedule;
    }

    /**
     * Adds a new optimizing successor.
     * 
     * @param action New optimizing successor.
     */
    public void addOptimizingSuccessor(AllocatableAction action) {
        this.optimizingSuccessors.add(action);
    }

    /**
     * Removes the given optimizing successor.
     * 
     * @param action Optmizing successor to remove.
     */
    public void removeOptimizingSuccessor(AllocatableAction action) {
        this.optimizingSuccessors.remove(action);
    }

    /**
     * Returns the list of optimizing successors.
     * 
     * @return The list of optimizing successors.
     */
    public List<AllocatableAction> getOptimizingSuccessors() {
        return this.optimizingSuccessors;
    }

    /**
     * Clears the list of optimizing successors.
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
     * Clears all the current gaps.
     */
    public void clearGaps() {
        this.openGaps = 0;
    }

    /**
     * Returns whether there are gaps or not.
     * 
     * @return {@literal true} if there are open gaps, {@literal false} otherwise.
     */
    public boolean hasGaps() {
        return this.openGaps > 0;
    }

    /**
     * Returns the number of open gaps.
     * 
     * @return The number of open gaps.
     */
    public int getGapCount() {
        return this.openGaps;
    }
}
