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
package es.bsc.compss.scheduler.fullGraphScheduler;

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


    public FullGraphSchedulingInformation(ResourceScheduler<?> enforcedTargetResource) {
        super(enforcedTargetResource);

        this.resourcePredecessors = new LinkedList<>();
        this.resourceSuccessors = new LinkedList<>();

        this.lastUpdate = System.currentTimeMillis();
        this.expectedStart = 0;
        this.expectedEnd = 0;

        this.optimizingSuccessors = new LinkedList<>();
    }

    public void addPredecessor(AllocatableAction predecessor) {
        this.resourcePredecessors.add(predecessor);
    }

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

    public LinkedList<AllocatableAction> getPredecessors() {
        return this.resourcePredecessors;
    }

    public void removePredecessor(AllocatableAction successor) {
        this.resourcePredecessors.remove(successor);
    }

    public void clearPredecessors() {
        this.resourcePredecessors.clear();
    }

    public void addSuccessor(AllocatableAction successor) {
        this.resourceSuccessors.add(successor);
    }

    public LinkedList<AllocatableAction> getSuccessors() {
        return this.resourceSuccessors;
    }

    public void removeSuccessor(AllocatableAction successor) {
        this.resourceSuccessors.remove(successor);
    }

    public void clearSuccessors() {
        this.resourceSuccessors.clear();
    }

    public LinkedList<AllocatableAction> replaceSuccessors(LinkedList<AllocatableAction> newSuccessors) {
        LinkedList<AllocatableAction> oldSuccessors = this.resourceSuccessors;
        this.resourceSuccessors = newSuccessors;
        return oldSuccessors;
    }

    public void setExpectedStart(long expectedStart) {
        this.expectedStart = expectedStart;
    }

    public long getExpectedStart() {
        return this.expectedStart;
    }

    public void setExpectedEnd(long expectedEnd) {
        this.expectedEnd = expectedEnd;
    }

    public long getExpectedEnd() {
        return this.expectedEnd;
    }

    public void setLastUpdate(long lastUpdate) {
        this.lastUpdate = lastUpdate;
    }

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

    public boolean tryToLock() {
        try {
            return this.l.tryLock(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            return false;
        }
    }

    public void lock() {
        this.l.lock();
    }

    public void unlock() {
        this.l.unlock();
    }

    public void unlockCompletely() {
        while (this.l.getHoldCount() > 1) {
            this.l.unlock();
        }
    }

    public void scheduled() {
        this.scheduled = true;
    }

    public void unscheduled() {
        this.scheduled = false;
        this.resourcePredecessors.clear();
        this.resourceSuccessors.clear();
    }

    boolean isScheduled() {
        return this.scheduled;
    }

    public void setOnOptimization(boolean b) {
        this.onOptimization = b;
    }

    public boolean isOnOptimization() {
        return this.onOptimization;
    }

    public void setToReschedule(boolean b) {
        this.toReschedule = b;
    }

    public boolean isToReschedule() {
        return this.toReschedule;
    }

    public void optimizingSuccessor(AllocatableAction action) {
        this.optimizingSuccessors.add(action);
    }

    public LinkedList<AllocatableAction> getOptimizingSuccessors() {
        return this.optimizingSuccessors;
    }

    public void clearOptimizingSuccessors() {
        this.optimizingSuccessors.clear();
    }

    public void addGap() {
        this.openGaps++;
    }

    public void removeGap() {
        this.openGaps--;
    }

    public void clearGaps() {
        this.openGaps = 0;
    }

    public boolean hasGaps() {
        return this.openGaps > 0;
    }

    public int getLockCount() {
        return this.l.getHoldCount();
    }

    public int getGapCount() {
        return this.openGaps;
    }

}
