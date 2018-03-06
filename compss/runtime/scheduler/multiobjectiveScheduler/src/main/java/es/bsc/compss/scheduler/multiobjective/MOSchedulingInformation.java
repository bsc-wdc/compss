/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.multiobjective;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.multiobjective.types.Gap;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
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


    public <T extends WorkerResourceDescription> MOSchedulingInformation(ResourceScheduler<T> enforcedTargetResource) {
        super(enforcedTargetResource);
        resourcePredecessors = new LinkedList<>();
        resourceSuccessors = new LinkedList<>();

        lastUpdate = System.currentTimeMillis();
        expectedStart = 0;
        expectedEnd = 0;

        optimizingSuccessors = new LinkedList<>();
    }

    public void addPredecessor(Gap predecessor) {
        resourcePredecessors.add(predecessor);
    }

    public boolean hasPredecessors() {
        return !resourcePredecessors.isEmpty();
    }

    @Override
    public final boolean isExecutable() {
        boolean b = true;
        lock();
        for (Gap g : resourcePredecessors) {
            b = b && (g.getOrigin() == null);
        }
        unlock();
        return b;
    }

    public List<Gap> getPredecessors() {
        return resourcePredecessors;
    }

    public Gap removePredecessor(AllocatableAction successor) {
        Iterator<Gap> it = resourcePredecessors.iterator();
        Gap g = null;
        while (it.hasNext()) {
            g = it.next();
            if (g.getOrigin() == successor) {
                it.remove();
            }
        }
        return g;
    }

    public void clearPredecessors() {
        resourcePredecessors.clear();
    }

    public void addSuccessor(AllocatableAction successor) {
        resourceSuccessors.add(successor);
    }

    public List<AllocatableAction> getSuccessors() {
        return resourceSuccessors;
    }

    public void removeSuccessor(AllocatableAction successor) {
        resourceSuccessors.remove(successor);
    }

    public void clearSuccessors() {
        resourceSuccessors.clear();
    }

    public List<AllocatableAction> replaceSuccessors(List<AllocatableAction> newSuccessors) {
        List<AllocatableAction> oldSuccessors = resourceSuccessors;
        resourceSuccessors = newSuccessors;
        return oldSuccessors;
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

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(
                "\tlastUpdate: " + lastUpdate + "\n" + "\texpectedStart: " + expectedStart + "\n" + "\texpectedEnd:" + expectedEnd + "\n");
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
        sb.append("\tOptimization Successors").append(optimizingSuccessors);
        return sb.toString();
    }

    public boolean tryToLock() {
        // System.out.println("[MOSI] trylock "+ this.hashCode() + "("+l.getHoldCount()+")");
        try {
            return l.tryLock(1, TimeUnit.MILLISECONDS);
        } catch (InterruptedException ie) {
            return false;
        }
    }

    public void lock() {
        // System.out.println("[MOSI] Aquiring lock "+ this.hashCode() + "("+l.getHoldCount()+")");
        l.lock();
    }

    public void unlock() {
        l.unlock();
        // System.out.println("[MOSI] Unlock "+ this.hashCode()+ "("+l.getHoldCount()+")");
    }

    public void unlockCompletely() {
        // System.out.println("[MOSI] Unlock compleately "+ this.hashCode());
        while (l.getHoldCount() > 1) {
            l.unlock();
        }
    }

    public void scheduled() {
        scheduled = true;
    }

    public void unscheduled() {
        scheduled = false;
        resourcePredecessors.clear();
        resourceSuccessors.clear();
    }

    boolean isScheduled() {
        return scheduled;
    }

    public void setOnOptimization(boolean b) {
        onOptimization = b;
    }

    public boolean isOnOptimization() {
        return onOptimization;
    }

    public void setToReschedule(boolean b) {
        this.toReschedule = b;
    }

    public boolean isToReschedule() {
        return this.toReschedule;
    }

    public void addOptimizingSuccessor(AllocatableAction action) {
        optimizingSuccessors.add(action);
    }

    public void removeOptimizingSuccessor(AllocatableAction action) {
        optimizingSuccessors.remove(action);
    }

    public List<AllocatableAction> getOptimizingSuccessors() {
        return optimizingSuccessors;
    }

    public void clearOptimizingSuccessors() {
        optimizingSuccessors.clear();
    }

    public void addGap() {
        openGaps++;
    }

    public void removeGap() {
        openGaps--;
    }

    public void clearGaps() {
        openGaps = 0;
    }

    public boolean hasGaps() {
        return openGaps > 0;
    }

    public int getLockCount() {
        return l.getHoldCount();
    }

    public int getGapCount() {
        return openGaps;
    }
}
