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
package es.bsc.compss.util;

import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class ActionSet {

    private final List<AllocatableAction> noCore;
    private List<AllocatableAction>[] coreIndexed;
    private int[] counts;
    private int totalActions;


    /**
     * Creates a new ActionSet instance.
     */
    @SuppressWarnings("unchecked")
    public ActionSet() {
        int coreCount = CoreManager.getCoreCount();

        this.noCore = new LinkedList<>();
        this.coreIndexed = new LinkedList[coreCount];
        this.counts = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            this.coreIndexed[coreId] = new LinkedList<>();
            this.counts[coreId] = 0;
        }
        this.totalActions = 0;
    }

    /**
     * Updates the number of cores of the ActionSet.
     * 
     * @param newCoreCount New core count.
     */
    @SuppressWarnings("unchecked")
    public void updateCoreCount(int newCoreCount) {
        int oldCoreCount = coreIndexed.length;
        if (oldCoreCount < newCoreCount) {
            // Increase the coreIndexed and the counts arrays
            List<AllocatableAction>[] coreIndexed = new LinkedList[newCoreCount];
            int[] counts = new int[newCoreCount];
            int coreId = 0;
            for (; coreId < oldCoreCount; coreId++) {
                coreIndexed[coreId] = this.coreIndexed[coreId];
                counts[coreId] = this.counts[coreId];
            }
            for (; coreId < newCoreCount; coreId++) {
                coreIndexed[coreId] = new LinkedList<>();
                counts[coreId] = 0;
            }
            this.coreIndexed = coreIndexed;
            this.counts = counts;
        }
    }

    /**
     * Adds a new action to the action set.
     * 
     * @param aa AllocatableAction to add.
     */
    public void addAction(AllocatableAction aa) {
        Integer core = aa.getCoreId();
        if (core == null) {
            this.noCore.add(aa);
        } else {
            // Update coreCount if the core is out of bounds (has been registered meanwhile)
            if (core >= this.coreIndexed.length) {
                updateCoreCount(CoreManager.getCoreCount());
            }
            this.coreIndexed[core].add(aa);
            this.counts[core]++;
        }
        this.totalActions++;
    }

    /**
     * Returns the total number of actions of the action set.
     * 
     * @return The total number of actions of the action set.
     */
    public int getNumberTotalActions() {
        return this.totalActions;
    }

    /**
     * Returns the number of counts of each action of the action set.
     * 
     * @return The number of counts of each action of the action set.
     */
    public int[] getActionCounts() {
        return this.counts;
    }

    /**
     * Returns the actions of the action set associated with the given core Id.
     * 
     * @param coreId Core Id.
     * @return Actions of the action set associated with the given core Id.
     */
    public List<AllocatableAction> getActions(Integer coreId) {
        if (coreId == null) {
            return this.noCore;
        } else {
            return this.coreIndexed[coreId];
        }
    }

    /**
     * Returns all the actions of the action set.
     * 
     * @return All the actions of the action set.
     */
    public List<AllocatableAction> getAllActions() {
        List<AllocatableAction> runnable = new LinkedList<>();
        runnable.addAll(this.noCore);

        for (int core = 0; core < this.coreIndexed.length; ++core) {
            runnable.addAll(coreIndexed[core]);
        }
        return runnable;
    }

    /**
     * Removes an action from the action set.
     * 
     * @param action AllocatableAction to remove.
     */
    public void removeAction(AllocatableAction action) {
        Integer coreId = action.getCoreId();
        boolean removed = false;
        if (coreId == null) {
            removed = this.noCore.remove(action);
        } else {
            if (coreId < this.coreIndexed.length) {
                removed = this.coreIndexed[coreId].remove(action);
                this.counts[coreId]--;
            }
        }
        if (removed) {
            totalActions--;
        }
    }

    /**
     * Remove from the action set all the compatible actions with the given worker {@code r}.
     * 
     * @param r Worker.
     * @return List of removed actions.
     */
    public <T extends WorkerResourceDescription> List<AllocatableAction> removeAllCompatibleActions(Worker<T> r) {
        List<AllocatableAction> runnable = new LinkedList<>();
        Iterator<AllocatableAction> actions = this.noCore.iterator();
        while (actions.hasNext()) {
            AllocatableAction action = actions.next();
            if (action.isCompatible(r)) {
                actions.remove();
                totalActions--;
                runnable.add(action);
            }
        }

        List<Integer> executableCores = r.getExecutableCores();
        for (int core : executableCores) {
            runnable.addAll(coreIndexed[core]);
            totalActions = totalActions - this.counts[core];
            this.coreIndexed[core] = new LinkedList<>();
            this.counts[core] = 0;

        }
        return runnable;
    }

    /**
     * Removes all actions from the action set.
     * 
     * @return List of removed actions.
     */
    public List<AllocatableAction> removeAllActions() {
        List<AllocatableAction> runnable = new LinkedList<>();
        Iterator<AllocatableAction> actions = this.noCore.iterator();
        while (actions.hasNext()) {
            AllocatableAction action = actions.next();
            actions.remove();
            runnable.add(action);
        }

        for (int core = 0; core < this.coreIndexed.length; ++core) {
            runnable.addAll(coreIndexed[core]);
            this.coreIndexed[core] = new LinkedList<>();
            this.counts[core] = 0;
        }
        totalActions = 0;
        return runnable;
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString() + "\n");
        sb.append("no Core (").append(noCore.size()).append(")-> ").append(noCore).append("\n");
        for (int coreId = 0; coreId < coreIndexed.length; coreId++) {
            sb.append("Core ").append(coreId).append(" (").append(counts[coreId]).append(") -> ")
                .append(coreIndexed[coreId]).append("\n");
        }

        return sb.toString();
    }

}
