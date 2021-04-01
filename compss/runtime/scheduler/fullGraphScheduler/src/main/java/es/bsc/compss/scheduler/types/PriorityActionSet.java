/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.scheduler.fullgraph.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;

import java.util.Comparator;
import java.util.PriorityQueue;


public class PriorityActionSet {

    private PriorityQueue<AllocatableAction> noCoreActions;
    private PriorityQueue<AllocatableAction>[] coreActions;
    private PriorityQueue<AllocatableAction> priority;
    public final Comparator<AllocatableAction> comparator;


    /**
     * Creates a new PriorityActionSet with the given action comparator.
     * 
     * @param comparator Action comparator.
     */
    @SuppressWarnings("unchecked")
    public PriorityActionSet(Comparator<AllocatableAction> comparator) {
        this.comparator = comparator;
        this.noCoreActions = new PriorityQueue<>(1, comparator);
        this.priority = new PriorityQueue<>(1, comparator);
        this.coreActions = new PriorityQueue[0];
    }

    /**
     * Clones the given PriorityActionSet.
     * 
     * @param clone PriorityActionSet to clone.
     */
    @SuppressWarnings("unchecked")
    public PriorityActionSet(PriorityActionSet clone) {
        this.comparator = clone.comparator;
        this.noCoreActions = new PriorityQueue<>(clone.noCoreActions);
        this.coreActions = new PriorityQueue[clone.coreActions.length];
        for (int idx = 0; idx < this.coreActions.length; idx++) {
            this.coreActions[idx] = new PriorityQueue<>(clone.coreActions[idx]);
        }
        this.priority = new PriorityQueue<>(clone.priority);
    }

    /**
     * Adds a new action to the pool.
     * 
     * @param action New action.
     */
    @SuppressWarnings("unchecked")
    public void offer(AllocatableAction action) {
        if (((FullGraphSchedulingInformation) action.getSchedulingInfo()).isToReschedule()) {
            Integer coreId = action.getCoreId();
            AllocatableAction currentPeek = null;
            if (coreId == null) {
                currentPeek = this.noCoreActions.peek();
                this.noCoreActions.offer(action);
            } else {
                if (coreId < this.coreActions.length) {
                    currentPeek = this.coreActions[coreId].peek();
                } else {
                    // Resize coreActions array
                    int originalSize = this.coreActions.length;
                    PriorityQueue<AllocatableAction>[] coreActions = new PriorityQueue[coreId + 1];
                    System.arraycopy(this.coreActions, 0, coreActions, 0, originalSize);
                    for (int coreIdx = originalSize; coreIdx < coreId + 1; coreIdx++) {
                        coreActions[coreIdx] = new PriorityQueue<>(1, this.comparator);
                    }
                    this.coreActions = coreActions;
                }
                this.coreActions[coreId].offer(action);
            }
            if (currentPeek != action) {
                rebuildPriorityQueue();
            }
        }
    }

    /**
     * Returns and removes the first action of the pool.
     * 
     * @return The first action of the pool.
     */
    public AllocatableAction poll() {
        AllocatableAction currentPeek;
        while ((currentPeek = this.priority.poll()) != null) {
            Integer coreId = currentPeek.getCoreId();
            AllocatableAction nextPeek;
            if (coreId == null) {
                this.noCoreActions.poll();
                nextPeek = this.noCoreActions.peek();
            } else {
                this.coreActions[coreId].poll();
                nextPeek = this.coreActions[coreId].peek();
            }
            if (nextPeek != null) {
                this.priority.offer(nextPeek);
            }
            FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) currentPeek.getSchedulingInfo();
            if (dsi.isToReschedule()) {
                break;
            }
        }
        return currentPeek;
    }

    /**
     * Removes the first action of the pool associated to the core element with id {@code coreId}.
     * 
     * @param coreId Core Element Id.
     */
    public void removeFirst(Integer coreId) {
        if (coreId == null) {
            this.noCoreActions.poll();
        } else {
            this.coreActions[coreId].poll();
        }
        rebuildPriorityQueue();
    }

    /**
     * Returns the first action of the pool (without removing it).
     * 
     * @return The first action of the pool.
     */
    public AllocatableAction peek() {
        AllocatableAction currentPeek = this.priority.peek();
        while (currentPeek != null
                && !((FullGraphSchedulingInformation) currentPeek.getSchedulingInfo()).isToReschedule()) {
            removeFirst(currentPeek.getCoreId());
            currentPeek = this.priority.peek();
        }
        return currentPeek;
    }

    /**
     * Returns all the registered actions in the pool.
     * 
     * @return A queue with all the registered actions in the pool.
     */
    public PriorityQueue<AllocatableAction> peekAll() {
        PriorityQueue<AllocatableAction> peeks = new PriorityQueue<>(this.coreActions.length + 1, this.comparator);

        AllocatableAction currentCore = this.noCoreActions.peek();
        if (currentCore != null
                && !((FullGraphSchedulingInformation) currentCore.getSchedulingInfo()).isToReschedule()) {
            this.noCoreActions.poll();
            currentCore = this.noCoreActions.peek();
        }
        if (currentCore != null) {
            peeks.offer(currentCore);
        }

        for (PriorityQueue<AllocatableAction> core : this.coreActions) {
            currentCore = core.peek();
            if (currentCore != null
                    && !((FullGraphSchedulingInformation) currentCore.getSchedulingInfo()).isToReschedule()) {
                core.poll();
                currentCore = core.peek();
            }
            if (currentCore != null) {
                peeks.offer(currentCore);
            }
        }
        return peeks;
    }

    private void rebuildPriorityQueue() {
        this.priority.clear();
        AllocatableAction action = this.noCoreActions.peek();
        if (action != null) {
            this.priority.offer(action);
        }
        for (PriorityQueue<AllocatableAction> coreAction : this.coreActions) {
            action = coreAction.peek();
            if (action != null) {
                this.priority.offer(action);
            }
        }
    }

    /**
     * Returns the number of registered actions.
     * 
     * @return The number of registered actions.
     */
    public int size() {
        int size = 0;
        size += this.noCoreActions.size();
        for (PriorityQueue<AllocatableAction> pq : this.coreActions) {
            size += pq.size();
        }
        return size;
    }

    /**
     * Returns whether the action pool is empty or not.
     * 
     * @return {@literal true} if the action set is empty, {@literal false} otherwise.
     */
    public boolean isEmpty() {
        return size() == 0;
    }

    /**
     * Retmoves the given action from the set.
     * 
     * @param action Action to remove.
     */
    public void remove(AllocatableAction action) {
        if (action.getCoreId() == null) {
            this.noCoreActions.remove(action);
        } else {
            this.coreActions[action.getCoreId()].remove(action);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("noCore -> ").append(this.noCoreActions).append("\n");
        for (int i = 0; i < this.coreActions.length; i++) {
            sb.append("Core ").append(i).append(" -> ").append(this.coreActions[i]).append("\n");
        }
        return sb.toString();
    }

}
