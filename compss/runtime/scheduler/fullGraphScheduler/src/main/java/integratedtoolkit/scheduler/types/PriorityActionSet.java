package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.Comparator;
import java.util.PriorityQueue;


public class PriorityActionSet<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    private PriorityQueue<AllocatableAction<P, T, I>> noCoreActions;
    private PriorityQueue<AllocatableAction<P, T, I>>[] coreActions;
    private PriorityQueue<AllocatableAction<P, T, I>> priority;
    public final Comparator<AllocatableAction<P, T, I>> comparator;


    @SuppressWarnings("unchecked")
    public PriorityActionSet(Comparator<AllocatableAction<P, T, I>> comparator) {
        this.comparator = comparator;
        noCoreActions = new PriorityQueue<>(1, comparator);
        priority = new PriorityQueue<>(1, comparator);
        coreActions = new PriorityQueue[0];
    }

    @SuppressWarnings("unchecked")
    public PriorityActionSet(PriorityActionSet<P, T, I> clone) {
        comparator = clone.comparator;
        noCoreActions = new PriorityQueue<>(clone.noCoreActions);
        coreActions = new PriorityQueue[clone.coreActions.length];
        for (int idx = 0; idx < coreActions.length; idx++) {
            coreActions[idx] = new PriorityQueue<>(clone.coreActions[idx]);
        }
        priority = new PriorityQueue<>(clone.priority);
    }

    @SuppressWarnings("unchecked")
    public void offer(AllocatableAction<P, T, I> action) {
        if (((FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo()).isToReschedule()) {
            Integer coreId = action.getCoreId();
            AllocatableAction<P, T, I> currentPeek = null;
            if (coreId == null) {
                currentPeek = noCoreActions.peek();
                noCoreActions.offer(action);
            } else {
                if (coreId < coreActions.length) {
                    currentPeek = coreActions[coreId].peek();
                } else {
                    // Resize coreActions array
                    int originalSize = this.coreActions.length;
                    PriorityQueue<AllocatableAction<P, T, I>>[] coreActions = new PriorityQueue[coreId + 1];
                    System.arraycopy(this.coreActions, 0, coreActions, 0, originalSize);
                    for (int coreIdx = originalSize; coreIdx < coreId + 1; coreIdx++) {
                        coreActions[coreIdx] = new PriorityQueue<>(1, comparator);
                    }
                    this.coreActions = coreActions;
                }
                coreActions[coreId].offer(action);
            }
            if (currentPeek != action) {
                rebuildPriorityQueue();
            }
        }
    }

    public AllocatableAction<P, T, I> poll() {
        AllocatableAction<P, T, I> currentPeek;
        while ((currentPeek = priority.poll()) != null) {
            Integer coreId = currentPeek.getCoreId();
            AllocatableAction<P, T, I> nextPeek;
            if (coreId == null) {
                noCoreActions.poll();
                nextPeek = noCoreActions.peek();
            } else {
                coreActions[coreId].poll();
                nextPeek = coreActions[coreId].peek();
            }
            if (nextPeek != null) {
                priority.offer(nextPeek);
            }
            FullGraphSchedulingInformation<P, T, I> dsi = (FullGraphSchedulingInformation<P, T, I>) currentPeek.getSchedulingInfo();
            if (dsi.isToReschedule()) {
                break;
            }
        }
        return currentPeek;
    }

    public void removeFirst(Integer coreId) {
        if (coreId == null) {
            noCoreActions.poll();
        } else {
            coreActions[coreId].poll();
        }
        rebuildPriorityQueue();
    }

    public AllocatableAction<P, T, I> peek() {
        AllocatableAction<P, T, I> currentPeek = priority.peek();
        while (currentPeek != null && !((FullGraphSchedulingInformation<P, T, I>) currentPeek.getSchedulingInfo()).isToReschedule()) {
            removeFirst(currentPeek.getCoreId());
            currentPeek = priority.peek();
        }
        return currentPeek;
    }

    public PriorityQueue<AllocatableAction<P, T, I>> peekAll() {
        PriorityQueue<AllocatableAction<P, T, I>> peeks = new PriorityQueue<>(coreActions.length + 1, comparator);

        AllocatableAction<P, T, I> currentCore = noCoreActions.peek();
        if (currentCore != null && !((FullGraphSchedulingInformation<P, T, I>) currentCore.getSchedulingInfo()).isToReschedule()) {
            noCoreActions.poll();
            currentCore = noCoreActions.peek();
        }
        if (currentCore != null) {
            peeks.offer(currentCore);
        }

        for (PriorityQueue<AllocatableAction<P, T, I>> core : coreActions) {
            currentCore = core.peek();
            if (currentCore != null && !((FullGraphSchedulingInformation<P, T, I>) currentCore.getSchedulingInfo()).isToReschedule()) {
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
        priority.clear();
        AllocatableAction<P, T, I> action = noCoreActions.peek();
        if (action != null) {
            priority.offer(action);
        }
        for (PriorityQueue<AllocatableAction<P, T, I>> coreAction : coreActions) {
            action = coreAction.peek();
            if (action != null) {
                priority.offer(action);
            }
        }
    }

    public int size() {
        int size = 0;
        size += noCoreActions.size();
        for (PriorityQueue<AllocatableAction<P, T, I>> pq : coreActions) {
            size += pq.size();
        }
        return size;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void remove(AllocatableAction<P, T, I> action) {
        if (action.getCoreId() == null) {
            noCoreActions.remove(action);
        } else {
            coreActions[action.getCoreId()].remove(action);
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("noCore -> ").append(noCoreActions).append("\n");
        for (int i = 0; i < coreActions.length; i++) {
            sb.append("Core ").append(i).append(" -> ").append(coreActions[i]).append("\n");
        }
        return sb.toString();
    }

}
