package es.bsc.compss.scheduler.multiobjective.types;

import es.bsc.compss.scheduler.multiobjective.MOSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import java.util.Comparator;
import java.util.PriorityQueue;


public class PriorityActionSet {

    private final PriorityQueue<AllocatableAction> noCoreActions;
    private PriorityQueue<AllocatableAction>[] coreActions;
    private final PriorityQueue<AllocatableAction> priority;
    public final Comparator<AllocatableAction> comparator;


    @SuppressWarnings("unchecked")
    public PriorityActionSet(Comparator<AllocatableAction> comparator) {
        this.comparator = comparator;
        noCoreActions = new PriorityQueue<>(1, comparator);
        priority = new PriorityQueue<>(1, comparator);
        coreActions = new PriorityQueue[0];
    }

    @SuppressWarnings("unchecked")
    public PriorityActionSet(PriorityActionSet clone) {
        comparator = clone.comparator;
        noCoreActions = new PriorityQueue<AllocatableAction>(clone.noCoreActions);
        coreActions = new PriorityQueue[clone.coreActions.length];
        for (int idx = 0; idx < coreActions.length; idx++) {
            coreActions[idx] = new PriorityQueue<AllocatableAction>(clone.coreActions[idx]);
        }
        priority = new PriorityQueue<>(clone.priority);
    }

    @SuppressWarnings("unchecked")
    public void offer(AllocatableAction action) {
        if (((MOSchedulingInformation) action.getSchedulingInfo()).isToReschedule()) {
            Integer coreId = action.getCoreId();
            AllocatableAction currentPeek = null;
            if (coreId == null) {
                currentPeek = noCoreActions.peek();
                noCoreActions.offer(action);
            } else {
                if (coreId < coreActions.length) {
                    currentPeek = coreActions[coreId].peek();
                } else {
                    // Resize coreActions array
                    int originalSize = this.coreActions.length;
                    PriorityQueue<AllocatableAction>[] coreActions = (PriorityQueue<AllocatableAction>[]) new PriorityQueue[coreId + 1];
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

    public AllocatableAction poll() {
        AllocatableAction currentPeek;
        while ((currentPeek = priority.poll()) != null) {
            Integer coreId = currentPeek.getCoreId();
            AllocatableAction nextPeek;
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
            MOSchedulingInformation dsi = (MOSchedulingInformation) currentPeek.getSchedulingInfo();
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

    public AllocatableAction peek() {
        AllocatableAction currentPeek = priority.peek();
        while (currentPeek != null && !((MOSchedulingInformation) currentPeek.getSchedulingInfo()).isToReschedule()) {
            removeFirst(currentPeek.getCoreId());
            currentPeek = priority.peek();
        }
        return currentPeek;
    }

    public PriorityQueue<AllocatableAction> peekAll() {
        PriorityQueue<AllocatableAction> peeks = new PriorityQueue<AllocatableAction>(coreActions.length + 1, comparator);

        AllocatableAction currentCore = noCoreActions.peek();
        if (currentCore != null && !((MOSchedulingInformation) currentCore.getSchedulingInfo()).isToReschedule()) {
            noCoreActions.poll();
            currentCore = noCoreActions.peek();
        }
        if (currentCore != null) {
            peeks.offer(currentCore);
        }

        for (PriorityQueue<AllocatableAction> core : coreActions) {
            currentCore = core.peek();
            if (currentCore != null && !((MOSchedulingInformation) currentCore.getSchedulingInfo()).isToReschedule()) {
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
        AllocatableAction action = noCoreActions.peek();
        if (action != null) {
            priority.offer(action);
        }
        for (PriorityQueue<AllocatableAction> coreAction : coreActions) {
            action = coreAction.peek();
            if (action != null) {
                priority.offer(action);
            }
        }
    }

    public int size() {
        int size = 0;
        size += noCoreActions.size();
        for (PriorityQueue<AllocatableAction> pq : coreActions) {
            size += pq.size();
        }
        return size;
    }

    public boolean isEmpty() {
        return size() == 0;
    }

    public void remove(AllocatableAction action) {
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
