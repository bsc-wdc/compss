package integratedtoolkit.util;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;


public class ActionSet<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    private final LinkedList<AllocatableAction<P, T, I>> noCore;
    private LinkedList<AllocatableAction<P, T, I>>[] coreIndexed;
    private int[] counts;


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
    }

    @SuppressWarnings("unchecked")
    public void updateCoreCount() {
        int oldCoreCount = coreIndexed.length;
        int newCoreCount = CoreManager.getCoreCount();
        
        LinkedList<AllocatableAction<P, T, I>>[] coreIndexed = new LinkedList[newCoreCount];
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

    public void addAction(AllocatableAction<P, T, I> aa) {
        Implementation<?>[] impls = aa.getImplementations();
        if (impls.length == 0) {
            this.noCore.add(aa);
        } else {
            int core = impls[0].getCoreId();
            this.coreIndexed[core].add(aa);
            this.counts[core]++;
        }
    }

    public LinkedList<AllocatableAction<P, T, I>> removeAllCompatibleActions(Worker<T, I> r) {
        LinkedList<AllocatableAction<P, T, I>> runnable = new LinkedList<>();
        Iterator<AllocatableAction<P, T, I>> actions = this.noCore.iterator();
        while (actions.hasNext()) {
            AllocatableAction<P, T, I> action = actions.next();
            if (action.isCompatible(r)) {
                actions.remove();
                runnable.add(action);
            }
        }

        LinkedList<Integer> executableCores = r.getExecutableCores();
        for (int core : executableCores) {
            runnable.addAll(coreIndexed[core]);
            this.coreIndexed[core] = new LinkedList<>();
            this.counts[core] = 0;
        }
        return runnable;
    }

    public int[] getActionCounts() {
        return this.counts;
    }

    public LinkedList<AllocatableAction<P, T, I>> getActions(Integer coreId) {
        if (coreId == null) {
            return this.noCore;
        } else {
            return this.coreIndexed[coreId];
        }
    }

    public void removeAction(AllocatableAction<P, T, I> action) {
        Integer coreId = action.getCoreId();
        if (coreId == null) {
            this.noCore.remove(action);
        } else {
            this.coreIndexed[coreId].remove(action);
            this.counts[coreId]--;
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder(super.toString() + "\n");
        sb.append("no Core (").append(noCore.size()).append(")-> ").append(noCore).append("\n");
        for (int coreId = 0; coreId < coreIndexed.length; coreId++) {
            sb.append("Core ").append(coreId).append(" (").append(counts[coreId]).append(") -> ").append(coreIndexed[coreId]).append("\n");
        }

        return sb.toString();
    }

}
