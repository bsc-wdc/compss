package integratedtoolkit.util;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.resources.Worker;
import java.util.Iterator;
import java.util.LinkedList;

public class ActionSet {

    private LinkedList<AllocatableAction> noCore;
    private LinkedList<AllocatableAction>[] coreIndexed;
    private int[] counts;

    public ActionSet() {
        noCore = new LinkedList<AllocatableAction>();
        int coreCount = CoreManager.getCoreCount();
        coreIndexed = new LinkedList[coreCount];
        counts = new int[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            coreIndexed[coreId] = new LinkedList();
            counts[coreId] = 0;
        }
    }

    public void updateCoreCount() {
        int oldCoreCount = coreIndexed.length;
        int newCoreCount = CoreManager.getCoreCount();
        LinkedList<AllocatableAction>[] coreIndexed = new LinkedList[newCoreCount];
        int[] counts = new int[newCoreCount];
        int coreId = 0;
        for (; coreId < oldCoreCount; coreId++) {
            coreIndexed[coreId] = this.coreIndexed[coreId];
            counts[coreId] = this.counts[coreId];
        }
        for (; coreId < newCoreCount; coreId++) {
            coreIndexed[coreId] = new LinkedList<AllocatableAction>();
            counts[coreId] = 0;
        }
        this.coreIndexed = coreIndexed;
        this.counts = counts;
    }

    public void addAction(AllocatableAction aa) {
        Implementation[] impls = aa.getImplementations();
        if (impls.length == 0) {
            noCore.add(aa);
        } else {
            int core = impls[0].getCoreId();
            coreIndexed[core].add(aa);
            counts[core]++;
        }
    }

    public LinkedList<AllocatableAction> removeAllCompatibleActions(Worker r) {
        LinkedList<AllocatableAction> runnable = new LinkedList();
        Iterator<AllocatableAction> actions = noCore.iterator();
        while (actions.hasNext()) {
            AllocatableAction action = actions.next();
            if (action.isCompatible(r)) {
                actions.remove();
                runnable.add(action);
            }
        }

        LinkedList<Integer> executableCores = r.getExecutableCores();
        for (int core : executableCores) {
            runnable.addAll(coreIndexed[core]);
            coreIndexed[core] = new LinkedList<AllocatableAction>();
            counts[core] = 0;
        }
        return runnable;
    }

    public int[] getActionCounts() {
        return counts;
    }

    public LinkedList<AllocatableAction> getActions(Integer coreId) {
        if (coreId == null) {
            return noCore;
        } else {
            return coreIndexed[coreId];
        }
    }

    public void removeAction(AllocatableAction action) {
        Integer coreId = action.getCoreId();
        if (coreId == null) {
            noCore.remove(action);
        } else {
            coreIndexed[coreId].remove(action);
            counts[coreId]--;
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
