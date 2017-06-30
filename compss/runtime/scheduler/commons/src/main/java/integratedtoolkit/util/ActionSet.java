package integratedtoolkit.util;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;


public class ActionSet {

    private final List<AllocatableAction> noCore;
    private List<AllocatableAction>[] coreIndexed;
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
    }

    public int[] getActionCounts() {
        return this.counts;
    }

    public List<AllocatableAction> getActions(Integer coreId) {
        if (coreId == null) {
            return this.noCore;
        } else {
            return this.coreIndexed[coreId];
        }
    }

    public List<AllocatableAction> getAllActions() {
        List<AllocatableAction> runnable = new LinkedList<>();
        runnable.addAll(this.noCore);

        for (int core = 0; core < this.coreIndexed.length; ++core) {
            runnable.addAll(coreIndexed[core]);
        }
        return runnable;
    }

    public void removeAction(AllocatableAction action) {
        Integer coreId = action.getCoreId();
        if (coreId == null) {
            this.noCore.remove(action);
        } else {
            this.coreIndexed[coreId].remove(action);
            this.counts[coreId]--;
        }
    }

    public <T extends WorkerResourceDescription> List<AllocatableAction> removeAllCompatibleActions(Worker<T> r) {
        List<AllocatableAction> runnable = new LinkedList<>();
        Iterator<AllocatableAction> actions = this.noCore.iterator();
        while (actions.hasNext()) {
            AllocatableAction action = actions.next();
            if (action.isCompatible(r)) {
                actions.remove();
                runnable.add(action);
            }
        }

        List<Integer> executableCores = r.getExecutableCores();
        for (int core : executableCores) {
            runnable.addAll(coreIndexed[core]);
            this.coreIndexed[core] = new LinkedList<>();
            this.counts[core] = 0;
        }
        return runnable;
    }

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
        return runnable;
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
