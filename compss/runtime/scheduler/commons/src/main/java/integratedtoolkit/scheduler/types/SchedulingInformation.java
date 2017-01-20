package integratedtoolkit.scheduler.types;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;

import java.util.LinkedList;
import java.util.List;


public class SchedulingInformation<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    @SuppressWarnings("unchecked")
    private static final LinkedList<ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>>[] coreToWorkers = new LinkedList[CoreManager
            .getCoreCount()];

    static {
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            coreToWorkers[coreId] = new LinkedList<>();
        }
    }

    // Execution Information
    private final List<AllocatableAction<P, T, I>> constrainingPredecessors;


    public SchedulingInformation() {
        constrainingPredecessors = new LinkedList<AllocatableAction<P, T, I>>();
    }

    public void addResourceConstraint(AllocatableAction<P, T, I> predecessor) {
        constrainingPredecessors.add(predecessor);
    }

    public List<AllocatableAction<P, T, I>> getConstrainingPredecessors() {
        return constrainingPredecessors;
    }

    @SuppressWarnings("unchecked")
    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> void changesOnWorker(
            ResourceScheduler<P, T, I> ui) {
        for (LinkedList<ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>> coreToWorker : coreToWorkers) {
            coreToWorker.remove(ui);
        }
        LinkedList<Integer> executableCores = ui.getExecutableCores();
        for (int coreId : executableCores) {
            coreToWorkers[coreId]
                    .add((ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>) ui);
        }
    }

    @SuppressWarnings("unchecked")
    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> LinkedList<ResourceScheduler<P, T, I>> getCoreElementExecutors(
            int coreId) {
        LinkedList<ResourceScheduler<P, T, I>> res = new LinkedList<>();
        for (ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> rs : coreToWorkers[coreId]) {
            res.add((ResourceScheduler<P, T, I>) rs);
        }
        return res;
    }

    public boolean isExecutable() {
        return true;
    }

}
