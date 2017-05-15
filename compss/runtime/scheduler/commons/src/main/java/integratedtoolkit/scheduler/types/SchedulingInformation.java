package integratedtoolkit.scheduler.types;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;

import java.util.LinkedList;
import java.util.List;


public class SchedulingInformation<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    // List of active resources per core
    private static final LinkedList<LinkedList<ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>>> coreToWorkers = new LinkedList<>();
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

    public static void updateCoreCount(int newCoreCount) {
        for (int currentCoreCount = coreToWorkers.size(); currentCoreCount < newCoreCount; ++currentCoreCount) {
            // Add empty workers list to new core entry
            coreToWorkers.add(new LinkedList<>());
        }
    }

    public boolean isExecutable() {
        return true;
    }

    @SuppressWarnings("unchecked")
    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> void changesOnWorker(
            ResourceScheduler<P, T, I> ui) {

        // Remove the previous description of the worker
        for (LinkedList<ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>> coreToWorker : coreToWorkers) {
            coreToWorker.remove(ui);
        }
        
        // Update registered coreElements
        SchedulingInformation.updateCoreCount(CoreManager.getCoreCount());
        
        // Add the new description of the worker
        LinkedList<Integer> executableCores = ui.getExecutableCores();
        for (int coreId : executableCores) {
            coreToWorkers.get(coreId)
                    .add((ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>>) ui);
        }
    }

    @SuppressWarnings("unchecked")
    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> LinkedList<ResourceScheduler<P, T, I>> getCoreElementExecutors(
            int coreId) {

        LinkedList<ResourceScheduler<P, T, I>> res = new LinkedList<>();
        for (ResourceScheduler<Profile, WorkerResourceDescription, Implementation<WorkerResourceDescription>> rs : coreToWorkers
                .get(coreId)) {
            res.add((ResourceScheduler<P, T, I>) rs);
        }
        return res;
    }

}
