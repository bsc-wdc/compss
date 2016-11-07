package integratedtoolkit.types;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceScheduler;

import java.util.LinkedList;
import java.util.List;


public class SchedulingInformation<P extends Profile, T extends WorkerResourceDescription> {

    @SuppressWarnings("unchecked")
    private static final LinkedList<ResourceScheduler<?, ?>>[] coreToWorkers = new LinkedList[CoreManager.getCoreCount()];

    static {
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            coreToWorkers[coreId] = new LinkedList<>();
        }
    }

    // Execution Information
    private final List<AllocatableAction<P, T>> constrainingPredecessors;


    public SchedulingInformation() {
        constrainingPredecessors = new LinkedList<AllocatableAction<P, T>>();
    }

    public void addResourceConstraint(AllocatableAction<P, T> predecessor) {
        constrainingPredecessors.add(predecessor);
    }

    public List<AllocatableAction<P, T>> getConstrainingPredecessors() {
        return constrainingPredecessors;
    }

    public static void changesOnWorker(ResourceScheduler<?, ?> ui) {
        for (LinkedList<ResourceScheduler<?, ?>> coreToWorker : coreToWorkers) {
            coreToWorker.remove(ui);
        }
        LinkedList<Integer> executableCores = ui.getExecutableCores();
        for (int coreId : executableCores) {
            coreToWorkers[coreId].add(ui);
        }
    }

    public LinkedList<ResourceScheduler<?, ?>> getCoreElementExecutors(int coreId) {
        return coreToWorkers[coreId];
    }

    public boolean isExecutable() {
        return true;
    }

}
