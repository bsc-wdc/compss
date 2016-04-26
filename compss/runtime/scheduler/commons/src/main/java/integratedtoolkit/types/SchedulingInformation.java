package integratedtoolkit.types;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceScheduler;
import java.util.LinkedList;

public class SchedulingInformation {

    private static final LinkedList<ResourceScheduler>[] coreToWorkers;

    static {
        coreToWorkers = new LinkedList[CoreManager.getCoreCount()];
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            coreToWorkers[coreId] = new LinkedList();
        }
    }

    //Execution Information
    private AllocatableAction constrainingPredecessor = null;

    public SchedulingInformation() {
        constrainingPredecessor = null;
    }

    public void setResourceConstraint(AllocatableAction predecessor) {
        constrainingPredecessor = predecessor;
    }

    public AllocatableAction getConstrainingPredecessor() {
        return constrainingPredecessor;
    }

    public static void changesOnWorker(ResourceScheduler ui) {
        for (LinkedList<ResourceScheduler> coreToWorker : coreToWorkers) {
            coreToWorker.remove(ui);
        }
        LinkedList<Integer> executableCores = ui.getExecutableCores();
        for (int coreId : executableCores) {
            coreToWorkers[coreId].add(ui);
        }
    }

    public LinkedList<ResourceScheduler> getCoreElementExecutors(int coreId) {
        return coreToWorkers[coreId];
    }

    public boolean isExecutable() {
        return true;
    }
}
