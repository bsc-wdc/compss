package integratedtoolkit.scheduler.multiobjective.types;

import integratedtoolkit.scheduler.multiobjective.MOResourceScheduler;
import integratedtoolkit.scheduler.multiobjective.MOScheduleOptimizer;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import java.util.PriorityQueue;


public class OptimizationWorker {

    private MOResourceScheduler<WorkerResourceDescription> resource;
    private PriorityQueue<AllocatableAction> donorActions;


    public OptimizationWorker(MOResourceScheduler<WorkerResourceDescription> resource) {
        this.resource = resource;
    }

    public void localOptimization(long optimizationTS) {
        donorActions = resource.localOptimization(optimizationTS, MOScheduleOptimizer.getSelectionComparator(),
                MOScheduleOptimizer.getDonationComparator());
    }

    public AllocatableAction pollDonorAction() {
        return donorActions.poll();
    }

    public long getDonationIndicator() {
        return resource.getLastGapExpectedStart();
    }

    public String getName() {
        return resource.getName();
    }

    public MOResourceScheduler<WorkerResourceDescription> getResource() {
        return resource;
    }

}
