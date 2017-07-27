package es.bsc.compss.scheduler.multiobjective.types;

import es.bsc.compss.scheduler.multiobjective.MOResourceScheduler;
import es.bsc.compss.scheduler.multiobjective.MOScheduleOptimizer;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.resources.WorkerResourceDescription;
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
