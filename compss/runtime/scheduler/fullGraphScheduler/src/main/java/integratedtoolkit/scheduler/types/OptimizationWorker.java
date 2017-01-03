package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.ScheduleOptimizer;
import integratedtoolkit.scheduler.types.AllocatableAction;

import java.util.PriorityQueue;


public class OptimizationWorker {

    private FullGraphResourceScheduler resource;
    private PriorityQueue<AllocatableAction> donorActions;


    public OptimizationWorker(FullGraphResourceScheduler resource) {
        this.resource = resource;
    }

    public void localOptimization(long optimizationTS) {
        donorActions = resource.localOptimization(optimizationTS, 
                ScheduleOptimizer.getSelectionComparator(),
                ScheduleOptimizer.getDonationComparator());
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

    public FullGraphResourceScheduler getResource() {
        return resource;
    }

}
