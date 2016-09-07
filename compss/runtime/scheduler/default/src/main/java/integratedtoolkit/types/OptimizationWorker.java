package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultResourceScheduler;
import integratedtoolkit.scheduler.defaultscheduler.ScheduleOptimizer;
import integratedtoolkit.scheduler.types.AllocatableAction;

import java.util.PriorityQueue;


public class OptimizationWorker {

    private DefaultResourceScheduler resource;
    private PriorityQueue<AllocatableAction> donorActions;


    public OptimizationWorker(DefaultResourceScheduler resource) {
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

    public DefaultResourceScheduler getResource() {
        return resource;
    }

}
