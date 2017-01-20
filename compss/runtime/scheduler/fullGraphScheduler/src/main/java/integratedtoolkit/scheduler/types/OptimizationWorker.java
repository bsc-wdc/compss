package integratedtoolkit.scheduler.types;

import integratedtoolkit.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import integratedtoolkit.scheduler.fullGraphScheduler.ScheduleOptimizer;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.PriorityQueue;


public class OptimizationWorker<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    private FullGraphResourceScheduler<P,T,I> resource;
    private PriorityQueue<AllocatableAction<P,T,I>> donorActions;


    public OptimizationWorker(FullGraphResourceScheduler<P,T,I> resource) {
        this.resource = resource;
    }

    public void localOptimization(long optimizationTS) {
        donorActions = resource.localOptimization(optimizationTS, 
                ScheduleOptimizer.getSelectionComparator(),
                ScheduleOptimizer.getDonationComparator());
    }

    public AllocatableAction<P,T,I> pollDonorAction() {
        return donorActions.poll();
    }

    public long getDonationIndicator() {
        return resource.getLastGapExpectedStart();
    }

    public String getName() {
        return resource.getName();
    }

    public FullGraphResourceScheduler<P,T,I> getResource() {
        return resource;
    }

}
