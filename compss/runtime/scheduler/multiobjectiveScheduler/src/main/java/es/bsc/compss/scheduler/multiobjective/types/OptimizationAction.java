package es.bsc.compss.scheduler.multiobjective.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.multiobjective.MOSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;

import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import java.util.LinkedList;


public class OptimizationAction extends AllocatableAction {

    public OptimizationAction() {
        super(new MOSchedulingInformation(null), null);
        MOSchedulingInformation asi = (MOSchedulingInformation) this.getSchedulingInfo();
        asi.scheduled();
        asi.setOnOptimization(true);
        asi.setExpectedStart(0);
        asi.setExpectedEnd(Long.MAX_VALUE);
    }

    @Override
    protected void doAction() {

    }

    @Override
    protected void doCompleted() {

    }

    @Override
    protected void doError() throws FailedActionException {

    }

    @Override
    protected void doFailed() {

    }

    @Override
    public Integer getCoreId() {
        return null;
    }

    @Override
    public LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers() {
        return null;
    }

    @Override
    public Implementation[] getImplementations() {
        return null;
    }

    @Override
    public <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r) {
        return true;
    }

    @Override
    public <T extends WorkerResourceDescription> LinkedList<Implementation> getCompatibleImplementations(ResourceScheduler<T> r) {
        return null;
    }

    @Override
    public int getPriority() {
        return 0;
    }

    @Override
    public <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker, Score actionScore) {
        return null;
    }

    @Override
    public void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException {

    }
    
    @Override
    public void tryToSchedule(Score actionScore) throws BlockedActionException, UnassignedActionException {
        
    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public <T extends WorkerResourceDescription> void schedule(ResourceScheduler<T> targetWorker, Implementation impl)
            throws BlockedActionException, UnassignedActionException {

    }

    @Override
    public boolean isToReserveResources() {
        return true;
    }

    @Override
    public boolean isToReleaseResources() {
        return true;
    }

    @Override
    public String toString() {
        return "Scheduling blocking action";
    }

}
