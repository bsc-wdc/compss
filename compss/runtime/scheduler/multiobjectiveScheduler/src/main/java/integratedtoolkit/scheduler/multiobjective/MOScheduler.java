package integratedtoolkit.scheduler.multiobjective;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.scheduler.multiobjective.types.MOProfile;
import integratedtoolkit.scheduler.multiobjective.types.MOScore;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.ObjectValue;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceOptimizer;
import java.util.Collection;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.PriorityQueue;
import org.json.JSONObject;

public class MOScheduler extends TaskScheduler {

    private final MOScore dummyScore = new MOScore(0, 0, 0, 0, 0, 0);
    private final MOScheduleOptimizer schedOptimizer = new MOScheduleOptimizer(this);

    public MOScheduler() {
        schedOptimizer.start();
    }

    @Override
    public MOProfile generateProfile() {
        return new MOProfile();
    }

    @Override
    public <T extends WorkerResourceDescription> MOResourceScheduler generateSchedulerForResource(Worker<T> w, JSONObject json) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate scheduler for resource " + w.getName());
        return new MOResourceScheduler<>(w, json);
    }

    @Override
    public <T extends WorkerResourceDescription> MOSchedulingInformation generateSchedulingInformation(ResourceScheduler<T> enforcedTargetResource) {
        return new MOSchedulingInformation(enforcedTargetResource);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        long actionScore = MOScore.getActionScore(action);
        long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
        return new MOScore(actionScore, dataTime, 0, 0, 0, 0);
    }

    @Override
    public void shutdown() {
        super.shutdown();
        Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers = this.getWorkers();
        System.out.println("End Profiles:");
        for (ResourceScheduler worker : workers) {
            System.out.println("\t" + worker.getName());
            for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
                for (Implementation impl : CoreManager.getCoreImplementations(coreId)) {
                    System.out.println("\t\t" + CoreManager.getSignature(coreId, impl.getImplementationId()));
                    MOProfile profile = (MOProfile) worker.getProfile(impl);
                    System.out.println("\t\t\tTime " + profile.getAverageExecutionTime() + " ms");
                    System.out.println("\t\t\tPower " + profile.getPower() + " W");
                    System.out.println("\t\t\tCost " + profile.getPrice() + " €");
                }
            }
        }
        try {
            schedOptimizer.shutdown();
            //Ascetic.stop();
        } catch (InterruptedException ie) {
            //No need to do anything.
        }
    }

    @Override
    public ResourceOptimizer generateResourceOptimizer() {
        return new MOResourceOptimizer(this);
    }

    /**
     * Notifies to the scheduler that some actions have become free of data
     * dependencies or resource dependencies.
     *
     * @param <T>
     * @param dataFreeActions IN, list of actions free of data dependencies
     * @param resourceFreeActions IN, list of actions free of resource
     * dependencies
     * @param blockedCandidates OUT, list of blocked candidates
     * @param resource Resource where the previous task was executed
     */
    @Override
    public <T extends WorkerResourceDescription> void handleDependencyFreeActions(
            LinkedList<AllocatableAction> dataFreeActions,
            LinkedList<AllocatableAction> resourceFreeActions,
            LinkedList<AllocatableAction> blockedCandidates,
            ResourceScheduler<T> resource) {

        HashSet<AllocatableAction> freeTasks = new HashSet<>();
        freeTasks.addAll(dataFreeActions);
        freeTasks.addAll(resourceFreeActions);
        for (AllocatableAction freeAction : freeTasks) {
            tryToLaunch(freeAction);
        }
    }
}
