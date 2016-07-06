package integratedtoolkit.util;

import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.LinkedList;

public class ResourceScheduler<P extends Profile, T extends WorkerResourceDescription> {

    //Task running in the resource
    private final LinkedList<AllocatableAction<P, T>> running;

    //Task without enough resources to be executed right now
    private final LinkedList<AllocatableAction<P, T>> blocked;

    //Profile information of the task executions
    private Profile[][] profiles;

    protected final Worker<T> myWorker;

    public ResourceScheduler(Worker<T> w) {
        int coreCount = CoreManager.getCoreCount();
        profiles = new Profile[coreCount][];
        for (int coreId = 0; coreId < coreCount; ++coreId) {
            int implCount = CoreManager.getCoreImplementations(coreId).length;
            profiles[coreId] = new Profile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                profiles[coreId][implId] = generateProfileForAllocatable();
            }
        }
        running = new LinkedList<AllocatableAction<P, T>>();
        blocked = new LinkedList<AllocatableAction<P, T>>();
        myWorker = w;
    }

    public final String getName() {
        return myWorker.getName();
    }

    public final Worker<T> getResource() {
        return myWorker;
    }

    public LinkedList<Integer> getExecutableCores() {
        return myWorker.getExecutableCores();
    }

    public LinkedList<Implementation<T>>[] getExecutableImpls() {
        return myWorker.getExecutableImpls();
    }

    public LinkedList<Implementation<T>> getExecutableImpls(int id) {
        return myWorker.getExecutableImpls(id);
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * --------- ACTION PROFILE MANAGEMENT -----------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public void updatedCoreElements() {
        int oldCoreCount = this.profiles.length;
        int newCoreCount = CoreManager.getCoreCount();
        Profile[][] profiles = new Profile[newCoreCount][0];
        for (int coreId = 0; coreId < newCoreCount; coreId++) {
            int oldImplCount = this.profiles[coreId].length;
            int newImplCount = CoreManager.getCoreImplementations(coreId).length;
            profiles[coreId] = (Profile[]) (new Profile[newImplCount]);
            int implId = 0;
            if (coreId < oldCoreCount) {
                //There were previous implementations. Copy old Implementations
                for (; implId < oldImplCount; implId++) {
                    profiles[coreId][implId] = this.profiles[coreId][implId];
                }
            } else {
                //Its a newly detected CE. Do Nothing.
            }
            for (; implId < newImplCount; implId++) {
                profiles[coreId][implId] = generateProfileForAllocatable();
            }
        }
        this.profiles = profiles;
    }

    public final P getProfile(Implementation<T> impl) {
        if (impl != null) {
            if (impl.getCoreId() != null) {
                return (P) profiles[impl.getCoreId()][impl.getImplementationId()];
            }
        }
        return null;
    }

    public final void profiledExecution(Implementation<T> impl, Profile profile) {
        if (impl != null) {
            int coreId = impl.getCoreId();
            int implId = impl.getImplementationId();
            profiles[coreId][implId].accumulate(profile);
        }

    }

    /**
     * Generates a Profile for an action.
     *
     * @return a profile object for an action.
     */
    public P generateProfileForAllocatable() {
        return (P) new Profile();
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * --------- RUNNING ACTIONS MANAGEMENT -----------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public final void hostAction(AllocatableAction<P, T> action) {
        running.add(action);
    }

    public final LinkedList<AllocatableAction<P, T>> getHostedActions() {
        return running;
    }

    public final LinkedList<AllocatableAction<P, T>> getBlockedActions() {
        return blocked;
    }

    public final void unhostAction(AllocatableAction<P, T> action) {
        running.remove(action);
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * --------- BLOCKED ACTIONS MANAGEMENT -----------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public final void waitOnResource(AllocatableAction<P, T> action) {
        blocked.add(action);
    }

    public final boolean hasBlockedActions() {
        return blocked.size() > 0;
    }

    public final AllocatableAction<P, T> getFirstBlocked() {
        return blocked.getFirst();
    }

    public final void removeFirstBlocked() {
        blocked.poll();
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * ------ RESOURCE - IMPLEMENTATION SCORE ---------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public Score getResourceScore(AllocatableAction<P, T> action, TaskParams params, Score actionScore) {
        long resourceScore = Score.getLocalityScore(params, myWorker);
        return new Score(actionScore, resourceScore, 0);

    }

    public Score getImplementationScore(AllocatableAction<P, T> action, TaskParams params, Implementation<T> impl, Score resourceScore) {
        long implScore = this.getProfile(impl).getAverageExecutionTime();
        return new Score(resourceScore, implScore);
    }

    public void initialSchedule(AllocatableAction<P, T> action) {
        //Assign no resource dependencies. The worker will automatically block 
        //the tasks when there are not enough resources available.
    }

    public LinkedList<AllocatableAction<P, T>> unscheduleAction(AllocatableAction<P, T> action) {
        return new LinkedList<AllocatableAction<P, T>>();
    }

    public final void cancelAction(AllocatableAction<P, T> action) {
        blocked.remove(action);
        unscheduleAction(action);
    }

    public String toString() {
        try {
            return "ResourceScheduler@" + getName();
        } catch (NullPointerException ne) {
            return super.toString();
        }
    }

    public void clear() {
        running.clear();
        blocked.clear();
        myWorker.releaseAllResources();
    }
}
