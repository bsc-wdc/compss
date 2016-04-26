package integratedtoolkit.util;

import integratedtoolkit.api.ITExecution;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.data.DataAccessId;
import integratedtoolkit.types.data.DataInstanceId;
import integratedtoolkit.types.parameter.DependencyParameter;
import integratedtoolkit.types.parameter.Parameter;
import integratedtoolkit.types.resources.Resource;
import integratedtoolkit.types.resources.Worker;
import java.util.HashSet;
import java.util.LinkedList;

public class ResourceScheduler<P extends Profile> {

    //Task running in the resource
    private final LinkedList<AllocatableAction> running;

    //Task without enough resources to be executed right now
    private final LinkedList<AllocatableAction> blocked;

    //Profile information of the task executions
    private Profile[][] profiles;

    protected final Worker myWorker;

    public ResourceScheduler(Worker w) {
        int coreCount = CoreManager.getCoreCount();
        profiles = new Profile[coreCount][];
        for (int coreId = 0; coreId < coreCount; ++coreId) {
            int implCount = CoreManager.getCoreImplementations(coreId).length;
            profiles[coreId] = new Profile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                profiles[coreId][implId] = generateProfileForAllocatable();
            }
        }
        running = new LinkedList();
        blocked = new LinkedList();
        myWorker = w;
    }

    public final String getName() {
        return myWorker.getName();
    }

    public final Worker getResource() {
        return myWorker;
    }

    public LinkedList<Integer> getExecutableCores() {
        return myWorker.getExecutableCores();
    }

    public LinkedList<Implementation>[] getExecutableImpls() {
        return myWorker.getExecutableImpls();
    }

    public LinkedList<Implementation> getExecutableImpls(int id) {
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

    public final P getProfile(Implementation impl) {
        if (impl != null) {
            return (P) profiles[impl.getCoreId()][impl.getImplementationId()];
        }
        return null;
    }

    public final void profiledExecution(Implementation impl, Profile profile) {
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
    public final void hostAction(AllocatableAction action) {
        running.add(action);
    }

    public final LinkedList<AllocatableAction> getHostedActions() {
        return running;
    }

    public final void unhostAction(AllocatableAction action) {
        running.remove(action);
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * --------- BLOCKED ACTIONS MANAGEMENT -----------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public final void waitOnResource(AllocatableAction action) {
        blocked.add(action);
    }

    public final boolean hasBlockedTasks() {
        return blocked.size() > 0;
    }

    public final AllocatableAction getFirstBlocked() {
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
    public Score getResourceScore(AllocatableAction action, TaskParams params, Score actionScore) {
        long resourceScore = Score.getLocalityScore(params, myWorker);
        return new Score(actionScore, resourceScore, 0);

    }

    public Score getImplementationScore(AllocatableAction action, TaskParams params, Implementation impl, Score resourceScore) {
        long implScore = this.getProfile(impl).getAverageExecutionTime();
        return new Score(resourceScore, implScore);
    }

    public void initialSchedule(AllocatableAction action, Implementation bestImpl) {
        //Assign no resource dependencies. The worker will automatically block 
        //the tasks when there are not enough resources available.
    }

    public LinkedList<AllocatableAction> unscheduleAction(AllocatableAction action) {
        return new LinkedList();
    }

    public final void cancelAction(AllocatableAction action) {
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
    }
}
