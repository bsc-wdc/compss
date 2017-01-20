package integratedtoolkit.components.impl;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * 
 * Scheduler representation for a given worker
 *
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class ResourceScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Task running in the resource
    private final LinkedList<AllocatableAction<P, T, I>> running;
    // Task without enough resources to be executed right now
    protected final PriorityQueue<AllocatableAction<P, T, I>> blocked;

    // Worker assigned to the resource scheduler
    protected final Worker<T, I> myWorker;

    // Profile information of the task executions
    private Profile[][] profiles;
    private int[] coreExecutionCounter;


    /**
     * Constructs a new Resource Scheduler associated to the worker @w
     * 
     * @param w
     */
    public ResourceScheduler(Worker<T, I> w) {
        this.running = new LinkedList<>();
        this.blocked = new PriorityQueue<>(20, new Comparator<AllocatableAction<P, T, I>>() {

            @Override
            public int compare(AllocatableAction<P, T, I> a1, AllocatableAction<P, T, I> a2) {
                Score score1 = generateBlockedScore(a1);
                Score score2 = generateBlockedScore(a2);
                return score1.compareTo(score2);
            }
        });
        this.myWorker = w;

        int coreCount = CoreManager.getCoreCount();
        this.coreExecutionCounter = new int[coreCount];
        this.profiles = new Profile[coreCount][];
        for (int coreId = 0; coreId < coreCount; ++coreId) {
            int implCount = CoreManager.getCoreImplementations(coreId).length;
            this.profiles[coreId] = new Profile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                this.profiles[coreId][implId] = generateProfileForAllocatable();
            }
        }
    }

    /**
     * Returns the worker name
     * 
     * @return
     */
    public final String getName() {
        return this.myWorker.getName();
    }

    /**
     * Returns the worker resource
     * 
     * @return
     */
    public final Worker<T, I> getResource() {
        return this.myWorker;
    }

    /**
     * Returns the coreElements that can be executed by the resource
     * 
     * @return
     */
    public final LinkedList<Integer> getExecutableCores() {
        return this.myWorker.getExecutableCores();
    }

    /**
     * Returns the implementations that can be executed by the resource
     * 
     * @return
     */
    public final LinkedList<I>[] getExecutableImpls() {
        return this.myWorker.getExecutableImpls();
    }

    /**
     * Returns the implementations of the core @id that can be executed by the resource
     * 
     * @param id
     * @return
     */
    public final LinkedList<I> getExecutableImpls(int id) {
        return this.myWorker.getExecutableImpls(id);
    }

    /*
     * ***************************************************************************************************************
     * ACTION PROFILE MANAGEMENT
     * ***************************************************************************************************************
     */
    /**
     * Updates the coreElement structures
     * 
     */
    public final void updatedCoreElements() {
        int oldCoreCount = this.profiles.length;
        int newCoreCount = CoreManager.getCoreCount();
        Profile[][] profiles = new Profile[newCoreCount][0];
        for (int coreId = 0; coreId < newCoreCount; coreId++) {
            int oldImplCount = this.profiles[coreId].length;
            int newImplCount = CoreManager.getCoreImplementations(coreId).length;
            profiles[coreId] = (Profile[]) (new Profile[newImplCount]);
            int implId = 0;
            if (coreId < oldCoreCount) {
                // There were previous implementations. Copy old Implementations
                for (; implId < oldImplCount; implId++) {
                    profiles[coreId][implId] = this.profiles[coreId][implId];
                }
            } else {
                // Its a newly detected CE. Do Nothing.
            }
            for (; implId < newImplCount; implId++) {
                profiles[coreId][implId] = generateProfileForAllocatable();
            }
        }
        this.profiles = profiles;

        int[] newCoreExecutionCounter = new int[newCoreCount];
        System.arraycopy(this.coreExecutionCounter, 0, newCoreExecutionCounter, 0, this.coreExecutionCounter.length);
        this.coreExecutionCounter = newCoreExecutionCounter;
    }

    /**
     * Generates a Profile for an action.
     *
     * @return a profile object for an action.
     */
    @SuppressWarnings("unchecked")
    public P generateProfileForAllocatable() {
        return (P) new Profile();
    }

    /**
     * Returns the profile for a given implementation @impl
     * 
     * @param impl
     * @return
     */
    @SuppressWarnings("unchecked")
    public final P getProfile(I impl) {
        if (impl != null) {
            if (impl.getCoreId() != null) {
                return (P) profiles[impl.getCoreId()][impl.getImplementationId()];
            }
        }
        return null;
    }

    /**
     * Updates the execution profile of implementation @impl by accumulating the profile @profile
     * 
     * @param impl
     * @param profile
     */
    public final void profiledExecution(I impl, P profile) {
        if (impl != null) {
            int coreId = impl.getCoreId();
            int implId = impl.getImplementationId();
            this.profiles[coreId][implId].accumulate(profile);
        }

    }

    /*
     * ***************************************************************************************************************
     * RUNNING ACTIONS MANAGEMENT
     * ***************************************************************************************************************
     */
    /**
     * Returns the number of tasks of type @taskId that this resource has ever executed
     * 
     * @param taskId
     * @return
     */
    public final int getNumTasks(int taskId) {
        if (taskId < this.coreExecutionCounter.length) {
            return this.coreExecutionCounter[taskId];
        }

        return -1;
    }

    /**
     * Returns all the hosted actions
     * 
     * @return
     */
    public final LinkedList<AllocatableAction<P, T, I>> getHostedActions() {
        return this.running;
    }

    /**
     * Adds a new running action on the resource
     * 
     * @param action
     */
    public final void hostAction(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[ResourceScheduler] Host action " + action);
        this.running.add(action);

        // Increase the task count counter if possible
        Integer taskId = action.getCoreId();
        if (taskId != null && taskId < this.coreExecutionCounter.length) {
            ++this.coreExecutionCounter[taskId];
        }
    }

    /**
     * Removes a running action
     * 
     * @param action
     */
    public final void unhostAction(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[ResourceScheduler] Unhost action " + action + " on resource " + getName());
        this.running.remove(action);

        // Decrease the task count counter if possible
        Integer taskId = action.getCoreId();
        if (taskId != null && taskId < this.coreExecutionCounter.length) {
            --this.coreExecutionCounter[taskId];
        }
    }

    /*
     * ***************************************************************************************************************
     * BLOCKED ACTIONS MANAGEMENT
     * ***************************************************************************************************************
     */
    /**
     * Adds a blocked action on this worker
     * 
     * @param action
     */
    public final void waitOnResource(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[ResourceScheduler] Block action " + action + " on resource " + getName());
        this.blocked.add(action);
    }

    /**
     * Returns if there are blocked actions or not
     * 
     * @return
     */
    public final boolean hasBlockedActions() {
        return this.blocked.size() > 0;
    }

    /**
     * Returns all the blocked actions
     * 
     * @return
     */
    public final PriorityQueue<AllocatableAction<P, T, I>> getBlockedActions() {
        return this.blocked;
    }

    /**
     * Returns the first blocked action without removing it
     * 
     * @return
     */
    public final AllocatableAction<P, T, I> getFirstBlocked() {
        return this.blocked.peek();
    }

    /**
     * Removes the first blocked action
     * 
     */
    public final void removeFirstBlocked() {
        this.blocked.poll();
    }

    /**
     * Tries to launch blocked actions on resource. When an action cannot be launched, its successors are not tried
     * 
     */
    public final void tryToLaunchBlockedActions() {
        LOGGER.debug("[ResourceScheduler] Try to launch blocked actions on resource " + getName());
        while (this.hasBlockedActions()) {
            AllocatableAction<P, T, I> firstBlocked = this.getFirstBlocked();
            if (firstBlocked.areEnoughResources()) {
                this.removeFirstBlocked();
                LOGGER.info(firstBlocked + " execution resumed on worker " + this.getName());
                firstBlocked.lockAction();
                firstBlocked.run();
            } else {
                break;
            }
        }
    }

    /*
     * ***************************************************************************************************************
     * SCHEDULE MANAGEMENT
     * ***************************************************************************************************************
     */
    /**
     * Assigns an initial Schedule for action @action
     * 
     * @param action
     */
    public void scheduleAction(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[ResourceScheduler] Schedule action " + action + " on resource " + getName());
        // Assign no resource dependencies.
        // The worker will automatically block the tasks when there are not enough resources available.
    }

    /**
     * Unschedules the action assigned to this worker
     * 
     * @param action
     * @return
     */
    public LinkedList<AllocatableAction<P, T, I>> unscheduleAction(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[ResourceScheduler] Unschedule action " + action + " on resource " + getName());
        return new LinkedList<>();
    }

    /**
     * Cancels an action execution
     * 
     * @param action
     */
    public final void cancelAction(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[ResourceScheduler] Cancel action " + action + " on resource " + getName());
        this.blocked.remove(action);
        unscheduleAction(action);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */

    /**
     * Returns the score for the action when it is blocked
     * 
     * @param action
     * @return
     */
    public Score generateBlockedScore(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[ResourceScheduler] Generate blocked score for action " + action);
        return new Score(action.getPriority(), 0, 0, 0);
    }

    /**
     * Returns the resource score of action @action
     * 
     * @param action
     * @param params
     * @param actionScore
     * @return
     */
    public Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore) {
        LOGGER.debug("[ResourceScheduler] Generate resource score for action " + action);
        // Gets the action priority
        double actionPriority = actionScore.getActionScore();

        // Computes the resource waiting score
        double waitingScore = 2.0;
        if (this.blocked.size() > 0) {
            waitingScore = (double) (1.0 / (double) this.blocked.size());
        }
        // Computes the priority of the resource
        double resourceScore = Score.calculateScore(params, this.myWorker);

        return new Score(actionPriority, waitingScore, resourceScore, 0);
    }

    /**
     * Returns the score of a given implementation @impl for action @action with a fixed resource score @resourceScore
     * 
     * @param action
     * @param params
     * @param impl
     * @param resourceScore
     * @return
     */
    public Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl, Score resourceScore) {
        LOGGER.debug("[ResourceScheduler] Generate implementation score for action " + action);

        double actionPriority = resourceScore.getActionScore();
        double waitingScore = resourceScore.getWaitingScore();
        double resourcePriority = resourceScore.getResourceScore();

        double implScore = 1.0 / ((double) this.getProfile(impl).getAverageExecutionTime());

        return new Score(actionPriority, waitingScore, resourcePriority, implScore);
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */

    /**
     * Clear internal structures
     * 
     */
    public void clear() {
        LOGGER.debug("[ResourceScheduler] Clear resource scheduler " + getName());
        this.running.clear();
        this.blocked.clear();
        this.myWorker.releaseAllResources();
    }

    @Override
    public String toString() {
        try {
            return "ResourceScheduler@" + getName();
        } catch (NullPointerException ne) {
            return super.toString();
        }
    }

}
