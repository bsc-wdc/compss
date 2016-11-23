package integratedtoolkit.util;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Comparator;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Scheduler representation for a given worker
 *
 * @param <P>
 * @param <T>
 */
public class ResourceScheduler<P extends Profile, T extends WorkerResourceDescription> {
    
    // Logger
    protected static final Logger logger = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    // Task running in the resource
    private final LinkedList<AllocatableAction<P, T>> running;

    // Task without enough resources to be executed right now
    private final PriorityQueue<AllocatableAction<P, T>> blocked;

    // Profile information of the task executions
    private Profile[][] profiles;
    private int[] coreExecutionCounter;

    protected final Worker<T> myWorker;


    /**
     * Constructs a new Resource Scheduler associated to the worker @w
     * 
     * @param w
     */
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
        coreExecutionCounter = new int[coreCount];
        running = new LinkedList<>();
        blocked = new PriorityQueue<>(20, new Comparator<AllocatableAction<P, T>>() {
        	public int compare(AllocatableAction<P, T> a1, AllocatableAction<P, T>a2){
        		return a2.getPriority()-a1.getPriority();
        	}       	
        }
        );
        myWorker = w;
    }

    /**
     * Returns the worker name
     * 
     * @return
     */
    public final String getName() {
        return myWorker.getName();
    }

    /**
     * Returns the worker resource
     * 
     * @return
     */
    public final Worker<T> getResource() {
        return myWorker;
    }

    /**
     * Returns the coreElements that can be executed by the resource
     * 
     * @return
     */
    public LinkedList<Integer> getExecutableCores() {
        return myWorker.getExecutableCores();
    }

    /**
     * Returns the implementations that can be executed by the resource
     * 
     * @return
     */
    public LinkedList<Implementation<T>>[] getExecutableImpls() {
        return myWorker.getExecutableImpls();
    }

    /**
     * Returns the implementations of the core @id that can be executed by the resource
     * 
     * @param id
     * @return
     */
    public LinkedList<Implementation<T>> getExecutableImpls(int id) {
        return myWorker.getExecutableImpls(id);
    }

    /*
     * ------------------------------------------------
     * ------------------------------------------------
     * --------- ACTION PROFILE MANAGEMENT ------------
     * ------------------------------------------------
     * ------------------------------------------------
     */
    /**
     * Updates the coreElement structures
     * 
     */
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
     * Returns the profile for a given implementation @impl
     * 
     * @param impl
     * @return
     */
    @SuppressWarnings("unchecked")
    public final P getProfile(Implementation<T> impl) {
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
    @SuppressWarnings("unchecked")
    public P generateProfileForAllocatable() {
        return (P) new Profile();
    }

    /*
     * ------------------------------------------------ 
     * ------------------------------------------------ 
     * --------- RUNNING ACTIONS MANAGEMENT ----------- 
     * ------------------------------------------------
     * ------------------------------------------------
     */
    /**
     * Adds a new action on the resource
     * 
     * @param action
     */
    public final void hostAction(AllocatableAction<P, T> action) {
        this.running.add(action);
    }
    
    /**
     * Returns the number of tasks of type @taskId that this resource has ever executed
     * 
     * @param taskId
     * @return
     */
    public int getNumTasks(int taskId) {
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
    public final LinkedList<AllocatableAction<P, T>> getHostedActions() {
        return this.running;
    }

    /**
     * Returns all the blocked actions
     * 
     * @return
     */
    public final PriorityQueue<AllocatableAction<P, T>> getBlockedActions() {
        return blocked;
    }

    /**
     * Removes a running action
     * 
     * @param action
     */
    public final void unhostAction(AllocatableAction<P, T> action) {
        running.remove(action);
    }

    /*
     * ------------------------------------------------ 
     * ------------------------------------------------ 
     * --------- BLOCKED ACTIONS MANAGEMENT -----------
     * ------------------------------------------------
     * ------------------------------------------------
     */
    /**
     * Adds a blocked action on this worker
     * 
     * @param action
     */
    public final void waitOnResource(AllocatableAction<P, T> action) {
        blocked.add(action);
    }

    /**
     * Returns if there are blocked actions or not
     * 
     * @return
     */
    public final boolean hasBlockedActions() {
        return blocked.size() > 0;
    }

    /**
     * Returns the first blocked action without removing it
     * 
     * @return
     */
    public final AllocatableAction<P, T> getFirstBlocked() {
        return blocked.peek();
    }

    /**
     * Removes the first blocked action
     * 
     */
    public final void removeFirstBlocked() {
        blocked.poll();
    }

    /*
     * ------------------------------------------------ 
     * ------------------------------------------------ 
     * ------ RESOURCE IMPLEMENTATION SCORE ----------- 
     * ------------------------------------------------
     * ------------------------------------------------
     */
    /**
     * Returns the resource score of action @action
     * 
     * @param action
     * @param params
     * @param actionScore
     * @return
     */
    public Score getResourceScore(AllocatableAction<P, T> action, TaskDescription params, Score actionScore) {
        long resourceScore = Score.getLocalityScore(params, myWorker);
        return new Score(actionScore, 0, resourceScore, 0);

    }
    
    /**
     * Returns the waiting score of action @action and implementation @impl
     * 
     * @param action
     * @param params
     * @param impl
     * @param resourceScore
     * @return
     */
    public Score getWaitingScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl, Score resourceScore) {      
    	double waitingScore = 2.0;
    	if (blocked.size() > 0) {
    		waitingScore = (double)(1/(double)blocked.size());
    	}
    	return new Score(resourceScore,waitingScore,0);
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
    public Score getImplementationScore(AllocatableAction<P, T> action, TaskDescription params, Implementation<T> impl, Score resourceScore) {
        long implScore = this.getProfile(impl).getAverageExecutionTime();
        return new Score(getWaitingScore(action, params, impl, resourceScore), (double)(1/(double)implScore));
    }

    /**
     * Assigns an initial Schedule for action @action
     * 
     * @param action
     */
    public void initialSchedule(AllocatableAction<P, T> action) {
        // Assign no resource dependencies. The worker will automatically block
        // the tasks when there are not enough resources available.
        
        // Increase the task count counter if needed
        Integer taskId = action.getCoreId();
        if (taskId != null && taskId < this.coreExecutionCounter.length) {
            ++this.coreExecutionCounter[taskId];
        }
    }

    /**
     * Unschedules the action assigned to this worker
     * 
     * @param action
     * @return
     */
    public LinkedList<AllocatableAction<P, T>> unscheduleAction(AllocatableAction<P, T> action) {
        return new LinkedList<>();
    }

    /**
     * Cancels an action execution
     * 
     * @param action
     */
    public final void cancelAction(AllocatableAction<P, T> action) {
        blocked.remove(action);
        unscheduleAction(action);
    }
    
    /**
     * Clear internal structures
     * 
     */
    public void clear() {
        running.clear();
        blocked.clear();
        myWorker.releaseAllResources();
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
