package integratedtoolkit.scheduler.types;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Abstract representation of an Allocatable Action (task execution, task transfer, etc.)
 *
 * @param <P>
 * @param <T>
 * @param <I>
 */
public abstract class AllocatableAction<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    /**
     * Available states for any allocatable action
     *
     */
    private enum State {
        RUNNABLE, // Action can be run
        WAITING, // Action is waiting
        RUNNING, // Action is running
        FINISHED, // Action has been successfully completed
        FAILED // Action has failed
    }


    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // AllocatableAction Id counter
    private static AtomicInteger nextId = new AtomicInteger();

    // Orchestrator
    protected final ActionOrchestrator<P, T, I> orchestrator;

    // Id of the current AllocatableAction
    private final long id;

    // Allocatable actions that the action depends on due data dependencies
    private final LinkedList<AllocatableAction<P, T, I>> dataPredecessors;
    // Allocatable actions depending on the allocatable action due data dependencies
    private final LinkedList<AllocatableAction<P, T, I>> dataSuccessors;

    private State state;
    protected ResourceScheduler<P, T, I> selectedResource;
    protected I selectedImpl;
    protected final LinkedList<ResourceScheduler<P, T, I>> executingResources;

    private final SchedulingInformation<P, T, I> schedulingInfo;

    protected P profile;

    // Lock to avoid many threads to modify the same action
    private final ReentrantLock lock = new ReentrantLock();

    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */

    /**
     * Registers a new allocatable action
     * 
     * @param schedulingInformation
     */
    public AllocatableAction(SchedulingInformation<P, T, I> schedulingInformation, ActionOrchestrator<P, T, I> orchestrator) {
        this.id = nextId.getAndIncrement();
        this.orchestrator = orchestrator;
        this.dataPredecessors = new LinkedList<>();
        this.dataSuccessors = new LinkedList<>();
        this.state = State.RUNNABLE;
        this.selectedResource = null;
        this.selectedImpl = null;
        this.executingResources = new LinkedList<>();
        this.schedulingInfo = schedulingInformation;
        this.profile = null;
    }
    
    /*
     * ***************************************************************************************************************
     * ORCHESTRATOR OPERATIONS
     * ***************************************************************************************************************
     */
    /**
     * Notify action completed to orchestrator
     * 
     */
    protected void notifyCompleted() {
        if (DEBUG) {
            LOGGER.debug("Notify completed of " + this + " to orchestrator " + orchestrator);
        }
        orchestrator.actionCompletion(this);
    }

    /**
     * Notify action failed to orchestrator
     */
    protected void notifyError() {
        if (DEBUG) {
            LOGGER.debug("Notify error of " + this + " to orchestrator " + orchestrator);
        }
        orchestrator.actionError(this);
    }

    /*
     * ***************************************************************************************************************
     * DATA DEPENDENCIES OPERATIONS
     * 
     * This operations are only executed by the main thread of the Task Dispatcher
     * ***************************************************************************************************************
     */

    /**
     * Returns the AA id
     * 
     * @return
     */
    public final long getId() {
        return id;
    }

    /**
     * Returns the data predecessors
     * 
     * @return
     */
    public final LinkedList<AllocatableAction<P, T, I>> getDataPredecessors() {
        return dataPredecessors;
    }

    /**
     * Returns the data successors
     * 
     * @return
     */
    public final LinkedList<AllocatableAction<P, T, I>> getDataSuccessors() {
        return dataSuccessors;
    }

    /**
     * Returns if there is any existing data predecessor
     * 
     * @return
     */
    public final boolean hasDataPredecessors() {
        return dataPredecessors.size() > 0;
    }

    /**
     * Adds a data predecessor
     * 
     * @param predecessor
     */
    public final void addDataPredecessor(AllocatableAction<P, T, I> predecessor) {
        if (predecessor.isPending()) {
            dataPredecessors.add(predecessor);
            predecessor.dataSuccessors.add(this);
        }

    }

    /**
     * Updates the done predecessors
     * 
     * @param finishedAction
     */
    private final void dataPredecessorDone(AllocatableAction<P, T, I> finishedAction) {
        Iterator<AllocatableAction<P, T, I>> it = dataPredecessors.iterator();
        while (it.hasNext()) {
            AllocatableAction<P, T, I> aa = it.next();
            if (aa == finishedAction) {
                it.remove();
            }
        }
    }

    /*
     * ***************************************************************************************************************
     * RESOURCES MANAGEMENT OPERATIONS
     * ***************************************************************************************************************
     */

    /**
     * Tells if the action has to run in the same resource as another action.
     *
     * @return {@literal true} if the action scheduling is constrained to a certain resource.
     */
    public final boolean isSchedulingConstrained() {
        return !schedulingInfo.getConstrainingPredecessors().isEmpty();
    }

    /**
     * Adds a resource predecessor
     * 
     * @param predecessor
     */
    public final void addResourceConstraint(AllocatableAction<P, T, I> predecessor) {
        schedulingInfo.addResourceConstraint(predecessor);
    }

    /**
     * Returns if a resource is not needed for the current AllocatableAction
     * 
     * @return
     */
    public final boolean unrequiredResource() {
        for (AllocatableAction<P, T, I> a : this.getConstrainingPredecessors()) {
            if (a.getAssignedResource() == selectedResource) {
                return false;
            }
        }

        return true;
    }

    /**
     * Tells the action whose running resource should execute this action.
     *
     * @return action that constraints the scheduling of the action.
     */
    public final List<AllocatableAction<P, T, I>> getConstrainingPredecessors() {
        return schedulingInfo.getConstrainingPredecessors();
    }

    /**
     * Returns the coreElement executors
     * 
     * @param coreId
     * @return
     */
    protected final LinkedList<ResourceScheduler<P, T, I>> getCoreElementExecutors(int coreId) {
        return SchedulingInformation.getCoreElementExecutors(coreId);
    }

    /**
     * Returns the scheduling information
     * 
     * @return
     */
    public final SchedulingInformation<P, T, I> getSchedulingInfo() {
        return schedulingInfo;
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION AND LIFECYCLE MANAGEMENT
     * ***************************************************************************************************************
     */
    /**
     * Returns if the AllocatableAction is pending or not
     * 
     * @return
     */
    public final boolean isPending() {
        return state != State.FAILED && state != State.FINISHED;
    }

    /**
     * Returns if the AllocatableAction is running or not
     * 
     * @return
     */
    public final boolean isRunning() {
        return state == State.RUNNING;
    }

    /**
     * Returns if the action is locked for another scheduling or not
     * 
     * @return
     */
    public final boolean isLocked() {
        return lock.isLocked();
    }
    
    /**
     * Locks the action scheduling
     * 
     */
    public final void lockAction() {
        lock.lock();
    }

    /**
     * Returns if the allocatable action is not beeing scheduled
     * 
     * @return
     */
    public final boolean isNotScheduling() {
        return !isLocked() && !isRunning() && selectedResource == null && state == State.RUNNABLE;
    }

    /**
     * Returns the start time (ms) of the allocatable action
     * 
     * @return
     */
    public final Long getStartTime() {
        if (profile == null) {
            return null;
        }
        return profile.getStartTime();
    }

    /**
     * Assigned an implementation to the allocatable action
     * 
     * @param impl
     */
    public final void assignImplementation(I impl) {
        if (state == State.RUNNABLE) {
            selectedImpl = impl;
        }
    }

    /**
     * Tells the implementation assigned to the action
     *
     * @return
     */
    public final I getAssignedImplementation() {
        return selectedImpl;
    }

    /**
     * Assign resources to the allocatable action
     * 
     * @param resource
     */
    public final void assignResources(ResourceScheduler<P, T, I> resource) {
        if (state == State.RUNNABLE) {
            selectedResource = resource;
        }
    }

    /**
     * Tells the resource assigned to the action
     *
     * @return
     */
    public final ResourceScheduler<P, T, I> getAssignedResource() {
        return selectedResource;
    }

    /**
     * Tries to schedule the current action
     * 
     * @throws InvalidSchedulingException
     */
    public final void tryToLaunch() throws InvalidSchedulingException {
        if (!isLocked()) {      
            // Gets the lock on the action
            lock.lock();
            if ( // has an assigned resource where to run
                    selectedResource != null && // has not been started yet
                    state == State.RUNNABLE && // has no data dependencies with other methods
                    !hasDataPredecessors() && // scheduler does not block the execution
                    schedulingInfo.isExecutable()) {
    
                // Invalid scheduling -> Should run in a specific resource and the assigned resource is not the required
                if (isSchedulingConstrained() && unrequiredResource()) {
                    // Allow other threads to access the action
                    lock.unlock();
                    // Notify invalid scheduling
                    throw new InvalidSchedulingException();
                }
                // Correct resource and task ready to run
                execute();
            } else {
                lock.unlock();
            }
        }
    }

    private final void execute() {
        LOGGER.info(this + " execution starts on worker " + selectedResource.getName());

        // there are enough resources to host the actions and no waiting tasks in the queue
        if (!selectedResource.hasBlockedActions() && areEnoughResources()) {
            // register executing resource
            executingResources.add(selectedResource);
            // Run action
            run();
        } else {
            LOGGER.info(this + " execution paused due to lack of resources on worker " + selectedResource.getName());
            // Task waits on the resource queue
            // It can only be resumed because of a task completion or error.
            // execute won't be executed again since tryToLaunch is blocked
            state = State.WAITING;
            selectedResource.waitOnResource(this);

            // Allow other threads to execute the task (complete and error executor)
            lock.unlock();
        }
    }

    public final void run() {
        // Actually runs the action. This function is called only once per action (except for reschedules)
        // Blocks other tryToLaunch
        state = State.RUNNING;
        // Allow other threads to execute the task (complete and error executor)
        lock.unlock();
        
        reserveResources();
        profile = selectedResource.generateProfileForAllocatable();
        selectedResource.hostAction(this);
        
        doAction();
    }

    /**
     * Returns if there are enough resources to run the action or not
     * 
     * @return
     */
    public abstract boolean areEnoughResources();

    /**
     * Reserves the needed resources to run the action
     * 
     */
    protected abstract void reserveResources();

    /**
     * Releases the needed resources to run the action
     * 
     */
    protected abstract void releaseResources();
    
    /**
     * Triggers the action
     */
    protected abstract void doAction();

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */

    /**
     * Operations to perform when AA has been successfully completed Calls specific operation doCompleted
     * 
     * @return
     */
    public final LinkedList<AllocatableAction<P, T, I>> completed() {
        // Mark as finished
        state = State.FINISHED;

        // Release resources and run tasks blocked on the resource
        releaseResources();
        selectedResource.unhostAction(this);
        selectedResource.tryToLaunchBlockedActions();

        // Action notification
        doCompleted();

        // Release data dependencies of the task
        LinkedList<AllocatableAction<P, T, I>> freeTasks = new LinkedList<>();
        for (AllocatableAction<P, T, I> aa : dataSuccessors) {
            aa.dataPredecessorDone(this);
            if (!aa.hasDataPredecessors()) {
                if (!aa.isLocked() && !aa.isRunning()) {
                    freeTasks.add(aa);
                }
            }
        }
        dataSuccessors.clear();

        return freeTasks;
    }

    /**
     * Operations to perform when AA has raised an error Calls specific operation doError
     * 
     * @return
     */
    public final void error() throws FailedActionException {
        // Mark as runnable since we can retry its execution
        state = State.RUNNABLE;
        
        // Release resources and run tasks blocked on the resource
        releaseResources();
        selectedResource.unhostAction(this);
        selectedResource.tryToLaunchBlockedActions();

        // Action notification
        doError();
    }

    /**
     * Operations to perform when AA has totally failed Calls specific operation doFailed
     * 
     * @return
     */
    public final LinkedList<AllocatableAction<P, T, I>> failed() {
        // Mark as failed
        this.state = State.FAILED;
        
        // Release resources and cancel action
        releaseResources();
        selectedResource.cancelAction(this);

        // Predecessors -> ignore Action
        for (AllocatableAction<P, T, I> pred : dataPredecessors) {
            pred.dataSuccessors.remove(this);
        }

        // Triggering failure on Data Successors
        LinkedList<AllocatableAction<P, T, I>> failed = new LinkedList<>();
        for (AllocatableAction<P, T, I> succ : dataSuccessors) {
            failed.addAll(succ.failed());
        }
        failed.add(this);

        dataPredecessors.clear();
        dataSuccessors.clear();

        // Action notification
        doFailed();

        return failed;
    }

    /**
     * Triggers the successful job completion notification
     */
    protected abstract void doCompleted();

    /**
     * Triggers a retry on a job completion
     * 
     * @throws FailedActionException
     */
    protected abstract void doError() throws FailedActionException;

    /**
     * Triggers the unsuccessful action completion notification
     *
     */
    protected abstract void doFailed();

    /*
     * ***************************************************************************************************************
     * SCHEDULING MANAGEMENT
     * ***************************************************************************************************************
     */
    public abstract Integer getCoreId();

    /**
     * Returns all the workers that are able to run the action.
     *
     * @return list of resources able to run the action
     */
    public abstract LinkedList<ResourceScheduler<P, T, I>> getCompatibleWorkers();

    /**
     * Returns all the possible implementations for the action.
     *
     * @return a list of implementations that can be executed to run the action.
     */
    public abstract Implementation<?>[] getImplementations();

    /**
     * Tells is the action can run in a given resource.
     *
     * @param r
     *            Resource where the action should run.
     *
     * @return {@literal true} if the action can run in the given resource.
     */
    public abstract boolean isCompatible(Worker<T, I> r);

    /**
     * Returns all the implementations for the action that can run on the given resource.
     *
     * @param r
     *            resource that should run the action
     *
     * @return list of the action implementations that can run on the resource.
     */
    public abstract LinkedList<I> getCompatibleImplementations(ResourceScheduler<P, T, I> r);

    /**
     * Returns the action priority
     * 
     * @return
     */
    public abstract int getPriority();
    
    /**
     * Returns the scheduling score of the action for a given worker @targetWorker
     * 
     * @param targetWorker
     * @param actionScore
     * @return
     */
    public abstract Score schedulingScore(ResourceScheduler<P, T, I> targetWorker, Score actionScore);

    /**
     * Schedules the action considering the @actionScore
     * 
     * @param actionScore
     * @throws BlockedActionException
     * @throws UnassignedActionException
     */
    public abstract void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException;

    /**
     * Schedules the action to a given @targetWorker with score @actionScore
     * 
     * @param targetWorker
     * @param actionScore
     * @throws BlockedActionException
     * @throws UnassignedActionException
     */
    public abstract void schedule(ResourceScheduler<P, T, I> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException;

    /**
     * Schedules the implementation @impl of the action to a given @targetWorker
     * 
     * @param targetWorker
     * @param impl
     * @throws BlockedActionException
     * @throws UnassignedActionException
     */
    public abstract void schedule(ResourceScheduler<P, T, I> targetWorker, I impl) throws BlockedActionException, UnassignedActionException;

}
