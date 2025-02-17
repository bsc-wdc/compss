/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.scheduler.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.ActionNotFoundException;
import es.bsc.compss.scheduler.exceptions.ActionNotWaitingException;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionGroup.MutexGroup;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.worker.COMPSsException;

import java.util.Collection;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Abstract representation of an Allocatable Action (task execution, task transfer, etc.).
 */
public abstract class AllocatableAction {

    /**
     * Available states for any allocatable action.
     */
    private enum State {
        RUNNABLE, // Action can be run
        WAITING, // Action is waiting
        RUNNING, // Action is running
        FINISHED, // Action has been successfully completed
        FAILED, // Action has failed
        CANCELLED, // Action has been canceled
        CANCELLING
    }


    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();
    protected static final String DBG_PREFIX = "[AllocatableAction] ";
    // AllocatableAction Id counter
    private static final AtomicInteger NEXT_ID = new AtomicInteger();

    // Default action group priorities (the less the better)
    protected static final long ACTION_SINGLE = Long.MAX_VALUE;
    protected static final long ACTION_GROUP_RUNNING = 0L;
    protected static final long ACTION_GROUP_IDLE = 1L;
    protected static final long ACTION_VALUE_TRANSFER = -1L;
    protected static final long ACTION_START_WORKER = -1L;
    protected static final long ACTION_STOP_WORKER = -1L;
    protected static final long ACTION_REDUCE_WORKER = -1L;
    protected static final long ACTION_OPTIMIZE = -1L;

    // Orchestrator
    protected final ActionOrchestrator orchestrator;

    // Id of the current AllocatableAction
    private final long id;

    // Allocatable actions that the action depends on due data dependencies
    private final List<AllocatableAction> dataPredecessors;
    // Allocatable actions depending on the allocatable action due data dependencies
    private final List<AllocatableAction> dataSuccessors;
    // Allocatable actions that produce stream elements that this allocatable action uses
    private final List<AllocatableAction> streamDataProducers;
    // Allocatable actions that consume stream elements produced by this allocatable action
    private final List<AllocatableAction> streamDataConsumers;

    // Mutual exclusion task groups
    private final List<MutexGroup> mutexGroups;

    private State state;
    private ResourceScheduler<? extends WorkerResourceDescription> selectedResource;
    private Implementation selectedImpl;
    private WorkerResourceDescription resourceConsumption;
    private final List<ResourceScheduler<? extends WorkerResourceDescription>> executingResources;

    private final SchedulingInformation schedulingInfo;

    protected Profile profile;

    // Lock to avoid many threads to modify the same action
    private final ReentrantLock lock = new ReentrantLock();

    /*
     * ***************************************************************************************************************
     * CONSTRUCTOR
     * ***************************************************************************************************************
     */


    /**
     * Registers a new allocatable action.
     *
     * @param schedulingInformation Associated scheduling information.
     * @param orchestrator Action Orchestrator (scheduler).
     */
    public AllocatableAction(SchedulingInformation schedulingInformation, ActionOrchestrator orchestrator) {
        this.id = NEXT_ID.getAndIncrement();
        this.orchestrator = orchestrator;
        this.dataPredecessors = new LinkedList<>();
        this.dataSuccessors = new LinkedList<>();
        this.streamDataProducers = new LinkedList<>();
        this.streamDataConsumers = new LinkedList<>();
        this.state = State.RUNNABLE;
        this.selectedResource = null;
        this.selectedImpl = null;
        this.executingResources = new LinkedList<>();
        this.schedulingInfo = schedulingInformation;
        this.profile = null;
        this.mutexGroups = new LinkedList<>();
    }

    /*
     * ***************************************************************************************************************
     * ORCHESTRATOR OPERATIONS
     * ***************************************************************************************************************
     */

    /**
     * Notify action running to the orchestrator.
     */
    protected void notifyRunning() {
        if (DEBUG) {
            LOGGER.debug("Notify running " + this + " to orchestrator " + this.orchestrator);
        }
        this.orchestrator.actionRunning(this);
    }

    /**
     * Notify action completed to the orchestrator.
     */
    protected void notifyCompleted() {
        if (DEBUG) {
            LOGGER.debug("Notify completed of " + this + " to orchestrator " + this.orchestrator);
        }
        this.orchestrator.actionCompletion(this);
    }

    /**
     * Notify action failed to the orchestrator.
     */
    protected void notifyError() {
        LOGGER.warn("Notify error of " + this + " to orchestrator " + this.orchestrator);
        this.orchestrator.actionError(this);
    }

    /**
     * Notify action raised a COMPSs exception to the orchestrator.
     */
    protected void notifyException(COMPSsException e) {
        LOGGER.warn("Notify COMPSs exception of " + this + " to orchestrator " + this.orchestrator);
        this.orchestrator.actionException(this, e);
    }

    /*
     * ***************************************************************************************************************
     * DATA DEPENDENCIES OPERATIONS
     *
     * This operations are only executed by the main thread of the Task Dispatcher
     * ***************************************************************************************************************
     */

    /**
     * Returns the AA id.
     *
     * @return The AA id.
     */
    public final long getId() {
        return this.id;
    }

    /**
     * Returns the data predecessors.
     *
     * @return The data predecessors.
     */
    public final List<AllocatableAction> getDataPredecessors() {
        return this.dataPredecessors;
    }

    /**
     * Returns the data successors.
     *
     * @return The data successors.
     */
    public final List<AllocatableAction> getDataSuccessors() {
        return this.dataSuccessors;
    }

    /**
     * Returns the list of stream producers.
     *
     * @return The stream producers.
     */
    public final List<AllocatableAction> getStreamDataProducers() {
        return this.streamDataProducers;
    }

    /**
     * Returns the list of stream consumers.
     *
     * @return The stream consumers.
     */
    public final List<AllocatableAction> getStreamDataConsumers() {
        return this.streamDataConsumers;
    }

    /**
     * Returns whether there are data predecessors or not.
     *
     * @return {@code true} if there are data predecessors, {@code false} otherwise.
     */
    public final boolean hasDataPredecessors() {
        Iterator<AllocatableAction> producers = this.dataPredecessors.iterator();
        while (producers.hasNext()) {
            AllocatableAction aa = producers.next();
            if (checkIfCanceled(aa)) {
                producers.remove();
            }
        }
        return !this.dataPredecessors.isEmpty();
    }

    /**
     * Returns whether there are stream data predecessors or not.
     *
     * @return {@code true} if there are stream data predecessors, {@code false} otherwise.
     */
    public final boolean hasStreamProducers() {
        Iterator<AllocatableAction> producers = this.streamDataProducers.iterator();
        while (producers.hasNext()) {
            AllocatableAction aa = producers.next();
            if (checkIfCanceled(aa)) {
                producers.remove();
            }
        }
        return !this.streamDataProducers.isEmpty();
    }

    /**
     * Returns if the task was cancelled.
     *
     * @param aa Allocatable action.
     * @return {@literal true} if the action was canceled {@literal false} otherwise.
     */
    public abstract boolean checkIfCanceled(AllocatableAction aa);

    /**
     * Adds a data predecessor.
     *
     * @param predecessor Predecessor Allocatable Action.
     */
    public final void addDataPredecessor(AllocatableAction predecessor) {
        if (predecessor.isPending()) {
            if (!this.dataPredecessors.contains(predecessor)) {
                this.dataPredecessors.add(predecessor);
            }
            if (!predecessor.dataSuccessors.contains(this)) {
                predecessor.dataSuccessors.add(this);
            }
        }
    }

    public void addAlreadyDoneAction(AllocatableAction predecessor) {
        // Nothing to do by default. Has different implementations.
    }

    /**
     * Adds a stream producer.
     *
     * @param predecessor Stream producer Allocatable Action.
     */
    public final void addStreamProducer(AllocatableAction predecessor) {
        if (predecessor.state.equals(State.RUNNABLE) || predecessor.state.equals(State.WAITING)) {
            if (!this.streamDataProducers.contains(predecessor)) {
                if (DEBUG) {
                    LOGGER.debug("Adding stream producer action " + predecessor.getId() + " to action " + this.getId());
                }
                this.streamDataProducers.add(predecessor);
            }
            if (!predecessor.streamDataConsumers.contains(this)) {
                if (DEBUG) {
                    LOGGER.debug("Adding stream consumer action " + this.getId() + " to action " + predecessor.getId());
                }
                predecessor.streamDataConsumers.add(this);
            }
        }
    }

    /**
     * Updates the predecessors by removing the finished action {@code finishedAction}.
     *
     * @param finishedAction Finished Allocatable Action.
     */
    protected void dataPredecessorDone(AllocatableAction finishedAction) {
        Iterator<AllocatableAction> it = this.dataPredecessors.iterator();
        while (it.hasNext()) {
            AllocatableAction aa = it.next();
            if (aa == finishedAction) {
                it.remove();
                break;
            }
        }
    }

    /**
     * Updates the stream data producers by removing the finished action {@code finishedAction}.
     *
     * @param finishedAction Finished Allocatable Action.
     */
    private void streamDataProducerStarted(AllocatableAction finishedAction) {
        Iterator<AllocatableAction> it = this.streamDataProducers.iterator();
        while (it.hasNext()) {
            AllocatableAction aa = it.next();
            if (aa == finishedAction) {
                if (DEBUG) {
                    LOGGER.debug("Removing stream producer action " + aa.getId() + " from action " + this.getId());
                }
                it.remove();
                break;
            }
        }
    }

    private List<AllocatableAction> releaseDataSuccessors() {
        // Release data dependencies of the task
        List<AllocatableAction> freeTasks = new LinkedList<>();
        for (AllocatableAction aa : this.dataSuccessors) {
            aa.dataPredecessorDone(this);
            if (!aa.hasDataPredecessors() && !aa.hasStreamProducers()) {
                freeTasks.add(aa);
            }
        }

        this.dataSuccessors.clear();
        return freeTasks;
    }

    private List<AllocatableAction> releaseStreamDataConsumers() {
        // Release producer from consumers and check if stream consumers are free
        List<AllocatableAction> freeActions = new LinkedList<>();
        for (AllocatableAction aa : this.streamDataConsumers) {
            aa.streamDataProducerStarted(this);
            if (!aa.hasStreamProducers() && !aa.hasDataPredecessors()) {
                freeActions.add(aa);
            }
        }

        return freeActions;
    }

    /*
     * ***************************************************************************************************************
     * MUTEX GROUPS OPERATIONS
     *
     * This operations are only executed by the main thread of the Task Dispatcher
     * ***************************************************************************************************************
     */

    /**
     * Registers the action into a mutexGroup.
     *
     * @param group group to which the task belongs
     */
    protected final void addToMutexGroup(MutexGroup group) {
        if (!this.mutexGroups.contains(group)) {
            this.mutexGroups.add(group);
        }
        group.addMember(this);
    }

    private boolean areMutexLocksAvailable() {
        for (MutexGroup group : this.mutexGroups) {
            if (!group.testLock(this)) {
                return false;
            }
        }
        return true;
    }

    private void acquireMutexLocks() {
        for (MutexGroup group : this.mutexGroups) {
            group.acquireLock(this);
        }
    }

    /**
     * Releases all the mutex locks for the action and update the released list parameters with all the dependency-free
     * tasks that can now be executed.
     *
     * @param released List where data-dependency, lock-free actions will be added.
     */
    private void releaseMutexLocks(List<AllocatableAction> released) {
        for (MutexGroup group : this.mutexGroups) {
            group.removeMember(this);
            for (AllocatableAction aa : group.getMembers()) {
                if (!aa.hasDataPredecessors()) {
                    if (!released.contains(aa)) {
                        released.add(aa);
                    }
                }
            }
            group.releaseLock();
        }
    }

    /*
     * ***************************************************************************************************************
     * RESOURCES MANAGEMENT OPERATIONS
     * ***************************************************************************************************************
     */

    /**
     * Tells whether the action has to run in the same resource as another action.
     *
     * @return {@literal true} if the action scheduling is constrained to a certain resource, {@literal false}
     *         otherwise.
     */
    public final boolean isTargetResourceEnforced() {
        return this.schedulingInfo.getEnforcedTargetResource() != null;
    }

    /**
     * Returns the enforced resource.
     *
     * @return The enforced resource.
     */
    public final ResourceScheduler<? extends WorkerResourceDescription> getEnforcedTargetResource() {
        return this.schedulingInfo.getEnforcedTargetResource();
    }

    /**
     * Tells if the action has to run in the same resource as another action.
     *
     * @return {@literal true} if the action scheduling is constrained to a certain resource, {@literal false}
     *         otherwise.
     */
    public final boolean isSchedulingConstrained() {
        return !this.schedulingInfo.getConstrainingPredecessors().isEmpty();
    }

    /**
     * Adds a resource constraint predecessor.
     *
     * @param predecessor Predecessor Allocatable Action.
     */
    public final void addResourceConstraint(AllocatableAction predecessor) {
        this.schedulingInfo.addResourceConstraint(predecessor);
    }

    /**
     * Returns whether a resource is not needed for the current AllocatableAction.
     *
     * @return {@literal true} if the resource is not needed, {@literal false} otherwise.
     */
    public final boolean unrequiredResource() {
        for (AllocatableAction a : this.getConstrainingPredecessors()) {
            if (a.getAssignedResource() == this.selectedResource) {
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
    public final List<AllocatableAction> getConstrainingPredecessors() {
        return this.schedulingInfo.getConstrainingPredecessors();
    }

    /**
     * Returns the coreElement executors.
     *
     * @param coreId Core Id.
     * @return List of coreElement executors.
     */
    protected final List<ResourceScheduler<? extends WorkerResourceDescription>> getCoreElementExecutors(int coreId) {
        return SchedulingInformation.getCoreElementExecutors(coreId);
    }

    /**
     * Returns the scheduling information.
     *
     * @return The scheduling information.
     */
    public final SchedulingInformation getSchedulingInfo() {
        return this.schedulingInfo;
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION AND LIFECYCLE MANAGEMENT
     * ***************************************************************************************************************
     */

    /**
     * Returns whether the AllocatableAction is pending or not.
     *
     * @return {@literal true} if the AllocatableAction is in pending state, {@literal false} otherwise.
     */
    public final boolean isPending() {
        return this.state != State.FAILED && this.state != State.FINISHED;
    }

    /**
     * Returns whether the AllocatableAction is running or not.
     *
     * @return {@literal true} if the AllocatableAction is running, {@literal false} otherwise.
     */
    public final boolean isRunning() {
        return this.state == State.RUNNING;
    }

    /**
     * Returns whether the AllocatableAction is being cancelled or not.
     *
     * @return {@literal true} if the AllocatableAction is cancelled, {@literal false} otherwise.
     */
    public final boolean isCancelling() {
        return this.state == State.CANCELLING;
    }

    /**
     * Returns whether the AllocatableAction has been cancelled.
     *
     * @return {@literal true} if the AllocatableAction is cancelled, {@literal false} otherwise.
     */
    public final boolean isCancelled() {
        return this.state == State.CANCELLED;
    }

    /**
     * Returns whether the AllocatableAction is in RUNNABLE state.
     *
     * @return {@literal true} if the AllocatableAction is runnable, {@literal false} otherwise.
     */
    public final boolean isRunnable() {
        return this.state == State.RUNNABLE;
    }

    /**
     * Returns whether the AllocatableAction is in FINISHED state.
     *
     * @return {@literal true} if the AllocatableAction is finished, {@literal false} otherwise.
     */
    public final boolean isFinished() {
        return this.state == State.FINISHED;
    }

    /**
     * Returns whether the action is locked for another scheduling or not.
     *
     * @return {@literal true} if the action is locked because of another scheduling, {@literal false} otherwise.
     */
    public final boolean isLocked() {
        return this.lock.isLocked();
    }

    /**
     * Returns whether the Allocatable Action is not being scheduled or yes.
     *
     * @return {@literal true} if the AllocatableAction is not being scheduled, {@literal false} otherwise.
     */
    public final boolean isNotScheduling() {
        return !isLocked() && !isRunning() && this.selectedResource == null && this.state == State.RUNNABLE;
    }

    /**
     * Returns the setSubmissionTime time (ms) of the Allocatable Action.
     *
     * @return The setSubmissionTime time (ms) of the Allocatable Action.
     */
    public final Long getStartTime() {
        if (this.profile == null) {
            return null;
        }
        return profile.getStartTime();
    }

    /**
     * Assigns an implementation to the Allocatable Action.
     *
     * @param impl Implementation to assign.
     */
    public final void assignImplementation(Implementation impl) {
        if (this.state == State.RUNNABLE) {
            this.selectedImpl = impl;
        }
    }

    /**
     * Returns the implementation assigned to the action.
     *
     * @return The implementation assigned to the action.
     */
    public final Implementation getAssignedImplementation() {
        return this.selectedImpl;
    }

    /**
     * Assign a resource to the Allocatable Action.
     *
     * @param resource Resource to assign.
     */
    public final <T extends WorkerResourceDescription> void assignResource(ResourceScheduler<T> resource) {
        if (this.state == State.RUNNABLE) {
            this.selectedResource = resource;
        }
    }

    /**
     * Returns the resource assigned to the action.
     *
     * @return The resource assigned to the action.
     */
    public final ResourceScheduler<? extends WorkerResourceDescription> getAssignedResource() {
        return this.selectedResource;
    }

    /**
     * Tries to schedule the current action.
     *
     * @throws InvalidSchedulingException When an invalid scheduling state has been reached.
     */
    public final void tryToLaunch() throws InvalidSchedulingException {
        // Gets the lock on the action
        this.lock.lock();

        if (selectedResource != null // has an assigned resource where to run
            && state == State.RUNNABLE // has not been started yet
            && !hasDataPredecessors() // has no pending data dependencies with other methods
            && !hasStreamProducers() // does not consume data from a stream from an unstarted producer
            && schedulingInfo.isExecutable() // scheduler does not block the execution
            && areMutexLocksAvailable() // there are no tasks being executed in a mutex group
        ) {
            // Invalid scheduling -> Allocatable action should run in a specific resource but: resource is removed and
            // task is not to stop; or the assigned resource is not the required
            if ((this.selectedResource.isRemoved() && !isToStopResource())
                || (isSchedulingConstrained() && unrequiredResource() || isTargetResourceEnforced()
                    && this.selectedResource != this.schedulingInfo.getEnforcedTargetResource())) {
                // Allow other threads to access the action
                this.lock.unlock();
                // Notify invalid scheduling
                LOGGER.debug("Action " + this + " incorrectly scheduled. Throwing exception.");
                throw new InvalidSchedulingException();
            }

            acquireMutexLocks();
            // Correct resource and task ready to run
            try {
                WorkerResourceDescription consumption = this.selectedResource.hostAction(this);
                run(consumption);
            } catch (BlockedActionException e) {
                this.state = State.WAITING;
                // Allow other threads to execute the task (complete and error executor)
                this.lock.unlock();
            } catch (InvalidSchedulingException ise) {
                this.lock.unlock();
                throw ise;
            }
        } else {
            if (hasDataPredecessors()) {
                if (DEBUG) {
                    LOGGER.debug(DBG_PREFIX + "Action " + this + " not executed because data dependencies");
                    for (AllocatableAction aa : getDataPredecessors()) {
                        LOGGER.debug("\n Predecessor: " + aa);
                    }
                    for (AllocatableAction aa : getStreamDataProducers()) {
                        LOGGER.debug("\n Producer: " + aa);
                    }
                }
            }
            this.lock.unlock();
        }
    }

    /**
     * Resumes the execution of a waiting AllocatableAction.
     *
     * @throws ActionNotWaitingException When the AllocatableAction is not in waiting state.
     */
    public final void resumeExecution(WorkerResourceDescription consumption) throws ActionNotWaitingException {
        this.lock.lock();
        if (this.state == State.WAITING) {
            LOGGER.info(this + " execution resumed on worker " + this.selectedResource.getName());
            run(consumption);
        } else {
            this.lock.unlock();
            throw new ActionNotWaitingException();
        }
    }

    private void run(WorkerResourceDescription consumption) {
        // register executing resource
        this.executingResources.add(this.selectedResource);
        this.resourceConsumption = consumption;

        // Actually runs the action. This function is called only once per action (except for reschedules)
        // Blocks other tryToLaunch
        this.state = State.RUNNING;
        // Allow other threads to execute the task (complete and error executor)
        this.lock.unlock();

        // Run
        this.profile = this.selectedResource.generateProfileForRun(this);
        doAction();

        // Notify the orchestrator that task is running (to free the stream data consumers if necessary)
        notifyRunning();
    }

    /**
     * Returns whether the AllocatableAction is to stop a resource or not.
     *
     * @return {@literal true} if the AllocatableAction is to stop a resource, {@literal false} otherwise.
     */
    public abstract boolean isToStopResource();

    /**
     * Returns whether the AllocatableAction needs to reserve some resources for its execution or not.
     *
     * @return {@literal true} if the AllocatableAction needs to reserve some resources for its execution,
     *         {@literal false} otherwise.
     */
    public abstract boolean isToReserveResources();

    /**
     * Returns whether the AllocatableAction releases some resources after its execution or not.
     *
     * @return {@literal true} if the AllocatableAction releases some resources after its execution, {@literal false}
     *         otherwise.
     */
    public abstract boolean isToReleaseResources();

    /**
     * Returns the description of the resources occupied during the action execution.
     *
     * @return The description of the resources occupied during the action execution.
     */
    public final WorkerResourceDescription getResourceConsumption() {
        return this.resourceConsumption;
    }

    /**
     * Returns the executing resources of the current AllocatableAction.
     *
     * @return The executing resources of the current AllocatableAction.
     */
    public final List<ResourceScheduler<? extends WorkerResourceDescription>> getExecutingResources() {
        return this.executingResources;
    }

    /*
     * ***************************************************************************************************************
     * EXECUTION TRIGGERS
     * ***************************************************************************************************************
     */

    /**
     * Triggers the action execution.
     */
    protected abstract void doAction();

    /**
     * Aborts the AllocatableAction execution.
     */
    public final void abortExecution() {
        if (this.state != State.RUNNING && this.state != State.WAITING) {
            // Action was not running -> Ignore request
            return;
        }

        // Release resources and run tasks blocked on the resource
        this.selectedResource.unhostAction(this);

        this.state = State.RUNNABLE;
        this.executingResources.remove(this.selectedResource);
        this.selectedResource = null;
        doAbort();
    }

    /**
     * Operations to perform when AA's execution has started.
     *
     * @return Freed stream dependency actions.
     */
    public final List<AllocatableAction> executionStarted() {
        return this.releaseStreamDataConsumers();
    }

    /**
     * Operations to perform when AA has been successfully completed. It calls specific operation doCompleted.
     *
     * @return Freed AllocatableActions after the completion of the current AllocatableAction.
     */
    public final List<AllocatableAction> completed() {
        // Mark as finished
        this.state = State.FINISHED;
        List<AllocatableAction> freeActions = releaseDataSuccessors();
        releaseMutexLocks(freeActions);

        // Action notification
        doCompleted();
        return freeActions;
    }

    /**
     * Operations to perform when AA has raised an error. Calls specific operation doError.
     *
     * @throws FailedActionException When the task fails.
     */
    public final void error() throws FailedActionException {
        // Mark as runnable since we can retry its execution
        this.state = State.RUNNABLE;

        // Release resources and run tasks blocked on the resource
        this.selectedResource.unhostAction(this);

        // Action notification
        doError();
    }

    /**
     * Operations to perform when AA has raised a COMPSs exception.
     *
     * @param e COMPSs Exception raised
     */
    public final List<AllocatableAction> exception(COMPSsException e) {
        // Mark as finished
        this.state = State.FAILED;

        cancelAction();

        // Action notification
        Collection<AllocatableAction> groupMembers = doException(e);

        // Triggering cancellation on tasks of the same group
        List<AllocatableAction> cancel = new LinkedList<>();

        // Forward cancellation to members of the same task group
        for (AllocatableAction aa : groupMembers) {
            if (aa.state == State.RUNNING || aa.state == State.WAITING || aa.state == State.RUNNABLE) {
                LOGGER.debug("[AllocatableAction] Cancelling action " + aa.id + " because in same group");
                cancel.addAll(aa.cancel());
            }
        }

        this.dataPredecessors.clear();
        this.dataSuccessors.clear();

        return cancel;
    }

    /**
     * Operations to perform when AA has totally failed. Calls specific operation doFailed.
     *
     * @return Failed successor Allocatable Actions.
     */
    public final List<AllocatableAction> failed() {
        // Mark as failed
        this.state = State.FAILED;

        cancelAction();

        // Triggering failure on Data Successors
        List<AllocatableAction> failed = new LinkedList<>();

        List<AllocatableAction> successors = new LinkedList<>();
        successors.addAll(this.dataSuccessors);

        // Failure notification
        doFailed();

        for (AllocatableAction succ : successors) {
            failed.addAll(succ.cancel());
        }

        this.dataPredecessors.clear();
        this.dataSuccessors.clear();

        return failed;
    }

    /**
     * Operations to perform when AA has totally failed but the failure is to be ignored. Calls specific operation
     * doFailed.
     *
     * @return List of released successors.
     */
    public final List<AllocatableAction> ignoredFailure() {

        // Mark as failed
        this.state = State.FAILED;

        cancelAction();

        // Failure notification
        doFailIgnored();

        // Release data dependencies of the task of all the successors that need to be executed
        List<AllocatableAction> releasedSuccessors = releaseDataSuccessors();

        releaseMutexLocks(releasedSuccessors);
        this.dataPredecessors.clear();

        return releasedSuccessors;
    }

    /**
     * Operations to perform when task successors have to be canceled. Calls specific operation doCanceled.
     *
     * @return List of cancelled successor Allocatable Actions.
     */
    public final List<AllocatableAction> cancel() {
        // Triggering cancelation on Data Successors
        List<AllocatableAction> cancel = new LinkedList<>();

        if (this.state == State.RUNNING) {
            try {
                this.state = State.CANCELLING;
                stopAction();
            } catch (Exception e) {
                LOGGER.error("Exception stoping action.", e);
            }
        } else {
            if (this.state == State.CANCELLING) {
                // Release resources and run tasks blocked on the resource
                this.selectedResource.unhostAction(this);
            }
            if (this.state != State.CANCELLED) {
                // Mark as canceled
                this.state = State.CANCELLED;

                cancelAction();

                List<AllocatableAction> successors = new LinkedList<>();
                successors.addAll(this.dataSuccessors);

                // Action notification
                boolean cancelSuccessors = doCanceled();

                if (cancelSuccessors) {
                    // Forward cancellation to successors

                    for (AllocatableAction succ : successors) {
                        if (!succ.isFinished()) {
                            LOGGER.debug("Cancelling action " + succ.getId()
                                + " because of successor of canceled action " + this.getId());
                            cancel.addAll(succ.cancel());
                        }
                    }
                }
                this.dataPredecessors.clear();
                this.dataSuccessors.clear();
            }
        }

        return cancel;
    }

    private void cancelAction() {
        // Cancel action
        boolean canceled = false;
        if (this.selectedResource != null) {
            while (!canceled) {
                try {
                    this.selectedResource.cancelAction(this);
                    canceled = true;
                } catch (ActionNotFoundException anfe) {
                    // Action could not be canceled since it was not scheduled to the resource
                    // Wait until a new resource is assigned
                    LOGGER.warn("[Allocatable Action] Action not found exception when canceling " + this);
                    while (this.selectedResource == null) {
                    }
                    // Try to cancel its execution on the new resource
                }
            }
        }
        // Predecessors -> ignore Action
        for (AllocatableAction pred : this.dataPredecessors) {
            pred.dataSuccessors.remove(this);
        }
    }

    /**
     * Cancels a running execution.
     *
     * @throws Exception Unstarted node exception.
     */
    protected abstract void stopAction() throws Exception;

    /**
     * Triggers the aborted action execution notification.
     */
    protected abstract void doAbort();

    /**
     * Triggers the successful job completion notification.
     */
    protected abstract void doCompleted();

    /**
     * Triggers a retry on a job completion.
     *
     * @throws FailedActionException When the action fails.
     */
    protected abstract void doError() throws FailedActionException;

    /**
     * Triggers a COMPSs exception on a job.
     *
     * @param e Exception arisen during the action
     * @return Other Allocatable actions to be cancelled due to the exception
     */
    protected abstract Collection<AllocatableAction> doException(COMPSsException e);

    /**
     * Triggers the unsuccessful action completion notification.
     */
    protected abstract void doFailed();

    /**
     * Triggers the cancellation action notification.
     *
     * @return if cancellation must be forwarded to successors
     */
    protected abstract boolean doCanceled();

    /**
     * Triggers the ignored unsuccessful action.
     */
    protected abstract void doFailIgnored();

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
    public abstract List<ResourceScheduler<? extends WorkerResourceDescription>> getCompatibleWorkers();

    /**
     * Returns all the possible implementations for the action.
     *
     * @return a list of implementations that can be executed to run the action.
     */
    public abstract Implementation[] getImplementations();

    /**
     * Tells is the action can run in a given resource.
     *
     * @param <W> WorkerResourceDescription.
     * @param r Resource where the action should run.
     * @return {@literal true} if the action can run in the given resource.
     */
    public abstract <W extends WorkerResourceDescription> boolean isCompatible(Worker<W> r);

    /**
     * Returns all the implementations for the action that can run on the given resource.
     *
     * @param <T> WorkerResourceDescription.
     * @param r resource that should run the action
     * @return list of the action implementations that can run on the resource.
     */
    public abstract <T extends WorkerResourceDescription> List<Implementation>
        getCompatibleImplementations(ResourceScheduler<T> r);

    /**
     * Returns the action priority.
     *
     * @return The action priority.
     */
    public abstract int getPriority();

    /**
     * Returns the action's MultiNodeGroup priority.
     *
     * @return The action's MultiNodeGroup priority.
     */
    public abstract long getGroupPriority();

    /**
     * Returns the behavior when action fails.
     *
     * @return The failing behavior.
     */
    public abstract OnFailure getOnFailure();

    /**
     * Returns the scheduling score of the action for a given worker.
     *
     * @param <T> WorkerResourceDescription.
     * @param targetWorker Target worker.
     * @param actionScore Scheduling action score.
     * @return Complete score at the given target worker.
     */
    public abstract <T extends WorkerResourceDescription> Score schedulingScore(ResourceScheduler<T> targetWorker,
        Score actionScore);

    /**
     * Schedules the action considering the {@code actionScore}. Actions can be scheduled on full workers.
     *
     * @param actionScore Scheduling action score.
     * @throws BlockedActionException When the action is blocked.
     * @throws UnassignedActionException When the action is not assigned to a target worker.
     */
    public abstract void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException;

    /**
     * Schedules the action considering a list of potential candidates where to run the task using the score
     * {@code actionScore}.
     *
     * @param candidates List of possible workers to host the action.
     * @param actionScore Scheduling action score.
     * @throws UnassignedActionException When the action is not assigned to any candidate worker.
     */
    public abstract void schedule(Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidates,
        Score actionScore) throws UnassignedActionException;

    /**
     * Schedules the action to a given {@code targetWorker} with score {@code actionScore}.
     *
     * @param targetWorker Target worker.
     * @param actionScore Scheduling action score.
     * @throws UnassignedActionException When the action is not assigned to a target worker.
     */
    public abstract void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker,
        Score actionScore) throws UnassignedActionException;

    /**
     * Schedules the implementation {@code impl} of the action to a given {@code targetWorker}.
     *
     * @param targetWorker Target worker.
     * @param impl Implementation to schedule.
     * @throws UnassignedActionException When the action is not assigned to a target worker.
     */
    public abstract void schedule(ResourceScheduler<? extends WorkerResourceDescription> targetWorker,
        Implementation impl) throws UnassignedActionException;

    /**
     * If the action was already assigned to any resource, it unassigns it and it is removed from the scheduling.
     *
     * @return List of freed actions.
     * @throws ActionNotFoundException When the action was assigned to a worker but the worker did not found it.
     * @throws UnassignedActionException When the action is not assigned to any worker.
     */
    public final List<AllocatableAction> unschedule() throws UnassignedActionException, ActionNotFoundException {
        if (this.selectedResource != null) {
            ResourceScheduler<? extends WorkerResourceDescription> target = this.selectedResource;
            if (this.state == State.RUNNING || this.state == State.WAITING) {
                target.unhostAction(this);
            }
            return target.unscheduleAction(this);
        } else {
            throw new UnassignedActionException();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("ID: " + this.id).append("\n");
        sb.append("HashCode ").append(this.hashCode()).append("\n");
        sb.append("\tdataPredecessors:");
        for (AllocatableAction aa : this.dataPredecessors) {
            sb.append(" ").append(aa.hashCode());
        }
        sb.append("\n");
        sb.append("\tdataSuccessors: ");
        for (AllocatableAction aa : this.dataSuccessors) {
            sb.append(" ").append(aa.hashCode());
        }
        sb.append("\n");
        sb.append(schedulingInfo);
        sb.append("\n");
        return sb.toString();
    }

}
