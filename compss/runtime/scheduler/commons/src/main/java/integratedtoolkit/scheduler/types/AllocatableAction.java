package integratedtoolkit.scheduler.types;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.concurrent.locks.ReentrantLock;

import org.apache.log4j.Logger;

public abstract class AllocatableAction<P extends Profile, T extends WorkerResourceDescription> {

    // Logger
    protected static final Logger logger = Logger.getLogger(Loggers.TS_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    public static interface ActionOrchestrator {

        public void actionCompletion(AllocatableAction<?, ?> action);

        public void actionError(AllocatableAction<?, ?> action);
    }

    private enum State {
        RUNNABLE,
        WAITING,
        RUNNING,
        FINISHED,
        FAILED
    }

    public static ActionOrchestrator orchestrator;

    private static long nextId = 0;

    private final long id;
    //Allocatable actions that the action depends on due data dependencies
    private final LinkedList<AllocatableAction<P, T>> dataPredecessors;
    //Allocatable actions depending on the allocatable action due data dependencies
    private final LinkedList<AllocatableAction<P, T>> dataSuccessors;

    private State state;
    protected ResourceScheduler<P, T> selectedResource;
    protected Implementation<T> selectedImpl;
    protected final LinkedList<ResourceScheduler<P, T>> executingResources;

    private final SchedulingInformation<P, T> schedulingInfo;

    protected Profile profile;

    //Lock to avoid many threads to modify the same action
    private final ReentrantLock lock = new ReentrantLock();

    public AllocatableAction(SchedulingInformation<P, T> schedulingInformation) {
        id = nextId++;
        state = State.RUNNABLE;
        dataPredecessors = new LinkedList<AllocatableAction<P, T>>();
        dataSuccessors = new LinkedList<AllocatableAction<P, T>>();
        selectedResource = null;
        executingResources = new LinkedList<ResourceScheduler<P, T>>();
        schedulingInfo = schedulingInformation;
    }

    public long getId() {
        return id;
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * ----------- PREDECESSORS MANAGEMENT ------------
     * ------------------------------------------------
     * They should only be updated by the main thread of the task Dispatcher
     * ----------------------------------------------*/
    public final void addDataPredecessor(AllocatableAction<P, T> predecessor) {
        if (predecessor.isPending()) {
            dataPredecessors.add(predecessor);
            predecessor.dataSuccessors.add(this);
        }

    }

    private void dataPredecessorDone(AllocatableAction<P, T> finishedAction) {
        Iterator<AllocatableAction<P, T>> it = dataPredecessors.iterator();
        while (it.hasNext()) {
            AllocatableAction<P, T> aa = it.next();
            if (aa == finishedAction) {
                it.remove();
            }
        }
    }

    public LinkedList<AllocatableAction<P, T>> getDataPredecessors() {
        return dataPredecessors;
    }

    public LinkedList<AllocatableAction<P, T>> getDataSuccessors() {
        return dataSuccessors;
    }

    public final boolean hasDataPredecessors() {
        return dataPredecessors.size() > 0;
    }

    public void setResourceConstraint(AllocatableAction<P, T> predecessor) {
        schedulingInfo.setResourceConstraint(predecessor);
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * ----------- RESOURCES MANAGEMENT ------------
     * ------------------------------------------------
     * ----------------------------------------------*/
    /**
     * Tells if the action has to run in the same resource as another action.
     *
     * @return {@literal true} if the action scheduling is constrained to a
     * certain resource.
     */
    public boolean isSchedulingConstrained() {
        return schedulingInfo.getConstrainingPredecessor() != null;
    }

    /**
     * Tells the action whose running resource should execute this action.
     *
     * @return action that constraints the scheduling of the action.
     */
    public AllocatableAction<P, T> getConstrainingPredecessor() {
        return schedulingInfo.getConstrainingPredecessor();
    }

    protected LinkedList<ResourceScheduler<?, ?>> getCoreElementExecutors(int coreId) {
        return schedulingInfo.getCoreElementExecutors(coreId);
    }

    public SchedulingInformation<P, T> getSchedulingInfo() {
        return schedulingInfo;
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * ----EXECUTION AND LIFECYCLE MANAGEMENT ---------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public boolean isPending() {
        return state != State.FAILED && state != State.FINISHED;
    }

    public Long getStartTime() {
        if (profile == null) {
            return null;
        }
        return profile.getStartTime();
    }

    public void assignImplementation(Implementation<T> impl) {
        if (state == State.RUNNABLE) {
            selectedImpl = impl;
        }
    }

    /**
     * Tells the implementation assigned to the action
     *
     * @return
     */
    public Implementation<T> getAssignedImplementation() {
        return selectedImpl;
    }

    public void assignResource(ResourceScheduler<P, T> res) {
        if (state == State.RUNNABLE) {
            selectedResource = res;
        }
    }

    /**
     * Tells the resource assigned to the action
     *
     * @return
     */
    public ResourceScheduler<P, T> getAssignedResource() {
        return selectedResource;
    }

    public void tryToLaunch() throws InvalidSchedulingException {
        //gets the lock on the action
        lock.lock();
        if ( // has an assigned resource where to run
                selectedResource != null
                && // has not been started yet
                state == State.RUNNABLE
                && // has no data dependencies with other methods
                !hasDataPredecessors()
                && // scheduler does not block the execution
                schedulingInfo.isExecutable()) {

            //Invalid scheduling -> Should run in a specific resource and the assigned resource is not the required
            if (isSchedulingConstrained() && this.getConstrainingPredecessor().getAssignedResource() != selectedResource) {
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

    private void execute() {
        logger.info(this + " execution starts on worker " + selectedResource.getName());

        // there are enough resources to host the actions and no waiting tasks in the queue
        if (!selectedResource.hasBlockedActions() && areEnoughResources()) {
            // register executing resource
            executingResources.add(selectedResource);
            // Run action
            run();
        } else {
            logger.info(this + " execution paused due to lack of resources on worker " + selectedResource.getName());
            // Task waits on the resource queue
            // It can only be resumed because of a task completion or error.
            // execute won't be executed again since tryToLaunch is blocked
            state = State.WAITING;
            selectedResource.waitOnResource(this);

            // Allow other threads to execute the task (complete and error executor)
            lock.unlock();
        }
    }

    private void run() {
        //Actually runs the action. This function is called only once per action (except for reschedules)
        // Blocks other tryToLaunch
        state = State.RUNNING;
        // Allow other threads to execute the task (complete and error executor)
        lock.unlock();
        reserveResources();
        profile = selectedResource.generateProfileForAllocatable();
        selectedResource.hostAction(this);
        doAction();
    }

    protected abstract boolean areEnoughResources();

    protected abstract void reserveResources();

    protected abstract void releaseResources();

    /**
     * Triggers the action
     */
    protected abstract void doAction();

    protected final void notifyCompleted() {
        orchestrator.actionCompletion(this);
    }

    public final LinkedList<AllocatableAction<P, T>> completed() {
        //Mark as finished
        state = State.FINISHED;

        //Release resources and run tasks blocked on the resource
        releaseResources();
        selectedResource.unhostAction(this);

        while (selectedResource.hasBlockedActions()) {
            AllocatableAction<P, T> firstBlocked = selectedResource.getFirstBlocked();
            if (firstBlocked.areEnoughResources()) {
                selectedResource.removeFirstBlocked();
                logger.info(this + " execution resumed on worker " + selectedResource.getName());
                firstBlocked.lock.lock();
                firstBlocked.run();
            } else {
                break;
            }
        }
        //Action notification
        doCompleted();

        //Release data dependencies of the task
        LinkedList<AllocatableAction<P, T>> freeTasks = new LinkedList<AllocatableAction<P, T>>();
        //Release data dependencies of the task
        for (AllocatableAction<P, T> aa : dataSuccessors) {
            aa.dataPredecessorDone(this);
            if (!aa.hasDataPredecessors()) {
                freeTasks.add(aa);
            }
        }
        dataSuccessors.clear();

        return freeTasks;
    }

    /**
     * Triggers the successful job completion notification
     */
    protected abstract void doCompleted();

    protected final void notifyError() {
        orchestrator.actionError(this);
    }

    public final void error() throws FailedActionException {
        //Mark as finished
        state = State.RUNNABLE;
        //Release resources and run tasks blocked on the resource
        releaseResources();
        selectedResource.unhostAction(this);
        while (selectedResource.hasBlockedActions()) {
            AllocatableAction<P, T> firstBlocked = selectedResource.getFirstBlocked();
            if (firstBlocked.areEnoughResources()) {
                selectedResource.removeFirstBlocked();
                logger.info(this + " execution resumed on worker " + selectedResource.getName());
                firstBlocked.lock.lock();
                firstBlocked.run();
            } else {
                break;
            }
        }

        //Action notification
        doError();
    }

    protected abstract void doError() throws FailedActionException;

    public final LinkedList<AllocatableAction<P, T>> failed() {
        LinkedList<AllocatableAction<P, T>> failed = new LinkedList<AllocatableAction<P, T>>();
        state = State.FAILED;

        //Predecessors -> ignore Action
        for (AllocatableAction<P, T> pred : dataPredecessors) {
            pred.dataSuccessors.remove(this);
        }

        selectedResource.cancelAction(this);

        //Remove data links
        //Triggering failure on Data Predecessors
        for (AllocatableAction<P, T> succ : dataSuccessors) {
            failed.addAll(succ.failed());
        }

        dataPredecessors.clear();
        dataSuccessors.clear();

        doFailed();
        failed.add(this);
        return failed;
    }

    /**
     * Triggers the unsuccessful action completion notification
     *
     */
    protected abstract void doFailed();

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("HashCode ").append(this.hashCode()).append("\n");
        sb.append("\tdataPredecessors:");
        for (AllocatableAction<P, T> aa : dataPredecessors) {
            sb.append(" ").append(aa.hashCode());
        }
        sb.append("\n");
        sb.append("\tdataSuccessors: ");
        for (AllocatableAction<P, T> aa : dataSuccessors) {
            sb.append(" ").append(aa.hashCode());
        }
        sb.append("\n");
        sb.append(schedulingInfo);
        sb.append("\n");
        return sb.toString();
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * ------------ SCHEDULING MANAGEMENT -------------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public abstract Integer getCoreId();

    /**
     * Returns all the workers that are able to run the action.
     *
     * @return list of resources able to run the action
     */
    public abstract LinkedList<ResourceScheduler<?, ?>> getCompatibleWorkers();

    /**
     * Returns all the possible implementations for the action.
     *
     * @return a list of implementations that can be executed to run the action.
     */
    public abstract Implementation<T>[] getImplementations();

    /**
     * Tells is the action can run in a given resource.
     *
     * @param r Resource where the action should run.
     *
     * @return {@literal true} if the action can run in the given resource.
     */
    public abstract boolean isCompatible(Worker<T> r);

    /**
     * Returns all the implementations for the action that can run on the given
     * resource.
     *
     * @param r resource that should run the action
     *
     * @return list of the action implementations that can run on the resource.
     */
    public abstract LinkedList<Implementation<T>> getCompatibleImplementations(ResourceScheduler<P, T> r);

    public abstract int getPriority();

    public abstract Score schedulingScore(ResourceScheduler<P, T> targetWorker, Score actionScore);

    public abstract void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException;

    public abstract void schedule(ResourceScheduler<P, T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException;

    public abstract void schedule(ResourceScheduler<P, T> targetWorker, Implementation impl) throws BlockedActionException, UnassignedActionException;

}
