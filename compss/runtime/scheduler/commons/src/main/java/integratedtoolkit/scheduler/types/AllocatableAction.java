package integratedtoolkit.scheduler.types;

import integratedtoolkit.components.impl.TaskScheduler;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingInformation;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.util.ResourceScheduler;
import java.util.Iterator;
import java.util.LinkedList;
import org.apache.log4j.Logger;

public abstract class AllocatableAction {

    // Logger
    protected static final Logger logger = Logger.getLogger(Loggers.TS_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    public static interface ActionOrchestrator {

        public void actionCompletion(AllocatableAction action);

        public void actionError(AllocatableAction action);
    }

    private enum State {

        RUNNABLE,
        RUNNING,
        FINISHED,
        FAILED
    }

    public static ActionOrchestrator orchestrator;

    //Allocatable actions that the action depends on due data dependencies
    private final LinkedList<AllocatableAction> dataPredecessors;
    //Allocatable actions depending on the allocatable action due data dependencies
    private final LinkedList<AllocatableAction> dataSuccessors;

    private State state;
    protected ResourceScheduler selectedResource;
    protected Implementation selectedImpl;
    protected final LinkedList<ResourceScheduler> executingResources;

    private final SchedulingInformation schedulingInfo;

    protected Profile profile;

    public AllocatableAction(SchedulingInformation schedulingInformation) {
        state = State.RUNNABLE;
        dataPredecessors = new LinkedList();
        dataSuccessors = new LinkedList();
        selectedResource = null;
        executingResources = new LinkedList();
        schedulingInfo = schedulingInformation;
    }

    /* ------------------------------------------------
     * ------------------------------------------------
     * ----------- PREDECESSORS MANAGEMENT ------------
     * ------------------------------------------------
     * ----------------------------------------------*/
    public final void addDataPredecessor(AllocatableAction predecessor) {
        if (predecessor.isPending()) {
            dataPredecessors.add(predecessor);
            predecessor.dataSuccessors.add(this);
        }

    }

    private void dataPredecessorDone(AllocatableAction finishedAction) {
        Iterator<AllocatableAction> it = dataPredecessors.iterator();
        while (it.hasNext()) {
            AllocatableAction aa = it.next();
            if (aa == finishedAction) {
                it.remove();
            }
        }
    }

    public LinkedList<AllocatableAction> getDataPredecessors() {
        return dataPredecessors;
    }

    public LinkedList<AllocatableAction> getDataSuccessors() {
        return dataSuccessors;
    }

    public final boolean hasDataPredecessors() {
        return dataPredecessors.size() > 0;
    }

    public void setResourceConstraint(AllocatableAction predecessor) {
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
    public AllocatableAction getConstrainingPredecessor() {
        return schedulingInfo.getConstrainingPredecessor();
    }

    protected LinkedList<ResourceScheduler> getCoreElementExecutors(int coreId) {
        return schedulingInfo.getCoreElementExecutors(coreId);
    }

    public SchedulingInformation getSchedulingInfo() {
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

    public long getStartTime() {
        return profile.getStartTime();
    }

    public void assignImplementation(Implementation impl) {
        selectedImpl = impl;
    }

    /**
     * Tells the implementation assigned to the action
     *
     * @return
     */
    public Implementation getAssignedImplementation() {
        return selectedImpl;
    }

    public void assignResource(ResourceScheduler res) {
        selectedResource = res;
    }

    /**
     * Tells the resource assigned to the action
     *
     * @return
     */
    public ResourceScheduler getAssignedResource() {
        return selectedResource;
    }

    public void tryToLaunch() throws BlockedActionException, UnassignedActionException, InvalidSchedulingException {
        if ( //Has an assigned resource where to run
                selectedResource != null
                && //has no dependencies with other methods
                !hasDataPredecessors() && schedulingInfo.isExecutable()) {

            if (isSchedulingConstrained() && this.getConstrainingPredecessor().getAssignedResource() != selectedResource) {
                throw new InvalidSchedulingException();
            } else {
                execute();
            }
        }
    }

    private void execute() {
        logger.info(this + " execution starts on worker " + selectedResource.getName());
        executingResources.add(selectedResource);
        if (!selectedResource.hasBlockedTasks() && areEnoughResources()) {
            run();
        } else {
            logger.info(this + " execution paused due to lack of resources on worker " + selectedResource.getName());
            selectedResource.waitOnResource(this);
        }
    }

    private void run() {
        state = State.RUNNING;
        reserveResources();
        selectedResource.hostAction(this);
        profile = selectedResource.generateProfileForAllocatable();
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

    public final LinkedList<AllocatableAction> completed() {
        //Mark as finished
        state = State.FINISHED;

        //Release resources and run tasks blocked on the resource
        releaseResources();
        selectedResource.unhostAction(this);
        while (selectedResource.hasBlockedTasks()) {
            AllocatableAction firstBlocked = selectedResource.getFirstBlocked();
            if (firstBlocked.areEnoughResources()) {
                selectedResource.removeFirstBlocked();
                logger.info(this + " execution resumed on worker " + selectedResource.getName());
                firstBlocked.run();
            } else {
                break;
            }
        }

        //Action notification
        doCompleted();

        //Release data dependencies of the task
        LinkedList<AllocatableAction> freeTasks = new LinkedList<AllocatableAction>();
        //Release data dependencies of the task
        for (AllocatableAction aa : dataSuccessors) {
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
        while (selectedResource.hasBlockedTasks()) {
            AllocatableAction firstblocked = selectedResource.getFirstBlocked();
            if (firstblocked.areEnoughResources()) {
                selectedResource.removeFirstBlocked();
                logger.info(this + " execution resumed on worker " + selectedResource.getName());
                firstblocked.run();
            } else {
                break;
            }
        }

        //Action notification
        doError();
    }

    protected abstract void doError() throws FailedActionException;

    public final LinkedList<AllocatableAction> failed() {
        LinkedList<AllocatableAction> failed = new LinkedList();
        state = State.FAILED;

        //Predecessors -> ignore Action
        for (AllocatableAction pred : dataPredecessors) {
            pred.dataSuccessors.remove(this);
        }

        selectedResource.cancelAction(this);

        //Remove data links
        //Triggering failure on Data Predecessors
        for (AllocatableAction succ : dataSuccessors) {
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
        for (AllocatableAction aa : dataPredecessors) {
            sb.append(" ").append(aa.hashCode());
        }
        sb.append("\n");
        sb.append("\tdataSuccessors: ");
        for (AllocatableAction aa : dataSuccessors) {
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
    public abstract LinkedList<ResourceScheduler> getCompatibleWorkers();

    /**
     * Returns all the possible implementations for the action.
     *
     * @return a list of implementations that can be executed to run the action.
     */
    public abstract Implementation[] getImplementations();

    /**
     * Tells is the action can run in a given resource.
     *
     * @param r Resource where the action should run.
     *
     * @return {@literal true} if the action can run in the given resource.
     */
    public abstract boolean isCompatible(Worker r);

    /**
     * Returns all the implementations for the action that can run on the given
     * resource.
     *
     * @param r resource that should run the action
     *
     * @return list of the action implementations that can run on the resource.
     */
    public abstract LinkedList<Implementation> getCompatibleImplementations(ResourceScheduler r);

    public abstract Score schedulingScore(TaskScheduler ts);

    public abstract Score schedulingScore(ResourceScheduler targetWorker, Score actionScore);

    public abstract void schedule(Score actionScore) throws BlockedActionException, UnassignedActionException;

    public abstract void schedule(ResourceScheduler targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException;

    public static class BlockedActionException extends Exception {
    }

    public static class UnassignedActionException extends Exception {
    }

    public static class InvalidSchedulingException extends Exception {
    }

    public static class FailedActionException extends Exception {
    }
}
