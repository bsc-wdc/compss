package integratedtoolkit.components.impl;

import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.ObjectValue;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.allocatableactions.StartWorkerAction;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ActionSet;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TaskScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    // Reference to action orchestrator (Task Dispatcher)
    private ActionOrchestrator<P, T, I> orchestrator;

    // Map of available workers and its resource schedulers
    private final HashMap<Worker<T, I>, ResourceScheduler<P, T, I>> workers;

    // List of blocked actions
    private final ActionSet<P, T, I> blockedActions;

    // Number of ready tasks for each coreId
    private int[] readyCounts;


    /**
     * Construct a new Task Scheduler
     * 
     */
    public TaskScheduler() {
        this.workers = new HashMap<>();
        this.blockedActions = new ActionSet<>();
        this.readyCounts = new int[CoreManager.getCoreCount()];
    }

    /**
     * Assigns the action orchestrator to this scheduler
     * 
     * @param orchestrator
     */
    public final void setOrchestrator(ActionOrchestrator<P, T, I> orchestrator) {
        this.orchestrator = orchestrator;
    }

    /**
     * Returns the action orchestator assigned to this scheduler
     * 
     * @return
     */
    public final ActionOrchestrator<P, T, I> getOrchestrator() {
        return this.orchestrator;
    }

    /**
     * Shutdown the Task Scheduler
     * 
     */
    public void shutdown() {
        // Nothing to do
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * New Core Elements have been detected; the Task Scheduler needs to be notified to modify any internal structure
     * using that information.
     *
     */
    public final void coreElementsUpdated() {
        LOGGER.info("[TaskScheduler] Update core elements");
        this.blockedActions.updateCoreCount();
        this.readyCounts = new int[CoreManager.getCoreCount()];
    }

    /**
     * New worker has been detected; the Task Scheduler is notified to modify any internal structure using that
     * information.
     *
     * @param resource
     *            new worker
     */
    protected void workerDetected(ResourceScheduler<P, T, I> resource) {
        LOGGER.info("[TaskScheduler] New worker " + resource.getName() + " detected");
        // There are no internal structures worker-related. No need to do anything.
    }

    /**
     * One worker has been removed from the pool; the Task Scheduler is notified to modify any internal structure using
     * that information.
     *
     * @param resource
     *            removed worker
     */
    protected void workerRemoved(ResourceScheduler<P, T, I> resource) {
        LOGGER.info("[TaskScheduler] Remove worker " + resource.getName());
        // There are no internal structures worker-related. No need to do anything.
    }

    /**
     * Updates the worker information
     * 
     * @param worker
     */
    public final void updatedWorker(Worker<T, I> worker) {
        LOGGER.info("[TaskScheduler] Updating worker " + worker.getName() + " information");
        ResourceScheduler<P, T, I> ui = this.workers.get(worker);
        if (ui == null) {
            // Register worker if it's the first time it is useful.
            ui = generateSchedulerForResource(worker);
            synchronized (this.workers) {
                this.workers.put(worker, ui);
            }

            // Add action to start worker
            StartWorkerAction<P, T, I> action = new StartWorkerAction<>(generateSchedulingInformation(), ui, this);
            newAllocatableAction(action);
            workerDetected(ui);
        }

        // Update links CE -> Worker
        SchedulingInformation.changesOnWorker(ui);

        if (ui.getExecutableCores().isEmpty()) {
            // Remove useless workers
            synchronized (this.workers) {
                this.workers.remove(ui.getResource());
            }
            workerRemoved(ui);
        } else {
            // Inspect blocked actions to be freed
            LinkedList<AllocatableAction<P, T, I>> compatibleActions = this.blockedActions.removeAllCompatibleActions(worker);

            // Prioritize them
            PriorityQueue<ObjectValue<AllocatableAction<P, T, I>>> sortedCompatibleActions = new PriorityQueue<>();
            for (AllocatableAction<P, T, I> action : compatibleActions) {
                ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(action, generateActionScore(action));
                sortedCompatibleActions.add(obj);
            }
            // Schedule them
            while (!sortedCompatibleActions.isEmpty()) {
                ObjectValue<AllocatableAction<P, T, I>> obj = sortedCompatibleActions.poll();
                Score actionScore = obj.getScore();
                AllocatableAction<P, T, I> action = obj.getObject();

                if (!action.hasDataPredecessors()) {
                    addToReady(action);
                }

                try {
                    scheduleAction(action, actionScore);
                    tryToLaunch(action);
                } catch (BlockedActionException bae) {
                    removeFromReady(action);
                    addToBlocked(action);
                }
            }

            // Update worker load
            workerLoadUpdate(ui);
        }
    }

    /**
     * Notifies to the scheduler that there have been changes in the load of a resource.
     *
     * @param resources
     *            updated resource
     */
    public void workerLoadUpdate(ResourceScheduler<P, T, I> resource) {
        LOGGER.info("[TaskScheduler] Update load on worker " + resource.getName());
        // Resource capabilities had already been taken into account when assigning the actions. No need to change the
        // assignation.
    }

    /**
     * Generates a ResourceScheduler for the worker @w
     * 
     * @param w
     * @return
     */
    public ResourceScheduler<P, T, I> generateSchedulerForResource(Worker<T, I> w) {
        LOGGER.info("[TaskScheduler] Generate scheduler for resource " + w.getName());
        return new ResourceScheduler<P, T, I>(w);
    }

    /**
     * Generates an empty Scheduling Information
     * 
     * @return
     */
    public SchedulingInformation<P, T, I> generateSchedulingInformation() {
        LOGGER.info("[TaskScheduler] Generate empty scheduling information");
        return new SchedulingInformation<P, T, I>();
    }

    /**
     * Generates a action score
     * 
     * @param action
     * @return
     */
    public Score generateActionScore(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[TaskScheduler] Generate priority action score");
        return new Score(action.getPriority(), 0, 0, 0);
    }

    /**
     * Increases the ready coreId counter
     * 
     * @param action
     */
    protected final void addToReady(AllocatableAction<P, T, I> action) {
        LOGGER.debug("[TaskScheduler] Add action " + action + " to ready count");
        if (action.getImplementations().length > 0) {
            Integer coreId = action.getImplementations()[0].getCoreId();
            if (coreId != null) {
                this.readyCounts[coreId]++;
            }
        }
    }

    /**
     * Decreases the ready coreId counter
     * 
     * @param action
     */
    protected final void removeFromReady(AllocatableAction<P, T, I> action) {
        LOGGER.info("[TaskScheduler] Remove action " + action + " from ready count");
        if (action.getImplementations().length > 0) {
            Integer coreId = action.getImplementations()[0].getCoreId();
            if (coreId != null) {
                this.readyCounts[coreId]--;
            }
        }
    }

    /**
     * Adds the action to the blocked list
     * 
     * @param action
     */
    protected final void addToBlocked(AllocatableAction<P, T, I> action) {
        LOGGER.warn("[TaskScheduler] Blocked Action: " + action);
        this.blockedActions.addAction(action);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Introduces a new action in the Scheduler system. The method should place the action in a resource hurriedly
     *
     * @param action
     *            Action to be scheduled.
     */
    public void newAllocatableAction(AllocatableAction<P, T, I> action) {
        LOGGER.info("[TaskScheduler] Registering new AllocatableAction " + action);

        if (!action.hasDataPredecessors()) {
            addToReady(action);
        }

        Score actionScore = generateActionScore(action);
        try {
            scheduleAction(action, actionScore);
            tryToLaunch(action);
        } catch (BlockedActionException bae) {
            removeFromReady(action);
            addToBlocked(action);
        }
    }

    /**
     * Registers an action as completed and releases all the resource and data dependencies.
     *
     * @param action
     *            action that has finished
     */
    public void actionCompleted(AllocatableAction<P, T, I> action) {
        LOGGER.info("[TaskScheduler] Action completed " + action);
        // Mark action as finished
        ResourceScheduler<P, T, I> resource = action.getAssignedResource();
        removeFromReady(action);
        LinkedList<AllocatableAction<P, T, I>> resourceFree = resource.unscheduleAction(action);

        // Get the data free actions and mark them as ready
        LinkedList<AllocatableAction<P, T, I>> dataFreeActions = action.completed();
        LinkedList<AllocatableAction<P, T, I>> executionCandidates = new LinkedList<>();
        for (AllocatableAction<P, T, I> dataFreeAction : dataFreeActions) {
            if (dataFreeAction.isNotScheduling()) {
                addToReady(dataFreeAction);
                executionCandidates.add(dataFreeAction);
            }
        }

        // Schedule data free actions
        LinkedList<AllocatableAction<P, T, I>> blockedCandidates = new LinkedList<>();
        // Actions can only be scheduled and those that remain blocked must be added to the blockedCandidates list
        // and those that remain unassigned must be added to the unassigned list
        handleDependencyFreeActions(executionCandidates, blockedCandidates, resource);
        for (AllocatableAction<P, T, I> aa : blockedCandidates) {
            removeFromReady(aa);
            addToBlocked(aa);
        }

        // We update the worker load
        workerLoadUpdate(resource);

        // Try to launch all the data free actions and the resource free actions
        PriorityQueue<ObjectValue<AllocatableAction<P, T, I>>> executableActions = new PriorityQueue<>();
        for (AllocatableAction<P, T, I> freeAction : executionCandidates) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(freeAction, fullScore);
            executableActions.add(obj);
        }
        for (AllocatableAction<P, T, I> freeAction : resourceFree) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(freeAction, fullScore);
            if (!executableActions.contains(obj)) {
                executableActions.add(obj);
            }
        }
        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction<P, T, I>> obj = executableActions.poll();
            AllocatableAction<P, T, I> freeAction = obj.getObject();

            // LOGGER.debug("Trying to launch action " + freeAction);
            try {
                scheduleAction(freeAction, obj.getScore());
                tryToLaunch(freeAction);
            } catch (BlockedActionException e) {
                removeFromReady(freeAction);
                addToBlocked(freeAction);
            }
        }
    }

    /**
     * Registers an error on the action given as a parameter. The action itself processes the error and triggers with
     * any possible solution to re-execute it. This code is executed only on re-schedule (no resubmit)
     *
     * @param action
     *            action raising the error
     */
    public void errorOnAction(AllocatableAction<P, T, I> action) {
        LOGGER.warn("[TaskScheduler] Error on action " + action);

        LinkedList<AllocatableAction<P, T, I>> resourceFree = new LinkedList<>();
        ResourceScheduler<P, T, I> resource = action.getAssignedResource();

        // Process the action error (removes the assigned resource)
        try {
            action.error();
        } catch (FailedActionException fae) {
            // Action has completely failed
            LOGGER.warn("[TaskScheduler] Action completely failed " + action);
            removeFromReady(action);
            addToBlocked(action);
            // Free all the dependent tasks
            for (AllocatableAction<P, T, I> failed : action.failed()) {
                resourceFree.addAll(resource.unscheduleAction(failed));
            }
        }

        // We free the current task and get the free actions from the resource
        resourceFree.addAll(resource.unscheduleAction(action));
        workerLoadUpdate(resource);

        // Try to re-schedule the action
        Score actionScore = generateActionScore(action);
        try {
            scheduleAction(action, actionScore);
            tryToLaunch(action);
        } catch (BlockedActionException bae) {
            removeFromReady(action);
            addToBlocked(action);
        }

        // Try to launch all the data free actions and the resource free actions
        PriorityQueue<ObjectValue<AllocatableAction<P, T, I>>> executionCandidates = new PriorityQueue<>();
        for (AllocatableAction<P, T, I> resoruceFreeAction : resourceFree) {
            Score freeActionScore = generateActionScore(resoruceFreeAction);
            Score fullScore = resoruceFreeAction.schedulingScore(resource, freeActionScore);
            ObjectValue<AllocatableAction<P, T, I>> obj = new ObjectValue<>(resoruceFreeAction, fullScore);
            executionCandidates.add(obj);
        }
        while (!executionCandidates.isEmpty()) {
            ObjectValue<AllocatableAction<P, T, I>> obj = executionCandidates.poll();
            AllocatableAction<P, T, I> freeAction = obj.getObject();
            tryToLaunch(freeAction);
        }
    }

    protected final void tryToLaunch(AllocatableAction<P, T, I> action) {
        try {
            // LOGGER.debug("[TaskScheduler] Trying to launch" + action);
            action.tryToLaunch();
            // LOGGER.debug("[TaskScheduler] Exited from tryToLaunch without exception");
        } catch (InvalidSchedulingException ise) {
            // LOGGER.debug("[TaskScheduler] There was a bad scheduling" + action);
            action.getAssignedResource().unscheduleAction(action);
            Score actionScore = generateActionScore(action);
            boolean actionReassigned = false;
            for (int i = 0; i < action.getConstrainingPredecessors().size() && !actionReassigned; ++i) {
                AllocatableAction<P, T, I> predecessor = action.getConstrainingPredecessors().get(i);
                try {
                    action.schedule(predecessor.getAssignedResource(), actionScore);
                    action.tryToLaunch();
                    actionReassigned = true;
                } catch (UnassignedActionException | BlockedActionException | InvalidSchedulingException ex) {
                    // Action can be reassigned to other constraining predecessors, we just log the exception
                    LOGGER.warn("Action " + action + " cannot be reassigned to " + predecessor.getAssignedResource().getName(), ex);
                }
            }

            if (!actionReassigned) {
                StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
                info.append(action.toString());
                ErrorManager.fatal(info.toString());
            }
        }
    }

    /**
     * Plans the execution of a given action in one of the compatible resources. The solution should be computed
     * hurriedly since it blocks the runtime thread and this initial allocation can be modified by the scheduler later
     * on the execution.
     *
     * @param action
     *            Action whose execution has to be allocated
     * @throws integratedtoolkit.scheduler.types.Action.BlockedActionException
     *
     */
    protected void scheduleAction(AllocatableAction<P, T, I> action, Score actionScore) throws BlockedActionException {
        LOGGER.debug("[TaskScheduler] Schedule action " + action);
        try {
            action.schedule(actionScore);
        } catch (UnassignedActionException ure) {
            StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
            info.append(action.toString());
            ErrorManager.fatal(info.toString());
        }
    }

    /**
     * Plans the execution of a given action in one of the compatible resources. The solution should be computed
     * hurriedly since it blocks the runtime thread and this initial allocation can be modified by the scheduler later
     * on the execution.
     *
     * @param action
     *            Action whose execution has to be allocated
     * @throws integratedtoolkit.scheduler.types.Action.BlockedActionException
     *
     */
    protected void scheduleAction(AllocatableAction<P, T, I> action, ResourceScheduler<P, T, I> targetWorker, Score actionScore)
            throws BlockedActionException, UnassignedActionException {
        
        LOGGER.debug("[TaskScheduler] Schedule action " + action);
        action.schedule(targetWorker, actionScore);
    }

    /**
     * Notifies to the scheduler that some actions have become free of data dependencies.
     * 
     * @param executionCandidates
     * @param unassignedCandidates
     *            OUT, list of unassigned candidates
     * @param blockedCandidates
     *            OUT, list of blocked candidates
     * @throws UnassignedActionException
     * @throws BlockedActionException
     */
    public void handleDependencyFreeActions(LinkedList<AllocatableAction<P, T, I>> executionCandidates,
            LinkedList<AllocatableAction<P, T, I>> blockedCandidates, ResourceScheduler<P, T, I> resource) {

        LOGGER.debug("[TaskScheduler] Treating dependency free actions");

        // All actions should have already been assigned to a resource, no need
        // to change the assignation once they become free of dependencies
        for (AllocatableAction<P, T, I> action : executionCandidates) {
            if (action.getAssignedResource() == null) {
                Score actionScore = generateActionScore(action);
                try {
                    action.schedule(resource, actionScore);
                    tryToLaunch(action);
                    executionCandidates.remove(action);
                } catch (BlockedActionException e) {
                    blockedCandidates.add(action);
                } catch (UnassignedActionException ex) {
                    // Nothing to do, action stay in executionCandidates queue
                }
            }
        }
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * *********************************** GETTER OPERATIONS ***************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Returns a list of the blocked actions
     * 
     * @return
     */
    public final LinkedList<AllocatableAction<P, T, I>> getBlockedActions() {
        LOGGER.info("[TaskScheduler] Get Blocked Actions");
        // Parameter null to get all blocked actions
        return this.blockedActions.getActions(null);
    }

    /**
     * Returns a list with the hosted actions on a given worker
     * 
     * @param worker
     * @return
     */
    public final LinkedList<AllocatableAction<P, T, I>> getHostedActions(Worker<T, I> worker) {
        LOGGER.info("[TaskScheduler] Get Hosted actions on worker " + worker.getName());
        ResourceScheduler<P, T, I> ui = workers.get(worker);
        if (ui != null) {
            return ui.getHostedActions();
        } else {
            return new LinkedList<AllocatableAction<P, T, I>>();
        }
    }

    /**
     * Returns the blocked actions assigned to a given resource
     * 
     * @param worker
     * @return
     */
    public final PriorityQueue<AllocatableAction<P, T, I>> getBlockedActionsOnResource(Worker<T, I> worker) {
        LOGGER.info("[TaskScheduler] Get Blocked actions on worker " + worker.getName());
        ResourceScheduler<P, T, I> ui = workers.get(worker);
        if (ui != null) {
            return ui.getBlockedActions();
        } else {
            return new PriorityQueue<AllocatableAction<P, T, I>>();
        }
    }

    /**
     * Returns the ResourceSchedulers assigned to all available workers
     * 
     * @return
     */
    public final Collection<ResourceScheduler<P, T, I>> getWorkers() {
        LOGGER.info("[TaskScheduler] Get all worker resource schedulers");
        return this.workers.values();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* MONITORING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Prints the task summary on a given logger @logger
     * 
     * @param logger
     */
    public final void getTaskSummary(Logger logger) {
        LOGGER.info("[TaskScheduler] Get task summary");
        // Structures for global and per worker stats
        int coreCount = CoreManager.getCoreCount();
        Profile[] coreGlobalProfiles = new Profile[coreCount];
        for (int i = 0; i < coreCount; ++i) {
            coreGlobalProfiles[i] = new Profile();
        }
        HashMap<String, Profile[]> coreProfilesPerWorker = new HashMap<>();

        // Retrieve information
        for (ResourceScheduler<P, T, I> ui : workers.values()) {
            if (ui == null) {
                continue;
            }

            Profile[] coreProfiles = new Profile[coreCount];
            for (int i = 0; i < coreCount; ++i) {
                coreProfiles[i] = new Profile();
            }
            LinkedList<I>[] impls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (I impl : impls[coreId]) {
                    String signature = CoreManager.getSignature(coreId, impl.getImplementationId());
                    boolean isPhantomSignature = signature.endsWith(")");
                    if (!isPhantomSignature) {
                        // Phantom signatures are used for external execution wrappers (MPI, OMPSs, etc.)
                        coreGlobalProfiles[coreId].accumulate(ui.getProfile(impl));
                        coreProfiles[coreId].accumulate(ui.getProfile(impl));
                    }
                }
            }
            coreProfilesPerWorker.put(ui.getName(), coreProfiles);
        }

        // Process information in output format
        logger.warn("------- COMPSs Task Execution Summary per Worker ------");
        for (Entry<String, Profile[]> workerInfo : coreProfilesPerWorker.entrySet()) {
            String workerName = workerInfo.getKey();
            Profile[] workerCoreProfiles = workerInfo.getValue();

            logger.warn("--- Summary for COMPSs Worker " + workerName);

            long totalExecutedTasksInWorker = 0;
            for (Entry<String, Integer> entry : CoreManager.getSignaturesToId().entrySet()) {
                String signature = entry.getKey();
                boolean isPhantomSignature = signature.endsWith(")");
                if (!isPhantomSignature) {
                    int coreId = entry.getValue();
                    long executionCount = workerCoreProfiles[coreId].getExecutionCount();
                    totalExecutedTasksInWorker += executionCount;

                    String info = executionCount + " " + signature + " tasks have been executed";
                    logger.warn(info);
                }
            }
            logger.warn("--- Total executed tasks in COMPSs Worker " + workerName + ": " + totalExecutedTasksInWorker);
        }
        logger.warn("-------------------------------------------------------");

        logger.warn("");
        logger.warn("------------ COMPSs Task Execution Summary ------------");
        long totalExecutedTasks = 0;
        for (Entry<String, Integer> entry : CoreManager.getSignaturesToId().entrySet()) {
            String signature = entry.getKey();
            boolean isPhantomSignature = signature.endsWith(")");
            if (!isPhantomSignature) {
                int coreId = entry.getValue();
                long executionCount = coreGlobalProfiles[coreId].getExecutionCount();
                totalExecutedTasks += executionCount;

                String info = executionCount + " " + signature + " tasks have been executed";
                logger.warn(info);
            }
        }
        logger.warn("Total executed tasks: " + totalExecutedTasks);
        logger.warn("-------------------------------------------------------");
    }

    /**
     * Returns the running actions on a given @worker pre-pending the @prefix
     * 
     * @param worker
     * @param prefix
     * @return
     */
    public final String getRunningActionMonitorData(Worker<T, I> worker, String prefix) {
        LOGGER.info("[TaskScheduler] Get running actions monitoring data");
        StringBuilder runningActions = new StringBuilder();

        ResourceScheduler<P, T, I> ui = workers.get(worker);
        if (ui != null) {
            LinkedList<AllocatableAction<P, T, I>> hostedActions = ui.getHostedActions();
            for (AllocatableAction<P, T, I> action : hostedActions) {
                runningActions.append(prefix);
                runningActions.append("<Action>").append(action.toString()).append("</Action>");
                runningActions.append("\n");
            }
        } else {
            LOGGER.info("[TaskScheduler] Worker is not in the list");

        }
        return runningActions.toString();
    }

    /**
     * Returns the coreElement information with the given @prefix
     * 
     * @param prefix
     * @return
     */
    public final String getCoresMonitoringData(String prefix) {
        LOGGER.info("[TaskScheduler] Get cores monitoring data");
        // Create size structure for profiles
        int coreCount = CoreManager.getCoreCount();
        Profile[][] implementationsProfile = new Profile[coreCount][];
        for (int i = 0; i < coreCount; ++i) {
            int implsCount = CoreManager.getNumberCoreImplementations(i);
            implementationsProfile[i] = new Profile[implsCount];
            for (int j = 0; j < implsCount; ++j) {
                implementationsProfile[i][j] = new Profile();
            }
        }

        // Retrieve information from workers
        for (ResourceScheduler<P, T, I> ui : workers.values()) {
            if (ui == null) {
                continue;
            }
            LinkedList<I>[] runningCoreImpls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (I impl : runningCoreImpls[coreId]) {
                    int implId = impl.getImplementationId();
                    implementationsProfile[coreId][implId].accumulate(ui.getProfile(impl));
                }
            }
        }

        // Construct information string
        StringBuilder coresInfo = new StringBuilder();
        coresInfo.append(prefix).append("<CoresInfo>").append("\n");
        for (int coreId = 0; coreId < implementationsProfile.length; ++coreId) {
            coresInfo.append(prefix).append("\t").append("<Core id=\"").append(coreId).append("\"").append(">").append("\n");
            for (int implId = 0; implId < implementationsProfile[coreId].length; ++implId) {
                String signature = CoreManager.getSignature(coreId, implId);

                coresInfo.append(prefix).append("\t\t").append("<Impl id=\"").append(implId).append("\"").append(">").append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<Signature>").append(signature).append("</Signature>").append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<MeanExecutionTime>")
                        .append(implementationsProfile[coreId][implId].getAverageExecutionTime()).append("</MeanExecutionTime>")
                        .append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<MinExecutionTime>")
                        .append(implementationsProfile[coreId][implId].getMinExecutionTime()).append("</MinExecutionTime>").append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<MaxExecutionTime>")
                        .append(implementationsProfile[coreId][implId].getMaxExecutionTime()).append("</MaxExecutionTime>").append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<ExecutedCount>")
                        .append(implementationsProfile[coreId][implId].getExecutionCount()).append("</ExecutedCount>").append("\n");
                coresInfo.append(prefix).append("\t\t").append("</Impl>").append("\n");

            }
            coresInfo.append(prefix).append("\t").append("</Core>").append("\n");
        }
        coresInfo.append(prefix).append("</CoresInfo>").append("\n");

        return coresInfo.toString();
    }

    /**
     * sets the workload state inside the @response parameter
     * 
     * @param response
     */
    public final void getWorkloadState(WorkloadStatus response) {
        LOGGER.info("[TaskScheduler] Get workload state");
        int coreCount = CoreManager.getCoreCount();
        Profile[] coreProfile = new Profile[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            coreProfile[coreId] = new Profile();
        }

        for (ResourceScheduler<P, T, I> ui : workers.values()) {
            if (ui == null) {
                continue;
            }
            LinkedList<I>[] impls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (I impl : impls[coreId]) {
                    coreProfile[coreId].accumulate(ui.getProfile(impl));
                }
            }

            LinkedList<AllocatableAction<P, T, I>> runningActions = ui.getHostedActions();
            long now = System.currentTimeMillis();
            for (AllocatableAction<P, T, I> running : runningActions) {
                if (running.getImplementations().length > 0) {
                    Integer coreId = running.getImplementations()[0].getCoreId();
                    // CoreId can be null for Actions that are not tasks
                    if (coreId != null) {
                        response.registerRunning(coreId, now - running.getStartTime());
                    }
                }
            }
        }

        for (int coreId = 0; coreId < coreCount; coreId++) {
            response.registerNoResources(coreId, blockedActions.getActionCounts()[coreId]);
            response.registerReady(coreId, readyCounts[coreId]);
            response.registerTimes(coreId, coreProfile[coreId].getMinExecutionTime(), coreProfile[coreId].getAverageExecutionTime(),
                    coreProfile[coreId].getMaxExecutionTime());
        }
    }

    public LinkedList<AllocatableAction<P, T, I>> getUnassignedActions() {
        // Not unassigned actions by default must be overwritten by schedulers
        return new LinkedList<>();
    }

}
