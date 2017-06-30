package integratedtoolkit.components.impl;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.ActionNotFoundException;
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
import integratedtoolkit.scheduler.types.WorkloadState;
import integratedtoolkit.scheduler.types.allocatableactions.ReduceWorkerAction;
import integratedtoolkit.scheduler.types.allocatableactions.StartWorkerAction;
import integratedtoolkit.scheduler.types.allocatableactions.StopWorkerAction;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.types.resources.updates.ResourceUpdate;
import integratedtoolkit.util.ActionSet;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;
import integratedtoolkit.util.ResourceManager;
import integratedtoolkit.util.ResourceOptimizer;
import integratedtoolkit.util.Tracer;

import java.util.Collection;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;

public class TaskScheduler {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    // Reference to action orchestrator (Task Dispatcher)
    private ActionOrchestrator orchestrator;

    // Map of available workers and its resource schedulers
    private final WorkersMap workers;

    // List of blocked actions
    private final ActionSet blockedActions;

    // Number of ready tasks for each coreId
    private int[] readyCounts;

    private final ResourceOptimizer ro;

    // Profiles from resources that have already been turned off
    private Profile[][] offVMsProfiles;

    /**
     * Construct a new Task Scheduler
     *
     */
    public TaskScheduler() {
        this.workers = new WorkersMap();
        this.blockedActions = new ActionSet();
        int coreCount = CoreManager.getCoreCount();
        this.readyCounts = new int[coreCount];
        this.offVMsProfiles = new Profile[coreCount][];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            int implCount = CoreManager.getNumberCoreImplementations(coreId);
            Profile[] implProfiles = new Profile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                implProfiles[implId] = generateProfile();
            }
            offVMsProfiles[coreId] = implProfiles;
        }
        // Start ResourceOptimizer
        ro = generateResourceOptimizer();
        ro.start();
    }

    /**
     * Assigns the action orchestrator to this scheduler
     *
     * @param orchestrator
     */
    public final void setOrchestrator(ActionOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
    }

    /**
     * Returns the action orchestator assigned to this scheduler
     *
     * @return
     */
    public final ActionOrchestrator getOrchestrator() {
        return this.orchestrator;
    }

    /**
     * Shutdown the Task Scheduler
     *
     */
    public void shutdown() {
        LOGGER.debug("Profile is: " + this.toJSONObject());
        // Stop Resource Optimizer
        ro.shutdown();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** TASK SCHEDULER STRUCTURES GENERATORS **************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    public Profile generateProfile() {
        return new Profile();
    }

    /**
     * Generates a ResourceScheduler for the worker @w
     *
     * @param <T>
     * @param w
     * @param json
     * @return
     */
    public <T extends WorkerResourceDescription> ResourceScheduler<T> generateSchedulerForResource(Worker<T> w, JSONObject json) {
        // LOGGER.info("[TaskScheduler] Generate scheduler for resource " + w.getName());
        return new ResourceScheduler<>(w, json);
    }

    /**
     * Generates an empty Scheduling Information
     *
     * @param <T>
     * @param rs
     * @return
     */
    public <T extends WorkerResourceDescription> SchedulingInformation generateSchedulingInformation(ResourceScheduler<T> rs) {
        // LOGGER.info("[TaskScheduler] Generate empty scheduling information");
        return new SchedulingInformation(rs);
    }

    /**
     * Generates a action score
     *
     * @param action
     * @return
     */
    public Score generateActionScore(AllocatableAction action) {
        // LOGGER.debug("[TaskScheduler] Generate priority action score");
        return new Score(action.getPriority(), 0, 0, 0);
    }

    public ResourceOptimizer generateResourceOptimizer() {
        return new ResourceOptimizer(this);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    /**
     * New Core Elements have been detected; the Task Scheduler needs to be
     * notified to modify any internal structure using that information.
     *
     */
    public final void coreElementsUpdated() {
        LOGGER.info("[TaskScheduler] Update core elements");
        int newCoreCount = CoreManager.getCoreCount();
        int oldCoreCount = offVMsProfiles.length;
        // Update scheduling information
        SchedulingInformation.updateCoreCount(newCoreCount);

        // Update actions
        this.blockedActions.updateCoreCount(newCoreCount);

        int[] readyCounts = new int[newCoreCount];
        System.arraycopy(this.readyCounts, 0, readyCounts, 0, oldCoreCount);
        this.readyCounts = readyCounts;

        Profile[][] offVMsProfiles = new Profile[newCoreCount][];
        int coreId = 0;
        for (; coreId < oldCoreCount; coreId++) {
            int oldImplCount = this.offVMsProfiles[coreId].length;
            int implCount = CoreManager.getNumberCoreImplementations(coreId);
            if (oldImplCount != implCount) {
                Profile[] implProfiles = new Profile[implCount];
                System.arraycopy(this.offVMsProfiles[coreId], 0, implProfiles, 0, oldImplCount);
                for (int implId = oldImplCount; implId < implCount; implId++) {
                    implProfiles[implId] = new Profile();
                }
                offVMsProfiles[coreId] = implProfiles;
            } else {
                offVMsProfiles[coreId] = this.offVMsProfiles[coreId];
            }
        }
        for (; coreId < newCoreCount; coreId++) {
            int implCount = CoreManager.getNumberCoreImplementations(coreId);
            Profile[] implProfiles = new Profile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                implProfiles[implId] = new Profile();
            }
            offVMsProfiles[coreId] = implProfiles;
        }
        this.offVMsProfiles = offVMsProfiles;
        // Update resource schedulers
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : workers.values()) {
            rs.updatedCoreElements(newCoreCount, getJSONForResource(rs.getResource()));
            SchedulingInformation.changesOnWorker(rs);
        }
    }

    /**
     * Increases the ready coreId counter
     *
     * @param action
     */
    protected final void addToReady(AllocatableAction action) {
        LOGGER.debug("[TaskScheduler] Add action " + action + " to ready count");
        Integer coreId = action.getCoreId();
        if (coreId != null) {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.Event.READY_COUNT.getId(), Tracer.Event.READY_COUNT.getType());
            }
            this.readyCounts[coreId]++;
        }
    }

    /**
     * Decreases the ready coreId counter
     *
     * @param action
     */
    protected final void removeFromReady(AllocatableAction action) {
        LOGGER.info("[TaskScheduler] Remove action " + action + " from ready count");
        if (action.getImplementations().length > 0) {
            Integer coreId = action.getImplementations()[0].getCoreId();
            if (coreId != null) {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.Event.READY_COUNT.getType());
                }
                this.readyCounts[coreId]--;
            }
        }
    }

    /**
     * Adds the action to the blocked list
     *
     * @param action
     */
    protected final void addToBlocked(AllocatableAction action) {
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
     * Introduces a new action in the Scheduler system. The method should place
     * the action in a resource hurriedly
     *
     * @param action Action to be scheduled.
     */
    public final void newAllocatableAction(AllocatableAction action) {
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
     * Registers an action as completed and releases all the resource and data
     * dependencies.
     *
     * @param action action that has finished
     */
    @SuppressWarnings("unchecked")
    public final void actionCompleted(AllocatableAction action) {
        LOGGER.info("[TaskScheduler] Action completed " + action);
        // Mark action as finished
        removeFromReady(action);

        ResourceScheduler<WorkerResourceDescription> resource = (ResourceScheduler<WorkerResourceDescription>) action.getAssignedResource();
        LinkedList<AllocatableAction> resourceFree;
        try {
            resourceFree = resource.unscheduleAction(action);
        } catch (ActionNotFoundException ex) {
            // Once the action starts running should cannot be moved from the resource
            resourceFree = new LinkedList<>();
        }

        // Get the data free actions and mark them as ready
        LinkedList<AllocatableAction> dataFreeActions = action.completed();
        Iterator<AllocatableAction> dataFreeIter = dataFreeActions.iterator();
        while (dataFreeIter.hasNext()) {
            AllocatableAction dataFreeAction = dataFreeIter.next();
            addToReady(dataFreeAction);
        }
        // We update the worker load
        workerLoadUpdate(resource);

        // Schedule data free actions
        LinkedList<AllocatableAction> blockedCandidates = new LinkedList<>();
        // Actions can only be scheduled and those that remain blocked must be added to the blockedCandidates list
        // and those that remain unassigned must be added to the unassigned list
        handleDependencyFreeActions(dataFreeActions, resourceFree, blockedCandidates, resource);
        for (AllocatableAction aa : blockedCandidates) {
            removeFromReady(aa);
            addToBlocked(aa);
        }
    }

    /**
     * Registers an error on the action given as a parameter. The action itself
     * processes the error and triggers with any possible solution to re-execute
     * it. This code is executed only on re-schedule (no resubmit)
     *
     * @param action action raising the error
     */
    @SuppressWarnings("unchecked")
    public final void errorOnAction(AllocatableAction action) {
        LOGGER.warn("[TaskScheduler] Error on action " + action);

        LinkedList<AllocatableAction> resourceFree = new LinkedList<>();
        ResourceScheduler<WorkerResourceDescription> resource = (ResourceScheduler<WorkerResourceDescription>) action.getAssignedResource();

        boolean failed = false;
        // Process the action error (removes the assigned resource)
        try {
            action.error();
        } catch (FailedActionException fae) {
            // Action has completely failed
            failed = true;
            LOGGER.warn("[TaskScheduler] Action completely failed " + action);
            removeFromReady(action);

            // Free all the dependent tasks
            for (AllocatableAction failedAction : action.failed()) {
                try {
                    resourceFree.addAll(resource.unscheduleAction(failedAction));
                } catch (ActionNotFoundException anfe) {
                    // Once the action starts running should cannot be moved from the resource
                }
            }
        }

        // We free the current task and get the free actions from the resource
        try {
            resourceFree.addAll(resource.unscheduleAction(action));
        } catch (ActionNotFoundException anfe) {
            // Once the action starts running should cannot be moved from the resource
        }
        workerLoadUpdate(resource);

        if (!failed) {
            // Try to re-schedule the action
            Score actionScore = generateActionScore(action);
            try {
                scheduleAction(action, actionScore);
                tryToLaunch(action);
            } catch (BlockedActionException bae) {
                removeFromReady(action);
                addToBlocked(action);
            }
        }

        LinkedList<AllocatableAction> blockedCandidates = new LinkedList<>();
        this.handleDependencyFreeActions(new LinkedList<>(), resourceFree, blockedCandidates, resource);
        for (AllocatableAction aa : blockedCandidates) {
            removeFromReady(aa);
            addToBlocked(aa);
        }
    }

    protected final void tryToLaunch(AllocatableAction action) {
        try {
            // LOGGER.debug("[TaskScheduler] Trying to launch" + action);
            action.tryToLaunch();
            // LOGGER.debug("[TaskScheduler] Exited from tryToLaunch without exception");
        } catch (InvalidSchedulingException ise) {
            // LOGGER.debug("[TaskScheduler] There was a bad scheduling" + action);
            // Unschedule the task from that resource
            try {
                action.getAssignedResource().unscheduleAction(action);
            } catch (ActionNotFoundException ex1) {
                // Not possible
            }
            Score actionScore = generateActionScore(action);
            // Reschedule it in another constraining resource
            try {
                scheduleAction(action, actionScore);
            } catch (BlockedActionException bae) {
                addToBlocked(action);
            }
        }
    }

    /**
     * Plans the execution of a given action in one of the compatible resources.
     * The solution should be computed hurriedly since it blocks the runtime
     * thread and this initial allocation can be modified by the scheduler later
     * on the execution.
     *
     * @param action Action whose execution has to be allocated
     * @param actionScore
     * @throws integratedtoolkit.scheduler.exceptions.BlockedActionException
     *
     */
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
        LOGGER.debug("[TaskScheduler] Schedule action " + action);
        try {
            action.schedule(actionScore);
        } catch (UnassignedActionException ure) {
            lostAllocatableAction(action);
        }
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
    public <T extends WorkerResourceDescription> void handleDependencyFreeActions(LinkedList<AllocatableAction> dataFreeActions,
            LinkedList<AllocatableAction> resourceFreeActions, LinkedList<AllocatableAction> blockedCandidates,
            ResourceScheduler<T> resource) {
        LOGGER.debug("[TaskScheduler] Treating dependency free actions");
        // All actions should have already been assigned to a resource, no need
        // to change the assignation once they become free of dependencies

        // Try to launch all the data free actions and the resource free actions
        PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
        for (AllocatableAction freeAction : dataFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, fullScore);
            executableActions.add(obj);
        }
        for (AllocatableAction freeAction : resourceFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, fullScore);
            if (!executableActions.contains(obj)) {
                executableActions.add(obj);
            }
        }

        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();
            tryToLaunch(freeAction);
        }
    }

    /**
     * The action is not registered in any data structure
     *
     * @param action
     */
    protected void lostAllocatableAction(AllocatableAction action) {
        StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
        info.append(action.toString());
        ErrorManager.fatal(info.toString());
    }

    /**
     * Notifies to the scheduler that there have been changes in the load of a
     * resource.
     *
     * @param <T>
     * @param resource updated resource
     */
    public <T extends WorkerResourceDescription> void workerLoadUpdate(ResourceScheduler<T> resource) {
        LOGGER.info("[TaskScheduler] Update load on worker " + resource.getName());
        // Resource capabilities had already been taken into account when assigning the actions. No need to change the
        // assignation.
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** RESOURCES MANAGEMENT OPERATIONS *******************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    /**
     *
     * @param <T>
     * @param worker
     * @param rs
     */
    public final <T extends WorkerResourceDescription> void updateWorker(Worker<T> worker, ResourceUpdate<T> rs) {
        ResourceScheduler<T> ui = (ResourceScheduler<T>) workers.get(worker);
        if (ui == null) {
            // Register worker if it's the first time it is useful.
            ui = addWorker(worker, getJSONForResource(worker));
            startWorker(ui);
            workerDetected(ui);
        }

        if (rs.checkCompleted()) {
            completedResourceUpdate(ui, rs);
        } else {
            pendingResourceUpdate(ui, rs);
        }
    }

    /**
     * Registers a new Worker node for the scheduler to use it and creates the
     * corresponding ResourceScheduler
     *
     * @param worker Worker to incorporate
     * @return the ResourceScheduler that will manage the scheduling for the
     * given worker
     */
    private <T extends WorkerResourceDescription> ResourceScheduler<T> addWorker(Worker<T> worker, JSONObject jsonResource) {
        ResourceScheduler<T> ui = generateSchedulerForResource(worker, jsonResource);
        synchronized (workers) {
            workers.put(worker, ui);
        }
        return ui;
    }

    /**
     * Contextualizes the worker by creating a new action StartWorker
     *
     * @param ui ResourceScheduler whose worker is to contextualize.
     */
    private <T extends WorkerResourceDescription> void startWorker(ResourceScheduler<T> ui) {
        StartWorkerAction<T> action = new StartWorkerAction<T>(generateSchedulingInformation(ui), ui, this);
        try {
            action.schedule(ui, (Score) null);
            action.tryToLaunch();
        } catch (BlockedActionException | UnassignedActionException | InvalidSchedulingException e) {
            // Can not be blocked nor unassigned
        }
    }

    /**
     * New worker has been detected; the Task Scheduler is notified to modify
     * any internal structure using that information.
     *
     * @param <T>
     * @param resource new worker
     */
    protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
        // There are no internal structures worker-related. No need to do
        // anything.
    }

    private <T extends WorkerResourceDescription> void pendingResourceUpdate(ResourceScheduler<T> worker, ResourceUpdate<T> modification) {
        switch (modification.getType()) {
            case INCREASE:
                // Can't happen
                break;
            case REDUCE:
                reduceWorkerResources(worker, modification);
                break;
            default:

        }
    }

    private <T extends WorkerResourceDescription> void reduceWorkerResources(ResourceScheduler<T> worker, ResourceUpdate<T> modification) {
        worker.pendingModification(modification);
        SchedulingInformation schedInfo = generateSchedulingInformation(worker);
        ReduceWorkerAction<T> action = new ReduceWorkerAction<T>(schedInfo, worker, this, modification);
        try {
            action.schedule(worker, (Score) null);
            action.tryToLaunch();
        } catch (BlockedActionException | UnassignedActionException | InvalidSchedulingException e) {
            // Can not be blocked nor unassigned
        }
    }

    /**
     *
     * @param <T>
     * @param worker
     * @param modification
     */
    @SuppressWarnings("unchecked")
    public final <T extends WorkerResourceDescription> void completedResourceUpdate(ResourceScheduler<T> worker,
            ResourceUpdate<T> modification) {
        worker.completedModification(modification);
        SchedulingInformation.changesOnWorker((ResourceScheduler<WorkerResourceDescription>) worker);
        switch (modification.getType()) {
            case INCREASE:
                increasedWorkerResources(worker, modification);
                break;
            case REDUCE:
                reducedWorkerResources(worker, modification);
                break;
            default:

        }
    }

    private <T extends WorkerResourceDescription> void increasedWorkerResources(ResourceScheduler<T> worker,
            ResourceUpdate<T> modification) {
        if (worker.getExecutableCores().isEmpty()) {
            // We no longer remove workers with empty executable cores since new core elements
            // can be registered on execution time
        } else {
            // Inspect blocked actions to be freed
            LinkedList<AllocatableAction> compatibleActions = this.blockedActions.removeAllCompatibleActions(worker.getResource());

            // Prioritize them
            PriorityQueue<ObjectValue<AllocatableAction>> sortedCompatibleActions = new PriorityQueue<>();
            for (AllocatableAction action : compatibleActions) {
                ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, generateActionScore(action));
                sortedCompatibleActions.add(obj);
            }
            // Schedule them
            while (!sortedCompatibleActions.isEmpty()) {
                ObjectValue<AllocatableAction> obj = sortedCompatibleActions.poll();
                Score actionScore = obj.getScore();
                AllocatableAction action = obj.getObject();

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
            this.workerLoadUpdate(worker);
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends WorkerResourceDescription> void reducedWorkerResources(ResourceScheduler<T> worker, ResourceUpdate<T> modification) {
        CloudMethodWorker cloudWorker = (CloudMethodWorker) worker.getResource();
        if (!cloudWorker.getDescription().getTypeComposition().isEmpty()) {
            synchronized (workers) {
                workers.remove(((ResourceScheduler<WorkerResourceDescription>) worker).getResource());
                int coreCount = CoreManager.getCoreCount();
                LinkedList<Implementation>[] runningCoreImpls = worker.getExecutableImpls();
                for (int coreId = 0; coreId < coreCount; coreId++) {
                    for (Implementation impl : runningCoreImpls[coreId]) {
                        Profile p = worker.getProfile(impl);
                        if (p != null) {
                            offVMsProfiles[coreId][impl.getImplementationId()].accumulate(p);
                        }
                    }
                }
            }
            this.workerRemoved((ResourceScheduler<WorkerResourceDescription>) worker);

            StopWorkerAction action = new StopWorkerAction(generateSchedulingInformation(worker), worker, this, modification);
            try {
                action.schedule((ResourceScheduler<WorkerResourceDescription>) worker, (Score) null);
                action.tryToLaunch();
            } catch (BlockedActionException | UnassignedActionException | InvalidSchedulingException e) {
                // Can not be blocked nor unassigned
            }
        } else {
            ResourceManager.terminateResource(cloudWorker, (CloudMethodResourceDescription) modification.getModification());
        }
    }

    /**
     * One worker has been removed from the pool; the Task Scheduler is notified
     * to modify any internal structure using that information.
     *
     * @param <T>
     * @param resource removed worker
     */
    protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
        LOGGER.info("[TaskScheduler] Remove worker " + resource.getName());
        // There are no internal structures worker-related. No need to do anything.
        PriorityQueue<AllocatableAction> blockedOnResource = resource.getBlockedActions();

        for (AllocatableAction action : blockedOnResource) {
            try {
                resource.unscheduleAction(action);
            } catch (ActionNotFoundException ex) {
                // Task was already moved from the worker. Do nothing!
                continue;
            }

            Score actionScore = generateActionScore(action);
            try {
                scheduleAction(action, actionScore);
                tryToLaunch(action);
            } catch (BlockedActionException bae) {
                if (!action.hasDataPredecessors()) {
                    removeFromReady(action);
                }
                addToBlocked(action);
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
     * Returns the ResourceSchedulers assigned to all available workers
     *
     * @return
     */
    public final Collection<ResourceScheduler<? extends WorkerResourceDescription>> getWorkers() {
        LOGGER.info("[TaskScheduler] Get all worker resource schedulers");
        return this.workers.values();
    }

    /**
     * Returns a list of the blocked actions
     *
     * @return
     */
    public final LinkedList<AllocatableAction> getBlockedActions() {
        LOGGER.info("[TaskScheduler] Get Blocked Actions");
        // Parameter null to get all blocked actions
        return this.blockedActions.getActions(null);
    }

    /**
     * Returns a list with the hosted actions on a given worker
     *
     * @param <T>
     * @param worker
     * @return
     */
    public final <T extends WorkerResourceDescription> LinkedList<AllocatableAction> getHostedActions(Worker<T> worker) {
        LOGGER.info("[TaskScheduler] Get Hosted actions on worker " + worker.getName());
        ResourceScheduler<T> ui = workers.get(worker);
        if (ui != null) {
            return ui.getHostedActions();
        } else {
            return new LinkedList<>();
        }
    }

    /**
     * Returns the blocked actions assigned to a given resource
     *
     * @param worker
     * @return
     */
    public final <T extends WorkerResourceDescription> PriorityQueue<AllocatableAction> getBlockedActionsOnResource(Worker<T> worker) {
        LOGGER.info("[TaskScheduler] Get Blocked actions on worker " + worker.getName());
        ResourceScheduler<T> ui = workers.get(worker);
        if (ui != null) {
            return ui.getBlockedActions();
        } else {
            return new PriorityQueue<>();
        }
    }

    /**
     * Returns the actions with no resources assigned
     *
     * @return
     */
    public LinkedList<AllocatableAction> getUnassignedActions() {
        return new LinkedList<>();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* RESOURCE OPTIMIZER INFORMATION ***************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    /**
     * returns the current workload state
     *
     * @return
     */
    public final WorkloadState getWorkload() {
        WorkloadState ws = generateWorkloadState();
        updateWorkloadState(ws);
        return ws;
    }

    protected WorkloadState generateWorkloadState() {
        return new WorkloadState();
    }

    protected void updateWorkloadState(WorkloadState state) {
        LOGGER.info("[TaskScheduler] Get workload state");
        int coreCount = CoreManager.getCoreCount();
        Profile[] coreProfile = new Profile[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            coreProfile[coreId] = new Profile();
        }

        for (ResourceScheduler<? extends WorkerResourceDescription> ui : workers.values()) {
            if (ui == null) {
                continue;
            }
            LinkedList<Implementation>[] impls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (Implementation impl : impls[coreId]) {
                    coreProfile[coreId].accumulate(ui.getProfile(impl));
                }
            }

            LinkedList<AllocatableAction> runningActions = ui.getHostedActions();
            long now = System.currentTimeMillis();
            for (AllocatableAction running : runningActions) {
                if (running.getImplementations().length > 0) {
                    Integer coreId = running.getImplementations()[0].getCoreId();
                    // CoreId can be null for Actions that are not tasks
                    if (coreId != null) {
                        state.registerRunning(coreId, now - running.getStartTime());
                    }
                }
            }
        }

        for (int coreId = 0; coreId < coreCount; coreId++) {
            state.registerNoResources(coreId, blockedActions.getActionCounts()[coreId]);
            state.registerReady(coreId, readyCounts[coreId]);
            state.registerTimes(coreId, coreProfile[coreId].getMinExecutionTime(), coreProfile[coreId].getAverageExecutionTime(),
                    coreProfile[coreId].getMaxExecutionTime());
        }
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
        for (ResourceScheduler<? extends WorkerResourceDescription> ui : workers.values()) {
            if (ui == null) {
                continue;
            }

            Profile[] coreProfiles = new Profile[coreCount];
            for (int i = 0; i < coreCount; ++i) {
                coreProfiles[i] = new Profile();
            }
            LinkedList<Implementation>[] impls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (Implementation impl : impls[coreId]) {
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
     * @param <T>
     * @param worker
     * @param prefix
     * @return
     */
    public final <T extends WorkerResourceDescription> String getRunningActionMonitorData(Worker<T> worker, String prefix) {
        LOGGER.info("[TaskScheduler] Get running actions monitoring data");
        StringBuilder runningActions = new StringBuilder();

        ResourceScheduler<T> ui = workers.get(worker);
        if (ui != null) {
            LinkedList<AllocatableAction> hostedActions = ui.getHostedActions();
            for (AllocatableAction action : hostedActions) {
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

        // Collect information from turned off VMs
        for (int i = 0; i < coreCount; ++i) {
            int implsCount = CoreManager.getNumberCoreImplementations(i);
            for (int j = 0; j < implsCount; ++j) {
                implementationsProfile[i][j].accumulate(offVMsProfiles[i][j]);
            }
        }
        // Retrieve information from workers
        for (ResourceScheduler<? extends WorkerResourceDescription> ui : workers.values()) {
            if (ui == null) {
                continue;
            }
            LinkedList<Implementation>[] runningCoreImpls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (Implementation impl : runningCoreImpls[coreId]) {
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
                // Get method's signature
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

    public JSONObject toJSONObject() {
        JSONObject json = new JSONObject();
        JSONObject resources = new JSONObject();
        JSONObject implementations = new JSONObject();
        json.put("resources", resources);
        json.put("implementation", implementations);
        int coreCount = this.offVMsProfiles.length;

        Profile[][] p = new Profile[coreCount][];
        // Aggregate offVMs as initial Profile values
        for (int coreId = 0; coreId < coreCount; coreId++) {
            int implCount = this.offVMsProfiles[coreId].length;
            p[coreId] = new Profile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                p[coreId][implId] = offVMsProfiles[coreId][implId].copy();
            }
        }
        // Add workers to JSON result and accumulate on initial global profile
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : workers.values()) {
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (int implId = 0; implId < CoreManager.getNumberCoreImplementations(coreId); implId++) {
                    p[coreId][implId].accumulate(rs.getProfile(coreId, implId));
                }
            }
            resources.put(rs.getName(), rs.toJSONObject());
        }

        // Add profiles to JSON result
        for (int coreId = 0; coreId < coreCount; coreId++) {
            int implCount = this.offVMsProfiles[coreId].length;
            for (int implId = 0; implId < implCount; implId++) {
                implementations.put(CoreManager.getSignature(coreId, implId), p[coreId][implId].toJSONObject());
            }
        }
        return json;
    }

    private <T extends WorkerResourceDescription> JSONObject getJSONForResource(Worker<T> resource) {
        JSONObject fileJSON = new JSONObject("{}");
        try {
            JSONObject resourcesJSON = fileJSON.getJSONObject("resources");
            return resourcesJSON.getJSONObject(resource.getName());
        } catch (JSONException je) {
            // Do nothing it will return null
        }
        return null;
    }

    private class WorkersMap {

        private final HashMap<Worker<? extends WorkerResourceDescription>, ResourceScheduler<? extends WorkerResourceDescription>> map = new HashMap<>();

        public <T extends WorkerResourceDescription> void put(Worker<T> w, ResourceScheduler<T> rs) {
            map.put(w, rs);
        }

        @SuppressWarnings("unchecked")
        public <T extends WorkerResourceDescription> ResourceScheduler<T> get(Worker<T> w) {
            return (ResourceScheduler<T>) map.get(w);
        }

        private <T extends WorkerResourceDescription> void remove(Worker<T> resource) {
            map.remove(resource);
        }

        private Collection<ResourceScheduler<? extends WorkerResourceDescription>> values() {
            return map.values();
        }
    }
}
