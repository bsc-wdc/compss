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
package es.bsc.compss.components.impl;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.ActionNotFoundException;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.FailedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.scheduler.types.WorkloadState;
import es.bsc.compss.scheduler.types.allocatableactions.BusyWorkerAction;
import es.bsc.compss.scheduler.types.allocatableactions.ReduceWorkerAction;
import es.bsc.compss.scheduler.types.allocatableactions.StartWorkerAction;
import es.bsc.compss.scheduler.types.allocatableactions.StopWorkerAction;
import es.bsc.compss.types.CloudProvider;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.DynamicMethodWorker;
import es.bsc.compss.types.resources.Resource;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.resources.description.CloudInstanceTypeDescription;
import es.bsc.compss.types.resources.updates.IdleResources;
import es.bsc.compss.types.resources.updates.PerformedReduction;
import es.bsc.compss.types.resources.updates.ResourceUpdate;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ActionSet;
import es.bsc.compss.util.CoreManager;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ExternalAdaptationManager;
import es.bsc.compss.util.JSONStateManager;
import es.bsc.compss.util.ResourceOptimizer;
import es.bsc.compss.util.SchedulingOptimizer;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;

import java.lang.reflect.Constructor;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;


/**
 * Basic Task scheduler implementation that only takes care of data dependencies.
 */
public class TaskScheduler {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    // Data Provenance logger
    private static final Logger DP_LOGGER = LogManager.getLogger(Loggers.DATA_PROVENANCE);
    private static final boolean DP_ENABLED = Boolean.parseBoolean(System.getProperty(COMPSsConstants.DATA_PROVENANCE));

    // Reference to action orchestrator (Task Dispatcher)
    private final ActionOrchestrator orchestrator;

    // Map of available workers and its resource schedulers
    protected final WorkersMap workers;

    // List of blocked actions
    private final ActionSet blockedActions;

    // Number of ready tasks for each coreId
    private int[] readyCounts;

    private final ResourceOptimizer ro;
    private final SchedulingOptimizer<TaskScheduler> so;
    private final boolean externalAdaptation;
    private final ExternalAdaptationManager extAdaptationManager;
    protected final JSONStateManager jsm;
    private Map<Integer, LinkedList<ResourceScheduler<? extends WorkerResourceDescription>>> distributedTasksResources;

    // Profiles from resources that have already been turned off
    private Profile[][] offVMsProfiles;

    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    /**
     * Constructs the scheduler.
     *
     * @param schedFQN Fully Qualified name of the class implementing the scheduler
     * @param orchestrator Element orchestrating the execution of actions
     * @return a TaskScheduler of the specified class
     * @throws ClassNotFoundException the schedFQN has not been loaded on the classpath
     * @throws IllegalAccessException if this Constructor object is enforcing Java language access control and the
     *     underlying constructor is inaccessible.
     * @throws IllegalArgumentException if the number of actual and formal parameters differ; if an unwrapping
     *     conversion for primitive arguments fails; or if, after possible unwrapping, a parameter value cannot be
     *     converted to the corresponding formal parameter type by a method invocation conversion; if this constructor
     *     pertains to an enum type.
     * @throws InstantiationException if the class that declares the underlying constructor represents an abstract
     *     class.
     * @throws InvocationTargetException if the underlying constructor throws an exception.
     * @throws ExceptionInInitializerError if the initialization provoked by this method fails.
     */
    public static TaskScheduler constructScheduler(String schedFQN, ActionOrchestrator orchestrator)
        throws ClassNotFoundException, InvocationTargetException, InstantiationException, IllegalAccessException,
        IllegalArgumentException, ExceptionInInitializerError {
        Class<?> schedClass = Class.forName(schedFQN);
        Constructor<?> schedCnstr = schedClass.getDeclaredConstructors()[0];
        TaskScheduler scheduler = (TaskScheduler) schedCnstr.newInstance(orchestrator);
        if (DEBUG) {
            LOGGER.debug("Loaded scheduler " + scheduler);
        }
        return scheduler;
    }

    /**
     * Constructs a new Task Scheduler.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public TaskScheduler(ActionOrchestrator orchestrator) {
        this.orchestrator = orchestrator;
        String enableAdaptStr = System.getProperty(COMPSsConstants.EXTERNAL_ADAPTATION);
        if (enableAdaptStr != null && !enableAdaptStr.isEmpty()) {
            this.externalAdaptation = Boolean.parseBoolean(enableAdaptStr);
        } else {
            this.externalAdaptation = false;
        }

        this.workers = new WorkersMap();
        this.jsm = new JSONStateManager();
        this.blockedActions = new ActionSet();
        int coreCount = CoreManager.getCoreCount();
        this.readyCounts = new int[coreCount];
        this.offVMsProfiles = new Profile[coreCount][];
        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            int implCount = ce.getImplementationsCount();
            Profile[] implProfiles = new Profile[implCount];
            for (Implementation impl : ce.getImplementations()) {
                int implId = impl.getImplementationId();
                implProfiles[implId] = generateProfile(null);
            }
            this.offVMsProfiles[coreId] = implProfiles;
        }
        this.distributedTasksResources = new HashMap<>();

        // Start SchedulingOptimizer
        this.so = generateSchedulingOptimizer();
        this.so.start();

        // Start ResourceOptimizer
        this.ro = generateResourceOptimizer();
        this.ro.start();

        // Start external adaptation
        if (this.externalAdaptation) {
            this.extAdaptationManager = generateExternalAdaptationManager();
            this.extAdaptationManager.start();
        } else {
            this.extAdaptationManager = null;
        }
    }

    /**
     * Returns the Action Orchestrator assigned to this scheduler.
     *
     * @return The Action Orchestrator assigned to this scheduler.
     */
    public final ActionOrchestrator getOrchestrator() {
        return this.orchestrator;
    }

    /**
     * Shutdown the Task Scheduler.
     */
    public final void shutdown() {
        customSchedulerShutdown();
        // Stop Resource Optimizer
        this.ro.shutdown();
        this.so.shutdown();
        if (this.externalAdaptation) {
            this.extAdaptationManager.shutdown();
        }
        try {
            updateState();
            this.jsm.write();
            if (DP_ENABLED) {
                // Write application execution metrics to dataprovenance.log file
                this.jsm.writeDataProvenance(DP_LOGGER);
            }
        } catch (Exception e) {
            LOGGER.error(e);
        }
    }

    protected void customSchedulerShutdown() {
        // Do nothing. Overriden if necessary by Task Scheduler extension.
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** TASK SCHEDULER STRUCTURES GENERATORS **************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Generates the Resource Optimizer for the scheduler.
     *
     * @return new instance of the specific Resource Optimizer for the Task Scheduler.
     */
    public ResourceOptimizer generateResourceOptimizer() {
        return new ResourceOptimizer(this);
    }

    /**
     * Generates the externalAdaptationManager .
     *
     * @return new instance of the externalApatationManager for the Task Scheduler.
     */
    public ExternalAdaptationManager generateExternalAdaptationManager() {
        return new ExternalAdaptationManager();
    }

    /**
     * Generates the Scheduling Optimizer for the scheduler.
     *
     * @return new instance of the specific Scheduling Optimizer for the Task Scheduler.
     */
    @SuppressWarnings("unchecked")
    public <T extends TaskScheduler> SchedulingOptimizer<T> generateSchedulingOptimizer() {
        return (SchedulingOptimizer<T>) new SchedulingOptimizer<>(this);
    }

    /**
     * Generates a profile for an action.
     *
     * @param json JSON information.
     * @return Profile.
     */
    public Profile generateProfile(JSONObject json) {
        return new Profile(json);
    }

    /**
     * Generates a ResourceScheduler for the worker {@code w}.
     *
     * @param <T> WorkerResourceDescription.
     * @param w Associated worker.
     * @param defaultResources JSON description of the resources.
     * @param defaultImplementations JSON description of the implementations.
     * @return A ResourceScheduler.
     */
    public <T extends WorkerResourceDescription> ResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
        JSONObject defaultResources, JSONObject defaultImplementations) {
        return new ResourceScheduler<>(w, defaultResources, defaultImplementations);
    }

    /**
     * Generates an empty Scheduling Information.
     *
     * @param <T> WorkerResourceDescription.
     * @param rs Associated ResourceScheduler.
     * @param params List of parameters of the task.
     * @param coreId Core element id.
     * @return An empty Scheduling Information.
     */
    public <T extends WorkerResourceDescription> SchedulingInformation generateSchedulingInformation(
        ResourceScheduler<T> rs, List<? extends Parameter> params, Integer coreId) {
        return new SchedulingInformation(rs);
    }

    /**
     * Generates a action score.
     *
     * @param action AllocatableAction.
     * @return An scheduling action score.
     */
    public Score generateActionScore(AllocatableAction action) {
        return new Score(action.getPriority(), action.getGroupPriority(), 0, 0, 0);
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
            CoreElement ce = CoreManager.getCore(coreId);
            int implCount = ce.getImplementationsCount();
            if (oldImplCount != implCount) {
                Profile[] implProfiles = new Profile[implCount];
                System.arraycopy(this.offVMsProfiles[coreId], 0, implProfiles, 0, oldImplCount);
                for (int implId = oldImplCount; implId < implCount; implId++) {
                    implProfiles[implId] = generateProfile(null);
                }
                offVMsProfiles[coreId] = implProfiles;
            } else {
                offVMsProfiles[coreId] = this.offVMsProfiles[coreId];
            }
        }
        for (; coreId < newCoreCount; coreId++) {
            CoreElement ce = CoreManager.getCore(coreId);
            int implCount = ce.getImplementationsCount();
            Profile[] implProfiles = new Profile[implCount];
            for (int implId = 0; implId < implCount; implId++) {
                implProfiles[implId] = generateProfile(null);
            }
            offVMsProfiles[coreId] = implProfiles;
        }
        this.offVMsProfiles = offVMsProfiles;
        // Update resource schedulers
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : workers.values()) {
            rs.updatedCoreElements(newCoreCount, jsm.getJSONForResource(rs.getResource()));
            SchedulingInformation.changesOnWorker(rs);
        }
        ro.coreElementsUpdated();
        customCoreElementsUpdated();
    }

    public void customCoreElementsUpdated() {
        // Do nothing. Overriden if necessary by Task Scheduler extension.
    }

    /**
     * Increases the ready coreId counter.
     *
     * @param action AllocatableAction.
     */
    private void addToReady(AllocatableAction action) {
        LOGGER.debug("[TaskScheduler] Add action " + action + " to ready count");
        Integer coreId = action.getCoreId();
        if (coreId != null) {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(TraceEvent.READY_COUNT);
            }
            this.readyCounts[coreId]++;
        }
    }

    /**
     * Decreases the ready coreId counter.
     *
     * @param action AllocatableAction.
     */
    private void removeFromReady(AllocatableAction action) {
        LOGGER.info("[TaskScheduler] Remove action " + action + " from ready count");
        if (action.getImplementations() != null) {
            if (action.getImplementations().length > 0) {
                Integer coreId = action.getImplementations()[0].getCoreId();
                if (coreId != null) {
                    if (Tracer.isActivated()) {
                        Tracer.emitEventEnd(TraceEvent.READY_COUNT);
                    }
                    this.readyCounts[coreId]--;
                }
            }
        }
    }

    /**
     * Adds the action to the blocked list.
     *
     * @param action Blocked AllocatableAction.
     */
    public final void addToBlocked(AllocatableAction action) {
        LOGGER.warn("[TaskScheduler] Blocked Action: " + action);
        this.blockedActions.addAction(action);
        if (!action.hasDataPredecessors() && !action.hasStreamProducers()) {
            removeFromReady(action);
        }
    }

    /**
     * Removes from the blocked list all the actions compatible with the resource.
     *
     * @param resource resource that could allocate the blocked tasks
     * @return list all the actions compatible with the resource.
     */
    protected final List<AllocatableAction> removeCompatibleFromBlocked(Worker resource) {
        List<AllocatableAction> unblockedActions = this.blockedActions.removeAllCompatibleActions(resource);

        for (AllocatableAction action : unblockedActions) {
            if (!action.hasDataPredecessors() && !action.hasStreamProducers()) {
                addToReady(action);
            }
        }
        return unblockedActions;
    }
    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Introduces a new action in the Scheduler system. The method should place the action in a resource hurriedly.
     *
     * @param action Action to be scheduled.
     */
    public final void newAllocatableAction(AllocatableAction action) {
        LOGGER.info("[TaskScheduler] Registering new AllocatableAction " + action);
        if (!action.hasDataPredecessors() && !action.hasStreamProducers()) {
            addToReady(action);
        }

        Score actionScore = generateActionScore(action);
        try {
            scheduleAction(action, actionScore);
            tryToLaunch(action);
        } catch (BlockedActionException bae) {
            addToBlocked(action);
        }
    }

    /**
     * Registers an action as canceled.
     *
     * @param action Action to cancel
     */
    public final void cancelAllocatableAction(AllocatableAction action) {
        action.cancel();
    }

    /**
     * Registers an action as running and releases its stream dependencies.
     *
     * @param action Running AllocatableAction.
     */
    public final void actionRunning(AllocatableAction action) {
        LOGGER.info("[TaskScheduler] Action running " + action);

        // Check if stream consumers are free
        List<AllocatableAction> freeActions = action.executionStarted();
        if (freeActions != null && freeActions.size() > 0) {
            for (AllocatableAction fAction : freeActions) {
                addToReady(fAction);
            }
            handleDependencyFreeActionsAndBlock(freeActions, new LinkedList<>(), action.getAssignedResource());
        }
    }

    private List<AllocatableAction> actionFinished(AllocatableAction action) {
        // Mark action as finished
        removeFromReady(action);

        ResourceScheduler<WorkerResourceDescription> resource;
        resource = (ResourceScheduler<WorkerResourceDescription>) action.getAssignedResource();
        List<AllocatableAction> resourceFree;
        try {
            resourceFree = action.unschedule();
        } catch (UnassignedActionException | ActionNotFoundException e) {
            resourceFree = new LinkedList<>();
        }

        // We update the worker load
        workerLoadUpdate(resource);
        return resourceFree;
    }

    private <T extends WorkerResourceDescription> void handleReadinessAndDependencyFreeActions(
        List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions,
        ResourceScheduler<T> resource) {

        for (AllocatableAction dataFreeAction : dataFreeActions) {
            addToReady(dataFreeAction);
        }
        handleDependencyFreeActionsAndBlock(dataFreeActions, resourceFreeActions, resource);
    }

    private <T extends WorkerResourceDescription> void handleDependencyFreeActionsAndBlock(
        List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions,
        ResourceScheduler<T> resource) {
        // Schedule data free actions
        List<AllocatableAction> blockedCandidates = new LinkedList<>();
        // Actions can only be scheduled and those that remain blocked must be added to the blockedCandidates list
        // and those that remain unassigned must be added to the unassigned list

        handleDependencyFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
        for (AllocatableAction aa : blockedCandidates) {
            addToBlocked(aa);
        }
    }

    /**
     * Registers an action as completed and releases all the resource and data dependencies.
     *
     * @param action Action that has finished
     */
    public final void actionCompleted(AllocatableAction action) {
        LOGGER.info("[TaskScheduler] Action completed " + action);
        ResourceScheduler<? extends WorkerResourceDescription> resource = action.getAssignedResource();
        List<AllocatableAction> resourceFreeActions = actionFinished(action);

        // Get the data free actions and mark them as ready
        List<AllocatableAction> dataFreeActions = action.completed();

        handleReadinessAndDependencyFreeActions(dataFreeActions, resourceFreeActions, resource);
    }

    /**
     * Registers a COMPSs exception to the group of the task.
     *
     * @param action Action raising the error.
     */
    public final void exceptionOnAction(AllocatableAction action, COMPSsException e) {
        LOGGER.info("[TaskScheduler] Exception on action " + action);
        ResourceScheduler<? extends WorkerResourceDescription> resource = action.getAssignedResource();
        List<AllocatableAction> resourceFreeActions = actionFinished(action);

        // Get the data free actions and mark them as ready
        List<AllocatableAction> dataFreeActions = action.exception(e);

        handleReadinessAndDependencyFreeActions(dataFreeActions, resourceFreeActions, resource);
    }

    /**
     * Registers an error on the action given as a parameter. The action itself processes the error and triggers with
     * any possible solution to re-execute it. This code is executed only on re-schedule (no resubmit).
     *
     * @param action Action raising the error.
     */
    @SuppressWarnings("unchecked")
    public final void errorOnAction(AllocatableAction action) {
        LOGGER.warn("[TaskScheduler] Error on action " + action);

        List<AllocatableAction> resourceFree = new LinkedList<>();

        List<AllocatableAction> dataFreeActions = new LinkedList<>();

        ResourceScheduler<WorkerResourceDescription> resource;
        resource = (ResourceScheduler<WorkerResourceDescription>) action.getAssignedResource();
        boolean failed = false;

        // Process the action error (removes the assigned resource)
        try {
            if (action.isCancelling()) {
                LOGGER.debug("[TaskScheduler] Action " + action + "is being cancelled. Invoking cancel...");
                action.cancel();
            } else {
                action.error();
            }
        } catch (FailedActionException fae) {
            // Action has completely failed
            failed = true;
            removeFromReady(action);
            if (action.getOnFailure() != OnFailure.IGNORE) {
                // Free all the dependent tasks
                for (AllocatableAction failedAction : action.failed()) {
                    try {
                        resourceFree.addAll(failedAction.unschedule());
                    } catch (ActionNotFoundException | UnassignedActionException anfe) {
                        // Once the action starts running should cannot be moved from the resource
                    }
                }
            } else {
                // Get the data free actions and mark them as ready
                dataFreeActions = action.ignoredFailure();
                for (AllocatableAction dataFreeAction : dataFreeActions) {
                    addToReady(dataFreeAction);
                }
            }
        }

        // We free the current task and get the free actions from the resource
        try {
            resourceFree.addAll(action.unschedule());
        } catch (ActionNotFoundException | UnassignedActionException anfe) {
            // Once the action starts running should cannot be moved from the resource
        }

        workerLoadUpdate(resource);

        if (action.getOnFailure() == OnFailure.RETRY && !failed) {
            if (DEBUG) {
                LOGGER.debug("Adding action " + action + " to data Free actions.");
            }
            dataFreeActions.add(action);

        }

        if (action.getOnFailure() != OnFailure.CANCEL_SUCCESSORS && !action.isCancelled()) {
            handleDependencyFreeActionsAndBlock(dataFreeActions, resourceFree, resource);
        }
    }

    protected final void tryToLaunch(AllocatableAction action) {
        try {
            LOGGER.debug("[TaskScheduler] Trying to launch " + action);
            action.tryToLaunch();
        } catch (InvalidSchedulingException ise) {
            // Unschedule the task from that resource
            List<AllocatableAction> resourceFree = new LinkedList<>();
            try {
                resourceFree.addAll(action.unschedule());
            } catch (ActionNotFoundException | UnassignedActionException ex1) {
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
     * Plans the execution of a given action in one of the compatible resources. The solution should be computed
     * hurriedly since it blocks the runtime thread and this initial allocation can be modified by the scheduler later
     * on the execution.
     *
     * @param action Action whose execution has to be allocated.
     * @param actionScore Scheduling action score.
     * @throws BlockedActionException When the action gets blocked.
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
     * Notifies to the scheduler that some actions have become free of data dependencies or resource dependencies.
     *
     * @param <T> WorkerResourceDescription.
     * @param dataFreeActions IN, list of actions free of data dependencies.
     * @param resourceFreeActions IN, list of actions free of resource dependencies.
     * @param blockedCandidates OUT, list of blocked candidates.
     * @param resource Resource where the previous task was executed.
     */
    protected <T extends WorkerResourceDescription> void handleDependencyFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
        LOGGER.debug("[TaskScheduler] Handling dependency free actions on resource " + resource.getName());
        // All actions should have already been assigned to a resource, no need
        // to change the assignation once they become free of dependencies
        // Try to launch all the data free actions and the resource free actions
        PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
        for (AllocatableAction freeAction : dataFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, actionScore);
            executableActions.add(obj);
        }
        for (AllocatableAction freeAction : resourceFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, actionScore);
            if (!executableActions.contains(obj)) {
                executableActions.add(obj);
            }
        }

        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            Score score = obj.getScore();
            AllocatableAction freeAction = obj.getObject();
            if (freeAction.getAssignedResource() != null) {
                if (freeAction.getAssignedImplementation() != null) {
                    tryToLaunch(freeAction);
                } else {
                    try {
                        freeAction.schedule(freeAction.getAssignedResource(), score);
                    } catch (UnassignedActionException uae) {
                        if (freeAction.getCompatibleWorkers().isEmpty()) {
                            this.addToBlocked(freeAction);
                        } else {
                            this.lostAllocatableAction(freeAction);
                        }
                    }
                }
            } else {
                // Task has no resource.
                List<ResourceScheduler<? extends WorkerResourceDescription>> compatibleWorkers =
                    freeAction.getCompatibleWorkers();
                if (compatibleWorkers.isEmpty()) {
                    // Because it is blocked
                    this.addToBlocked(freeAction);
                } else {
                    List<ResourceScheduler<? extends WorkerResourceDescription>> hostWorkers =
                        freeAction.getExecutingResources();
                    if (hostWorkers.containsAll(compatibleWorkers)) {
                        this.addToBlocked(freeAction);
                    } else {
                        this.lostAllocatableAction(freeAction);
                    }
                }
            }
        }
    }

    /**
     * The action is not registered in any data structure.
     *
     * @param action Lost AllocatableAction.
     */
    protected void lostAllocatableAction(AllocatableAction action) {
        StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
        info.append(action.toString());
        ErrorManager.fatal(info.toString());
    }

    /**
     * Notifies to the scheduler that there have been changes in the load of a resource.
     *
     * @param <T> WorkerResourceDescription.
     * @param resource Updated resource.
     */
    protected <T extends WorkerResourceDescription> void workerLoadUpdate(ResourceScheduler<T> resource) {
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
     * Updates the worker information.
     *
     * @param <T> WorkerResourceDescription
     * @param worker Worker to update.
     * @param rs Resource Update information.
     */
    public final <T extends WorkerResourceDescription> void updateWorker(Worker<T> worker, ResourceUpdate<T> rs) {
        ResourceScheduler<T> ui = this.workers.get(worker);
        if (ui == null) {
            // Register worker if it's the first time it is useful.
            ui = addWorker(worker, this.jsm.getJSONForResource(worker), this.jsm.getJSONForImplementations());
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
     * Restarts the Worker.
     *
     * @param <T> WorkerResourceDescription
     * @param worker Worker to update.
     * @param ru Resource Update information.
     */
    public final <T extends WorkerResourceDescription> void restartWorker(Worker<T> worker, ResourceUpdate<T> ru) {
        ResourceScheduler<T> rs = this.workers.get(worker);
        workerStoppedToBeRestarted(worker, rs);
    }

    /**
     * Registers a new Worker node for the scheduler to use it and creates the corresponding ResourceScheduler.
     *
     * @param worker Worker to incorporate.
     * @return the ResourceScheduler that will manage the scheduling for the given worker.
     */
    private <T extends WorkerResourceDescription> ResourceScheduler<T> addWorker(Worker<T> worker,
        JSONObject jsonResource, JSONObject jsonImpls) {

        ResourceScheduler<T> ui = generateSchedulerForResource(worker, jsonResource, jsonImpls);
        synchronized (this.workers) {
            this.workers.put(worker, ui);
        }
        return ui;
    }

    /**
     * Contextualizes the worker by creating a new action StartWorker.
     *
     * @param ui ResourceScheduler whose worker is to contextualize.
     */
    private <T extends WorkerResourceDescription> void startWorker(ResourceScheduler<T> ui) {
        StartWorkerAction<T> action = new StartWorkerAction<>(generateSchedulingInformation(ui, null, null), ui, this);
        try {
            action.schedule(ui, (Score) null);
            action.tryToLaunch();
        } catch (UnassignedActionException | InvalidSchedulingException e) {
            // Can not be blocked nor unassigned
            LOGGER.warn(" StartWorkerAction failed: " + e);
            LOGGER.warn(" Failed ResourceScheduler: " + ui.getName());
        }
    }

    private <T extends WorkerResourceDescription> void pendingResourceUpdate(ResourceScheduler<T> worker,
        ResourceUpdate<T> modification) {
        switch (modification.getType()) {
            case INCREASE:
                // Can't happen
                break;
            case REDUCE:
                reduceWorkerResources(worker, modification);
                break;
            case BUSY:
                busyWorkerResources(worker, modification);
                break;
            default:

        }
    }

    private <T extends WorkerResourceDescription> void reduceWorkerResources(ResourceScheduler<T> worker,
        ResourceUpdate<T> modification) {
        worker.pendingModification(modification);
        SchedulingInformation schedInfo = generateSchedulingInformation(worker, null, null);
        ReduceWorkerAction<T> action = new ReduceWorkerAction<>(schedInfo, worker, this, modification);
        try {
            action.schedule(worker, (Score) null);
            action.tryToLaunch();
        } catch (UnassignedActionException | InvalidSchedulingException e) {
            // Can not be blocked nor unassigned
            LOGGER.error(" Error while reducing the worker..");
        }
    }

    private <T extends WorkerResourceDescription> void busyWorkerResources(ResourceScheduler<T> worker,
        ResourceUpdate<T> modification) {

        SchedulingInformation schedInfo = generateSchedulingInformation(worker, null, null);
        BusyWorkerAction<T> action = new BusyWorkerAction<>(schedInfo, worker, this, modification);
        try {
            action.schedule(worker, (Score) null);
            action.tryToLaunch();
        } catch (UnassignedActionException | InvalidSchedulingException e) {
            // Can not be blocked nor unassigned
            LOGGER.error(" Error while reserving resources on the worker..");
        }
    }

    /**
     * Marks a resource update as completed.
     *
     * @param <T> WorkerResourceDescription.
     * @param worker Worker to update.
     * @param modification Completed modification.
     */
    @SuppressWarnings("unchecked")
    private <T extends WorkerResourceDescription> void completedResourceUpdate(ResourceScheduler<T> worker,
        ResourceUpdate<T> modification) {
        worker.completedModification(modification);
        SchedulingInformation.changesOnWorker((ResourceScheduler<WorkerResourceDescription>) worker);
        switch (modification.getType()) {
            case INCREASE:
                increasedWorkerResources(worker, modification);
                break;
            case REDUCE:
                reducedWorkerResources(worker, (PerformedReduction<T>) modification);
                break;
            case IDLE:
                idleWorkerResources(worker, (IdleResources<T>) modification);
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
            List<AllocatableAction> unblockedActions;
            unblockedActions = this.removeCompatibleFromBlocked(worker.getResource());

            // Update worker features
            LinkedList<AllocatableAction> blockedActions = new LinkedList<>();
            this.workerFeaturesUpdate(worker, modification.getModification(), unblockedActions, blockedActions);
            // When the resource is increased it never blocks actions. No need to process
        }
    }

    @SuppressWarnings("unchecked")
    private <T extends WorkerResourceDescription> void reducedWorkerResources(ResourceScheduler<T> worker,
        PerformedReduction<T> modification) {

        // Update worker features
        // When a worker is reduced it never unblocks actions
        LinkedList<AllocatableAction> unblockedActions = new LinkedList<>();
        LinkedList<AllocatableAction> blockedActions = new LinkedList<>();
        this.workerFeaturesUpdate(worker, modification.getModification(), unblockedActions, blockedActions);
        for (AllocatableAction action : blockedActions) {
            addToBlocked(action);
        }

        LOGGER.info("Resources for worker " + worker.getName() + " have been reduced");
        DynamicMethodWorker dynamicWorker = (DynamicMethodWorker) worker.getResource();
        if (dynamicWorker.shouldBeStopped()) {
            LOGGER.info("Starting stop process for worker " + worker.getName());
            workerStopped((ResourceScheduler<WorkerResourceDescription>) worker);
            StopWorkerAction action;
            action = new StopWorkerAction(generateSchedulingInformation(worker, null, null), worker,
                this, modification);
            try {
                action.schedule((ResourceScheduler<WorkerResourceDescription>) worker, (Score) null);
                action.tryToLaunch();
            } catch (UnassignedActionException | InvalidSchedulingException e) {
                // Can not be blocked nor unassigned
                LOGGER.error("WARN: Stop action has been blocked or unassigned. It should not happen!");
            }
        } else {
            dynamicWorker.destroyResources(modification.getModification());
        }
    }


    private <T extends WorkerResourceDescription> void idleWorkerResources(ResourceScheduler<T> worker,
        IdleResources<T> modification) {

        LOGGER.debug("Releasing idle resources in the worker  " + worker.getName());
        worker.idleResources(modification.getModification());

        // We update the worker load
        workerLoadUpdate(worker);

        List<AllocatableAction> dataFreeActions = new LinkedList<>();
        List<AllocatableAction> resourceFree = new LinkedList<>();
        handleDependencyFreeActionsAndBlock(dataFreeActions, resourceFree, worker);
    }

    /**
     * One worker has been removed from the pool; actions on the node are moved out from it and the Task Scheduler is
     * notified about it.
     *
     * @param <T> WorkerResourceDescription.
     * @param resource Removed worker.
     */
    private <T extends WorkerResourceDescription> void workerStopped(ResourceScheduler<T> resource) {
        @SuppressWarnings("unchecked")
        ResourceScheduler<WorkerResourceDescription> workerRS = (ResourceScheduler<WorkerResourceDescription>) resource;
        Worker<WorkerResourceDescription> workerResource = workerRS.getResource();
        this.workers.remove(workerResource);
        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            for (Implementation impl : ce.getImplementations()) {
                int implId = impl.getImplementationId();

                LOGGER.debug("Removed Workers profile for CoreId: " + coreId + ", ImplId: " + implId
                    + " before removing:" + offVMsProfiles[coreId][implId]);
                Profile p = resource.getProfile(coreId, implId);
                if (p != null) {
                    LOGGER.info(" Accumulating worker profile data for CoreId: " + coreId + ", ImplId: " + implId
                        + " in removed workers profile");
                    offVMsProfiles[coreId][implId].accumulate(p);
                }
                LOGGER.debug("Removed Workers profile for CoreId: " + coreId + ", ImplId: " + implId
                    + " after removing:" + offVMsProfiles[coreId][implId]);
            }
        }

        List<AllocatableAction> hostedActions = resource.getHostedActions();
        for (AllocatableAction action : hostedActions) {
            action.abortExecution();
            try {
                action.unschedule();
            } catch (ActionNotFoundException | UnassignedActionException ex) {
                // Task was already moved from the worker. Do nothing!
                continue;
            }

            Score actionScore = generateActionScore(action);
            try {
                scheduleAction(action, actionScore);
                tryToLaunch(action);
            } catch (BlockedActionException bae) {
                addToBlocked(action);
            }
        }

        resource.setRemoved(true);

        this.workerRemoved(resource);

    }

    private <T extends WorkerResourceDescription> void workerStoppedToBeRestarted(Worker<T> worker,
        ResourceScheduler<T> resource) {

        // remove the worker before re-scheduling its actions so the actions aren't
        //  assigned to the same worker before the worker is re-initialized
        removeResource(resource);

        List<AllocatableAction> hostedOnResource = resource.getHostedActions();

        for (AllocatableAction action : hostedOnResource) {
            action.abortExecution();
            try {
                action.unschedule();
            } catch (ActionNotFoundException | UnassignedActionException ex) {
                // Task was already moved from the worker. Do nothing!
            }

        }

        resource.setRemoved(false);
        resource.getResource().startingNode();
        startWorker(resource);
        workerDetected(resource);

        synchronized (this.workers) {
            this.workers.put(worker, resource);
        }

        for (AllocatableAction action : hostedOnResource) {
            Score actionScore = generateActionScore(action);
            try {
                scheduleAction(action, actionScore);
                tryToLaunch(action);
            } catch (BlockedActionException bae) {
                addToBlocked(action);
            }
        }
    }

    /**
     * New worker has been detected; the Task Scheduler is notified to modify any internal structure using that
     * information.
     *
     * @param <T> WorkerResourceDescription.
     * @param resource New worker.
     */
    protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
        // There are no internal structures worker-related. No need to do
        // anything.
    }

    /**
     * One worker has been removed from the pool; the Task Scheduler is notified to modify any internal structure using
     * that information.
     *
     * @param <T> WorkerResourceDescription.
     * @param resource Removed worker.
     */
    protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
        LOGGER.info("[TaskScheduler] Remove worker " + resource.getName());
        // There are no internal structures worker-related. No need to do anything.
    }

    /**
     * Notifies to the scheduler that there have been changes in the capabilities of a resource.
     *
     * @param <T> WorkerResourceDescription.
     * @param worker Updated resource.
     * @param modification Changes performed on the resource.
     * @param unblockedActions List of actions that were blocked before the resource update and no longer are.
     * @param blockedCandidates List for returning the tasks that became blocked after the resource update.
     */
    protected <T extends WorkerResourceDescription> void workerFeaturesUpdate(ResourceScheduler<T> worker,
        T modification, List<AllocatableAction> unblockedActions, List<AllocatableAction> blockedCandidates) {
        LOGGER.info("[TaskScheduler] Updated features on worker " + worker.getName());
        // Resource capabilities had already been taken into account when assigning the actions. No need to change the
        // scheduling.

        // Prioritize them
        PriorityQueue<ObjectValue<AllocatableAction>> sortedCompatibleActions = new PriorityQueue<>();
        for (AllocatableAction action : unblockedActions) {
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, generateActionScore(action));
            sortedCompatibleActions.add(obj);
        }
        // Schedule them
        while (!sortedCompatibleActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = sortedCompatibleActions.poll();
            Score actionScore = obj.getScore();
            AllocatableAction action = obj.getObject();
            try {
                scheduleAction(action, actionScore);
                tryToLaunch(action);
            } catch (BlockedActionException bae) {
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
     * Returns the ResourceSchedulers assigned to all available workers.
     *
     * @return The ResourceSchedulers assigned to all available workers.
     */
    public final Collection<ResourceScheduler<? extends WorkerResourceDescription>> getWorkers() {
        LOGGER.info("[TaskScheduler] Get all worker resource schedulers");
        return this.workers.values();
    }

    /**
     * Returns a list of the blocked actions.
     *
     * @return A list of the blocked actions.
     */
    public final List<AllocatableAction> getBlockedActions() {
        LOGGER.info("[TaskScheduler] Get Blocked Actions");
        return this.blockedActions.getAllActions();
    }

    /**
     * Returns the number of the blocked actions.
     *
     * @return The number of the blocked actions.
     */
    public final int getNumberOfBlockedActions() {
        return this.blockedActions.getNumberTotalActions();
    }

    /**
     * Returns a list with the hosted actions on a given worker.
     *
     * @param <T> WorkerResourceDescription.
     * @param worker Worker.
     * @return A list with the hosted actions on the given worker.
     */
    public final <T extends WorkerResourceDescription> AllocatableAction[] getHostedActions(Worker<T> worker) {
        LOGGER.info("[TaskScheduler] Get Hosted actions on worker " + worker.getName());
        ResourceScheduler<T> ui = workers.get(worker);
        if (ui != null) {
            return ui.getRunningActions();
        } else {
            return new AllocatableAction[0];
        }
    }

    /**
     * Returns the blocked actions assigned to a given resource.
     *
     * @param <T> WorkerResourceDescription
     * @param worker Worker.
     * @return The blocked actions assigned to the given resource.
     */
    public final <T extends WorkerResourceDescription> PriorityQueue<AllocatableAction> getBlockedActionsOnResource(
        Worker<T> worker) {
        LOGGER.info("[TaskScheduler] Get Blocked actions on worker " + worker.getName());
        ResourceScheduler<T> ui = workers.get(worker);
        if (ui != null) {
            return ui.getBlockedActions();
        } else {
            return new PriorityQueue<>();
        }
    }

    /**
     * Returns the actions with no resources assigned.
     *
     * @return The actions with no resources assigned.
     */
    public Collection<AllocatableAction> getUnassignedActions() {
        return new LinkedList<>();
    }

    /**
     * Upgrade the action because another action of the same multi-node group has been scheduled
     * and it should be prioritised to avoid possible deadlocks.
     *
     * @param action Action to upgrade
     */
    public void upgradeAction(AllocatableAction action) {
        LOGGER.info("No upgrade required by default for " + action);
        // Nothing to do by default
    }

    /**
     * Remove a resource when lost.
     *
     * @param resource Resource to be removed
     */
    public <T extends WorkerResourceDescription> void removeResource(ResourceScheduler<T> resource) {

        @SuppressWarnings("unchecked")
        ResourceScheduler<WorkerResourceDescription> workerRS =
            (ResourceScheduler<WorkerResourceDescription>) resource;

        Worker<WorkerResourceDescription> workerResource = workerRS.getResource();
        this.workers.remove(workerResource);

        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            for (Implementation impl : ce.getImplementations()) {
                int implId = impl.getImplementationId();

                LOGGER.debug("Removed Workers profile for CoreId: " + coreId + ", ImplId: " + implId
                    + " before removing:" + offVMsProfiles[coreId][implId]);
                Profile p = resource.getProfile(coreId, implId);
                if (p != null) {
                    LOGGER.info(" Accumulating worker profile data for CoreId: " + coreId + ", ImplId: " + implId
                        + " in removed workers profile");
                    offVMsProfiles[coreId][implId].accumulate(p);
                }
                LOGGER.debug("Removed Workers profile for CoreId: " + coreId + ", ImplId: " + implId
                    + " after removing:" + offVMsProfiles[coreId][implId]);
            }
        }

        resource.setRemoved(true);
        this.workerRemoved(resource);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* RESOURCE OPTIMIZER INFORMATION ***************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    /**
     * Returns the current workload state.
     *
     * @return The current workload state.
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
            List<Implementation>[] impls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (Implementation impl : impls[coreId]) {
                    coreProfile[coreId].accumulate(ui.getProfile(impl));
                }
            }

            AllocatableAction[] runningActions = ui.getRunningActions();
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
            state.registerTimes(coreId, coreProfile[coreId].getMinExecutionTime(),
                coreProfile[coreId].getAverageExecutionTime(), coreProfile[coreId].getMaxExecutionTime());
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
     * Prints the task summary on a given logger {@code logger}.
     *
     * @param logger Logger where to pring the task summary.
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
            for (CoreElement ce : CoreManager.getAllCores()) {
                int coreId = ce.getCoreId();
                for (Implementation impl : ce.getImplementations()) {
                    String signature = impl.getSignature();
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
            for (Entry<String, Integer> entry : CoreManager.getSignaturesToCeAndImpls().entrySet()) {
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
        for (Entry<String, Integer> entry : CoreManager.getSignaturesToCeAndImpls().entrySet()) {
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
     * Returns the running actions on a given {@code worker} pre-pending the given {@code prefix}.
     *
     * @param <T> WorkerResourceDescription
     * @param worker Worker.
     * @param prefix String prefix.
     * @return The running actions on the given worker.
     */
    public final <T extends WorkerResourceDescription> String getRunningActionMonitorData(Worker<T> worker,
        String prefix) {
        LOGGER.info("[TaskScheduler] Get running actions monitoring data");
        StringBuilder runningActions = new StringBuilder();

        ResourceScheduler<T> ui = workers.get(worker);
        if (ui != null) {
            AllocatableAction[] hostedActions = ui.getRunningActions();
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
     * Returns the coreElement information with the given {@code prefix}.
     *
     * @param prefix String prefix.
     * @return The coreElement information with a given prefix.
     */
    public final String getCoresMonitoringData(String prefix) {
        LOGGER.info("[TaskScheduler] Get cores monitoring data");
        // Create size structure for profiles and populate with information from turned off VMs
        int coreCount = CoreManager.getCoreCount();
        Profile[][] implementationsProfile = new Profile[coreCount][];
        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            int implsCount = ce.getImplementationsCount();
            implementationsProfile[coreId] = new Profile[implsCount];
            for (int implId = 0; implId < implsCount; ++implId) {
                implementationsProfile[coreId][implId] = new Profile();
                implementationsProfile[coreId][implId].accumulate(this.offVMsProfiles[coreId][implId]);
            }
        }

        // Retrieve information from workers
        for (ResourceScheduler<? extends WorkerResourceDescription> ui : this.workers.values()) {
            if (ui == null) {
                continue;
            }
            List<Implementation>[] runningCoreImpls = ui.getExecutableImpls();
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
        for (CoreElement ce : CoreManager.getAllCores()) {
            int coreId = ce.getCoreId();
            coresInfo.append(prefix).append("\t").append("<Core id=\"").append(coreId).append("\"").append(">")
                .append("\n");
            for (Implementation impl : ce.getImplementations()) {
                int implId = impl.getImplementationId();
                // Get method's signature
                String signature = impl.getSignature();

                coresInfo.append(prefix).append("\t\t").append("<Impl id=\"").append(implId).append("\"").append(">")
                    .append("\n");

                coresInfo.append(prefix).append("\t\t\t").append("<Signature>").append(signature).append("</Signature>")
                    .append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<MeanExecutionTime>")
                    .append(implementationsProfile[coreId][implId].getAverageExecutionTime())
                    .append("</MeanExecutionTime>").append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<MinExecutionTime>")
                    .append(implementationsProfile[coreId][implId].getMinExecutionTime()).append("</MinExecutionTime>")
                    .append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<MaxExecutionTime>")
                    .append(implementationsProfile[coreId][implId].getMaxExecutionTime()).append("</MaxExecutionTime>")
                    .append("\n");
                coresInfo.append(prefix).append("\t\t\t").append("<ExecutedCount>")
                    .append(implementationsProfile[coreId][implId].getExecutionCount()).append("</ExecutedCount>")
                    .append("\n");
                coresInfo.append(prefix).append("\t\t").append("</Impl>").append("\n");

            }
            coresInfo.append(prefix).append("\t").append("</Core>").append("\n");
        }
        coresInfo.append(prefix).append("</CoresInfo>").append("\n");

        return coresInfo.toString();
    }

    /**
     * Updates the current state.
     */
    public void updateState() {
        // Update static workers
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : this.workers.values()) {
            JSONObject oldResource = this.jsm.getJSONForResource(rs.getResource());
            if (oldResource == null) {
                this.jsm.addResourceJSON(rs);
            } else {
                updateResourceJSON(rs);
            }
        }
        /*
         * for (CloudProvider cp : ResourceManager.getAvailableCloudProviders()) { JSONObject cpJSON =
         * jsm.getJSONForCloudProvider(cp); if (cpJSON == null) { cpJSON = new JSONObject(); cloud.put(cp.getName(),
         * cpJSON); } for (CloudInstanceTypeDescription citd : cp.getAllTypes()) { JSONObject citdJSON =
         * jsm.getJSONForCloudInstanceTypeDescription(cp, citd); if (citdJSON == null) { citdJSON = new JSONObject();
         * cpJSON.put(citd.getName(), citdJSON); } } }
         *
         *
         * int coreCount = this.offVMsProfiles.length; // Aggregate offVMs as initial Profile values for (int coreId =
         * 0; coreId < coreCount; coreId++) { int implCount = this.offVMsProfiles[coreId].length; for (int implId = 0;
         * implId < implCount; implId++) { accumulateImplementationJSON(coreId, implId, offVMsProfiles[coreId][implId]);
         * } }
         */
    }

    /**
     * Updates the ResourceScheduler with the loaded JSON information.
     *
     * @param rs ResourceScheduler to update.
     */
    public void updateResourceJSON(ResourceScheduler<? extends WorkerResourceDescription> rs) {
        JSONObject difference = this.jsm.updateResourceJSON(rs);
        JSONObject implsdiff = difference.getJSONObject("implementations");

        // Increasing Implementation stats
        for (CoreElement ce : CoreManager.getAllCores()) {
            for (Implementation impl : ce.getImplementations()) {
                JSONObject implJSON = this.jsm.getJSONForImplementation(impl);
                Profile p = generateProfile(implsdiff.getJSONObject(impl.getSignature()));
                if (implJSON == null) {
                    this.jsm.addImplementationJSON(impl, p);
                } else {
                    this.jsm.accumulateImplementationJSON(impl, p);
                }
            }
        }
    }

    /**
     * Returns whether the external adaptation is enabled or not.
     *
     * @return {@literal true} if the external adaptation is enabled, {@literal false} otherwise.
     */
    public boolean isExternalAdaptationEnabled() {
        return this.externalAdaptation;
    }

    /**
     * Returns the JSON representation of a cloud instance type.
     *
     * @param cp Cloud provider.
     * @param ctid Cloud instance type description.
     * @return The JSON representation of the given cloud instance type.
     */
    public JSONObject getJSONForCloudInstanceTypeDescription(CloudProvider cp, CloudInstanceTypeDescription ctid) {
        return this.jsm.getJSONForCloudInstanceTypeDescription(cp, ctid);
    }

    /**
     * Returns the JSON information of all the implementations.
     *
     * @return A JSONObject containing all the information about the implementations.
     */
    public JSONObject getJSONForImplementations() {
        return this.jsm.getJSONForImplementations();
    }


    /**
     * Get next resource to execute a distributed task.
     *
     * @param coreId CoreId of the task
     * @return resource to execute the task.
     */
    public ResourceScheduler<? extends WorkerResourceDescription> getNextResourForDistributed(int coreId) {
        LinkedList<ResourceScheduler<? extends WorkerResourceDescription>> resourceList =
            this.distributedTasksResources.computeIfAbsent(coreId,
                resources -> new LinkedList<ResourceScheduler<? extends WorkerResourceDescription>>(getWorkers()));
        // Get the first and add at the end
        ResourceScheduler<? extends WorkerResourceDescription> res = resourceList.poll();
        resourceList.add(res);
        return res;
    }

    private class WorkersMap {

        private final Map<Resource,
            ResourceScheduler<? extends WorkerResourceDescription>> map;


        public WorkersMap() {
            this.map = new HashMap<>();
        }

        public <T extends WorkerResourceDescription> void put(Resource w, ResourceScheduler<T> rs) {
            this.map.put(w, rs);
        }

        @SuppressWarnings("unchecked")
        public <T extends WorkerResourceDescription> ResourceScheduler<T> get(Resource w) {
            return (ResourceScheduler<T>) this.map.get(w);
        }

        private <T extends WorkerResourceDescription> void remove(Resource resource) {
            this.map.remove(resource);
        }

        private Collection<ResourceScheduler<? extends WorkerResourceDescription>> values() {
            return map.values();
        }
    }

}
