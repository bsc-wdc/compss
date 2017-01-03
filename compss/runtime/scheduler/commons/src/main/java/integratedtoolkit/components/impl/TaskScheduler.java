package integratedtoolkit.components.impl;

import integratedtoolkit.components.ResourceUser.WorkloadStatus;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.FailedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingInformation;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.allocatableactions.StartWorkerAction;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.util.ResourceScheduler;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ActionSet;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ErrorManager;

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedList;
import java.util.Map.Entry;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TaskScheduler<P extends Profile, T extends WorkerResourceDescription> {

    // Logger
    protected static final Logger logger = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean debug = logger.isDebugEnabled();

    private final ActionSet<P, T> blockedActions = new ActionSet<>();
    private int[] readyCounts = new int[CoreManager.getCoreCount()];
    protected final HashMap<Worker<T>, ResourceScheduler<P, T>> workers = new HashMap<>();


    /**
     * Construct a new Task Scheduler
     * 
     */
    public TaskScheduler() {
        // Nothing to do since all attributes are already initialized
    }

    /**
     * New Core Elements have been detected; the Task Scheduler needs to be notified to modify any internal structure
     * using that information.
     *
     */
    public final void coreElementsUpdated() {
        blockedActions.updateCoreCount();
        readyCounts = new int[CoreManager.getCoreCount()];
    }

    /**
     * Introduces a new action in the Scheduler system. The method should place the action in a resource hurriedly
     *
     * @param action
     *            Action to be scheduled.
     */
    public final void newAllocatableAction(AllocatableAction<P, T> action) {
        if (!action.hasDataPredecessors()) {
            if (action.getImplementations().length > 0) {
                int coreId = action.getImplementations()[0].getCoreId();
                readyCounts[coreId]++;
            }
        }
        Score actionScore = generateActionScore(action);
        try {
            scheduleAction(action, actionScore);
            try {
                action.tryToLaunch();
            } catch (InvalidSchedulingException ise) {
                boolean keepTrying = true;
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                    AllocatableAction<P, T> pre = action.getConstrainingPredecessors().get(i);
                    action.schedule(pre.getAssignedResource(), actionScore);
                    try {
                        action.tryToLaunch();
                        keepTrying = false;
                    } catch (InvalidSchedulingException ise2) {
                        // Try next predecessor
                        keepTrying = true;
                    }
                }
            }
        } catch (UnassignedActionException ure) {
            StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
            info.append(action.toString());
            ErrorManager.fatal(info.toString());
        } catch (BlockedActionException bae) {
            logger.info("Blocked Action: " + action);
            blockedActions.addAction(action);
        }
    }

    /**
     * Registers an action as completed and releases all the resource and data dependencies.
     *
     * @param action
     *            action that has finished
     */
    public final void actionCompleted(AllocatableAction<P, T> action) {
        ResourceScheduler<P, T> resource = action.getAssignedResource();
        if (action.getImplementations().length > 0) {
            Integer coreId = action.getImplementations()[0].getCoreId();
            if (coreId != null) {
                readyCounts[coreId]--;
            }
        }
        LinkedList<AllocatableAction<P, T>> dataFreeActions = action.completed();
        for (AllocatableAction<P, T> dataFreeAction : dataFreeActions) {
            if (dataFreeAction != null && dataFreeAction.isNotScheduling()) {
                if (dataFreeAction.getImplementations().length > 0) {
                    Integer coreId = dataFreeAction.getImplementations()[0].getCoreId();
                    if (coreId != null) {
                        readyCounts[coreId]++;
                    }
                }

                try {
                    dependencyFreeAction(dataFreeAction);
                } catch (BlockedActionException bae) {
                    if (!dataFreeAction.isLocked() && !dataFreeAction.isRunning()) {
                        logger.info("Blocked Action: " + dataFreeAction);
                        blockedActions.addAction(dataFreeAction);
                    }
                }
            }
        }

        LinkedList<AllocatableAction<P, T>> resourceFree = resource.unscheduleAction(action);
        workerLoadUpdate((ResourceScheduler<P, T>) action.getAssignedResource());
        HashSet<AllocatableAction<P, T>> freeTasks = new HashSet<>();
        freeTasks.addAll(dataFreeActions);
        freeTasks.addAll(resourceFree);
        for (AllocatableAction<P, T> a : freeTasks) {
            if (a != null && !a.isLocked() && !a.isRunning()) {
                try {
                    try {
                        a.tryToLaunch();
                    } catch (InvalidSchedulingException ise) {
                        Score aScore = generateActionScore(a);
                        boolean keepTrying = true;
                        for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                            AllocatableAction<P, T> pre = action.getConstrainingPredecessors().get(i);
                            action.schedule(pre.getAssignedResource(), aScore);
                            try {
                                action.tryToLaunch();
                                keepTrying = false;
                            } catch (InvalidSchedulingException ise2) {
                                // Try next predecessor
                                keepTrying = true;
                            }
                        }
                    }

                } catch (UnassignedActionException ure) {
                    StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
                    info.append(action.toString());
                    ErrorManager.fatal(info.toString());
                } catch (BlockedActionException bae) {
                    if (a != null && !a.isLocked() && !a.isRunning()) {
                        logger.info("Blocked Action: " + a, bae);
                        blockedActions.addAction(a);
                    }
                }
            }
        }

    }

    /**
     * Registers an error on the action given as a parameter. The action itself processes the error and triggers with
     * any possible solution to re-execute it.
     *
     * @param action
     *            action raising the error
     */
    public final void errorOnAction(AllocatableAction<P, T> action) {
        ResourceScheduler<P, T> resource = action.getAssignedResource();
        LinkedList<AllocatableAction<P, T>> resourceFree;
        try {
            action.error();
            resourceFree = resource.unscheduleAction(action);
            Score actionScore = generateActionScore(action);
            try {
                scheduleAction(action, actionScore);
                try {
                    action.tryToLaunch();
                } catch (InvalidSchedulingException ise) {
                    boolean keepTrying = true;
                    for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                        AllocatableAction<P, T> pre = action.getConstrainingPredecessors().get(i);
                        action.schedule(pre.getAssignedResource(), actionScore);
                        try {
                            action.tryToLaunch();
                            keepTrying = false;
                        } catch (InvalidSchedulingException ise2) {
                            // Try next predecessor
                            keepTrying = true;
                        }
                    }
                }

            } catch (UnassignedActionException ure) {
                StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
                info.append(action.toString());
                ErrorManager.fatal(info.toString());

            } catch (BlockedActionException ex) {
                logger.info("Blocked Action: " + action);
                blockedActions.addAction(action);
                if (action.getImplementations().length > 0) {
                    int coreId = action.getImplementations()[0].getCoreId();
                    readyCounts[coreId]--;
                }
            }
        } catch (FailedActionException fae) {
            if (action.getImplementations().length > 0) {
                int coreId = action.getImplementations()[0].getCoreId();
                readyCounts[coreId]--;
            }
            resourceFree = new LinkedList<>();
            for (AllocatableAction<P, T> failed : action.failed()) {
                resourceFree.addAll(resource.unscheduleAction(failed));
            }
            workerLoadUpdate(action.getAssignedResource());
        }
        for (AllocatableAction<P, T> a : resourceFree) {
            try {
                try {
                    a.tryToLaunch();
                } catch (InvalidSchedulingException ise) {
                    Score aScore = generateActionScore(a);
                    boolean keepTrying = true;
                    for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                        AllocatableAction<P, T> pre = a.getConstrainingPredecessors().get(i);
                        a.schedule(pre.getAssignedResource(), aScore);
                        try {
                            action.tryToLaunch();
                            keepTrying = false;
                        } catch (InvalidSchedulingException ise2) {
                            // Try next predecessor
                            keepTrying = true;
                        }
                    }
                }
            } catch (UnassignedActionException ure) {
                StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
                info.append(action.toString());
                ErrorManager.fatal(info.toString());
            } catch (BlockedActionException bae) {
                logger.info("Blocked Action: " + action);
                blockedActions.addAction(action);
            }
        }
    }

    /**
     * Updates the worker information
     * 
     * @param worker
     */
    @SuppressWarnings({ "unchecked", "rawtypes" })
    public final void updatedWorker(Worker<T> worker) {
        ResourceScheduler<P, T> ui = workers.get(worker);
        if (ui == null) {
            // Register worker if it's the first time it is useful.
            ui = generateSchedulerForResource(worker);
            synchronized (workers) {
                workers.put(worker, ui);
            }

            StartWorkerAction<P, T> action = new StartWorkerAction(generateSchedulingInformation(), ui, this);
            try {
                action.schedule(ui, (Score) null);
                action.tryToLaunch();
            } catch (Exception e) {
                // Can not be blocked nor unassigned
            }
            workerDetected(ui);
        }

        // Update links CE -> Worker
        SchedulingInformation.changesOnWorker(ui);

        if (ui.getExecutableCores().isEmpty()) {
            synchronized (workers) {
                workers.remove(ui.getResource());
            }
            this.workerRemoved(ui);
        } else {
            // Inspect blocked actions to be freed
            LinkedList<AllocatableAction<P, T>> stillBlocked = new LinkedList<>();
            for (AllocatableAction<P, T> action : blockedActions.removeAllCompatibleActions(worker)) {
                Score actionScore = generateActionScore(action);
                try {
                    logger.info("Unblocked Action: " + action);
                    scheduleAction(action, actionScore);
                    if (!action.hasDataPredecessors()) {
                        if (action.getImplementations().length > 0) {
                            int coreId = action.getImplementations()[0].getCoreId();
                            readyCounts[coreId]++;
                        }
                    }
                    try {
                        action.tryToLaunch();
                    } catch (InvalidSchedulingException ise) {
                        boolean keepTrying = true;
                        for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                            AllocatableAction<P, T> pre = action.getConstrainingPredecessors().get(i);
                            action.schedule(pre.getAssignedResource(), actionScore);
                            try {
                                action.tryToLaunch();
                                keepTrying = false;
                            } catch (InvalidSchedulingException ise2) {
                                // Try next predecessor
                                keepTrying = true;
                            }
                        }
                    }
                } catch (UnassignedActionException ure) {
                    StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
                    info.append(action.toString());
                    ErrorManager.fatal(info.toString());

                } catch (BlockedActionException bae) {
                    // We should never follow this path except if there is some
                    // error on the resource management
                    stillBlocked.add(action);
                }
            }

            for (AllocatableAction<P, T> a : stillBlocked) {
                blockedActions.addAction(a);
            }
            this.workerLoadUpdate(ui);
        }

    }

    public LinkedList<AllocatableAction<P, T>> getBlockedActions() {
        // Parameter null to get all blocked actions
        return this.blockedActions.getActions(null);
    }

    public LinkedList<AllocatableAction<P, T>> getHostedActions(Worker<T> worker) {
        ResourceScheduler<P, T> ui = workers.get(worker);
        return ui.getHostedActions();
    }

    public PriorityQueue<AllocatableAction<P, T>> getBlockedActionsOnResource(Worker<T> worker) {
        ResourceScheduler<P, T> ui = workers.get(worker);
        return ui.getBlockedActions();
    }

    /**
     * Prints the task summary on a given logger @logger
     * 
     * @param logger
     */
    public void getTaskSummary(Logger logger) {
        // Structures for global and per worker stats
        int coreCount = CoreManager.getCoreCount();
        Profile[] coreGlobalProfiles = new Profile[coreCount];
        for (int i = 0; i < coreCount; ++i) {
            coreGlobalProfiles[i] = new Profile();
        }
        HashMap<String, Profile[]> coreProfilesPerWorker = new HashMap<>();

        // Retrieve information
        for (ResourceScheduler<P, T> ui : workers.values()) {
            if (ui == null) {
                continue;
            }

            Profile[] coreProfiles = new Profile[coreCount];
            for (int i = 0; i < coreCount; ++i) {
                coreProfiles[i] = new Profile();
            }
            LinkedList<Implementation<T>>[] impls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (Implementation<T> impl : impls[coreId]) {
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
    public String getRunningActionMonitorData(Worker<T> worker, String prefix) {
        StringBuilder runningActions = new StringBuilder();

        ResourceScheduler<P, T> ui = workers.get(worker);
        LinkedList<AllocatableAction<P, T>> hostedActions = ui.getHostedActions();
        for (AllocatableAction<P, T> action : hostedActions) {
            runningActions.append(prefix);
            runningActions.append("<Action>").append(action.toString()).append("</Action>");
            runningActions.append("\n");
        }
        return runningActions.toString();
    }

    /**
     * Returns the coreElement information with the given @prefix
     * 
     * @param prefix
     * @return
     */
    public String getCoresMonitoringData(String prefix) {
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
        for (ResourceScheduler<P, T> ui : workers.values()) {
            if (ui == null) {
                continue;
            }
            LinkedList<Implementation<T>>[] runningCoreImpls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (Implementation<T> impl : runningCoreImpls[coreId]) {
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
        int coreCount = CoreManager.getCoreCount();
        Profile[] coreProfile = new Profile[coreCount];
        for (int coreId = 0; coreId < coreCount; coreId++) {
            coreProfile[coreId] = new Profile();
        }

        for (ResourceScheduler<P, T> ui : workers.values()) {
            if (ui == null) {
                continue;
            }
            LinkedList<Implementation<T>>[] impls = ui.getExecutableImpls();
            for (int coreId = 0; coreId < coreCount; coreId++) {
                for (Implementation<T> impl : impls[coreId]) {
                    coreProfile[coreId].accumulate(ui.getProfile(impl));
                }
            }

            LinkedList<AllocatableAction<P, T>> runningActions = ui.getHostedActions();
            long now = System.currentTimeMillis();
            for (AllocatableAction<P, T> running : runningActions) {
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
    /*
     * protected void scheduleAction(AllocatableAction<P, T> action, Score actionScore) throws BlockedActionException {
     * try { action.schedule(actionScore); } catch (UnassignedActionException ure) { StringBuilder info = new
     * StringBuilder("Scheduler has lost track of action "); info.append(action.toString());
     * ErrorManager.fatal(info.toString()); } }
     */
    protected void scheduleAction(AllocatableAction<P, T> action, Score actionScore) throws BlockedActionException {
        try {
            action.schedule(actionScore);
        } catch (UnassignedActionException ure) {
            StringBuilder info = new StringBuilder("Scheduler has lost track of action ");
            info.append(action.toString());
            ErrorManager.fatal(info.toString());
        }
    }

    /**
     * Notifies to the scheduler that some actions have become free of data dependencies.
     *
     * @param dataFree
     *            Data dependency-free action
     */
    public void dependencyFreeAction(AllocatableAction<P, T> dataFree) throws BlockedActionException {
        // All actions should have already been assigned to a resource, no need
        // to change the assignation once they become free of dependencies
    }

    /**
     * New worker has been detected; the Task Scheduler is notified to modify any internal structure using that
     * information.
     *
     * @param resource
     *            new worker
     */
    protected void workerDetected(ResourceScheduler<P, T> resource) {
        // There are no internal structures worker-related. No need to do
        // anything.
    }

    /**
     * One worker has been removed from the pool; the Task Scheduler is notified to modify any internal structure using
     * that information.
     *
     * @param resource
     *            removed worker
     */
    protected void workerRemoved(ResourceScheduler<P, T> resource) {
        // There are no internal structures worker-related. No need to do
        // anything.
    }

    /**
     * Notifies to the scheduler that there have been changes in the load of a resource.
     *
     * @param resources
     *            updated resource
     */
    public void workerLoadUpdate(ResourceScheduler<P, T> resources) {
        // Resource capabilities had already been taken into account when
        // assigning the actions. No need to change the assignation.

    }

    public ResourceScheduler<P, T> generateSchedulerForResource(Worker<T> w) {
        return new ResourceScheduler<P, T>(w);
    }

    public SchedulingInformation<P, T> generateSchedulingInformation() {
        return new SchedulingInformation<P, T>();
    }

    public Score generateActionScore(AllocatableAction<P, T> action) {
        return new Score(action.getPriority(), 0, 0, 0);
    }

    @SuppressWarnings("unchecked")
    public ResourceScheduler<P, T>[] getWorkers() {
        synchronized (workers) {
            Collection<ResourceScheduler<P, T>> resScheds = workers.values();
            ResourceScheduler<P, T>[] scheds = new ResourceScheduler[resScheds.size()];
            workers.values().toArray(scheds);
            return scheds;
        }
    }

    public void shutdown() {
        // Nothing to do
    }

}
