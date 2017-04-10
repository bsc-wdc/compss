package integratedtoolkit.scheduler.fullGraphScheduler;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.ActionOrchestrator;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.FullGraphScore;
import integratedtoolkit.scheduler.types.Gap;
import integratedtoolkit.scheduler.types.LocalOptimizationState;
import integratedtoolkit.scheduler.types.OptimizationAction;
import integratedtoolkit.scheduler.types.PriorityActionSet;
import integratedtoolkit.scheduler.types.Profile;
import integratedtoolkit.scheduler.types.SchedulingEvent;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.TaskDescription;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Set;

/**
 * Representation of a Scheduler that considers the full task graph
 *
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class FullGraphResourceScheduler<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        extends ResourceScheduler<P, T, I> {

    public static final long DATA_TRANSFER_DELAY = 200;
    
    private final ActionOrchestrator<P,T,I> orchestrator;

    private final LinkedList<Gap<P, T, I>> gaps;
    private OptimizationAction<P, T, I> opAction;
    private Set<AllocatableAction<P, T, I>> pendingUnschedulings = new HashSet<>();


    public FullGraphResourceScheduler(Worker<T, I> w, ActionOrchestrator<P,T,I> orchestrator) {
        super(w);
        this.orchestrator = orchestrator;
        gaps = new LinkedList<>();
        addGap(new Gap<P, T, I>(Long.MIN_VALUE, Long.MAX_VALUE, null, myWorker.getDescription().copy(), 0));
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     ------------------ Score Methods ------------------
     ---------------------------------------------------
     --------------------------------------------------*/
    /**
     *
     * @param action
     * @param params
     * @param actionScore
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public Score generateResourceScore(AllocatableAction<P, T, I> action, TaskDescription params, Score actionScore) {
        LOGGER.debug("[FullGraphScheduler] Generate resource score for action " + action.getId());
        
        double resScore = actionScore.calculateResourceScore(params, myWorker);
        for (AllocatableAction<P, T, I> pred : action.getDataPredecessors()) {
            if (pred.isPending() && pred.getAssignedResource() == this) {
                resScore++;
            }
        }
        resScore = params.getParameters().length - resScore;
        long lessTimeStamp = Long.MAX_VALUE;
        Gap<P, T, I> g = gaps.peekFirst();
        if (g != null) {
            lessTimeStamp = g.getInitialTime();
            if (lessTimeStamp < 0) {
                lessTimeStamp = 0;
            }
        }
        return new FullGraphScore<P, T, I>((FullGraphScore<P, T, I>) actionScore, resScore * DATA_TRANSFER_DELAY, 0, lessTimeStamp, 0);
    }

    /**
     *
     * @param action
     * @param params
     * @param impl
     * @param resourceScore
     * @return
     */
    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction<P, T, I> action, TaskDescription params, I impl, Score resourceScore) {
        LOGGER.debug("[FullGraphScheduler] Generate implementation score for action " + action.getId());
        
        ResourceDescription rd = impl.getRequirements().copy();
        long resourceFreeTime = 0;
        try {
            for (Gap<P, T, I> g : gaps) {
                rd.reduceDynamic(g.getResources());
                if (rd.isDynamicUseless()) {
                    resourceFreeTime = g.getInitialTime();
                    break;
                }
            }
        } catch (ConcurrentModificationException cme) {
            resourceFreeTime = 0;
        }
        if (resourceFreeTime < 0) {
            resourceFreeTime = 0;
        }
        long implScore;
        Profile p = this.getProfile(impl);
        if (p != null) {
            implScore = p.getAverageExecutionTime();
        } else {
            implScore = 0;
        }
        // The data transfer penalty is already included on the datadependency time of the resourceScore
        return new FullGraphScore<P, T, I>((FullGraphScore<P, T, I>) resourceScore, 0, 0, resourceFreeTime, implScore);
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     ---------------- Scheduler Methods ----------------
     ---------------------------------------------------
     --------------------------------------------------*/
    @Override
    public void scheduleAction(AllocatableAction<P, T, I> action) {
        try {
            synchronized (gaps) {
                if (opAction != null) {
                    ((FullGraphSchedulingInformation<P, T, I>) opAction.getSchedulingInfo()).addSuccessor(action);
                    ((FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo()).addPredecessor(opAction);
                } else {
                    scheduleUsingGaps(action, gaps);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception on initial schedule", e);
        }
    }

    @Override
    public LinkedList<AllocatableAction<P, T, I>> unscheduleAction(AllocatableAction<P, T, I> action) {
        LinkedList<AllocatableAction<P, T, I>> freeTasks = new LinkedList<>();
        FullGraphSchedulingInformation<P, T, I> actionDSI = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
        LinkedList<AllocatableAction<P, T, I>> successors = new LinkedList<>();

        // Create predecessor list
        // Lock access to predecessors
        for (AllocatableAction<P, T, I> pred : actionDSI.getPredecessors()) {
            FullGraphSchedulingInformation<P, T, I> predDSI = (FullGraphSchedulingInformation<P, T, I>) pred.getSchedulingInfo();
            predDSI.lock();
        }
        // Lock access to the current Action
        actionDSI.lock();
        // Remove action from predecessors
        for (AllocatableAction<P, T, I> pred : actionDSI.getPredecessors()) {
            FullGraphSchedulingInformation<P, T, I> predDSI = (FullGraphSchedulingInformation<P, T, I>) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        // Create successor list
        // lock access to successors
        for (AllocatableAction<P, T, I> successor : actionDSI.getSuccessors()) {
            FullGraphSchedulingInformation<P, T, I> succDSI = (FullGraphSchedulingInformation<P, T, I>) successor.getSchedulingInfo();
            succDSI.lock();
            successors.add(successor);
        }

        for (AllocatableAction<P, T, I> successor : actionDSI.getSuccessors()) {
            FullGraphSchedulingInformation<P, T, I> successorDSI = (FullGraphSchedulingInformation<P, T, I>) successor.getSchedulingInfo();
            // Remove predecessor
            successorDSI.removePredecessor(action);
            // Link with action predecessors
            for (AllocatableAction<P, T, I> predecessor : actionDSI.getPredecessors()) {
                FullGraphSchedulingInformation<P, T, I> predDSI = (FullGraphSchedulingInformation<P, T, I>) predecessor.getSchedulingInfo();
                if (predDSI.isScheduled()) {
                    successorDSI.addPredecessor(predecessor);
                    predDSI.addSuccessor(successor);
                }
            }
            // Check executability
            if (successorDSI.isExecutable()) {
                freeTasks.add(successor);
            }
        }

        // Unlock access to predecessors
        for (AllocatableAction<P, T, I> pred : actionDSI.getPredecessors()) {
            FullGraphSchedulingInformation<P, T, I> predDSI = (FullGraphSchedulingInformation<P, T, I>) pred.getSchedulingInfo();
            predDSI.unlock();
        }

        // Clear action predecessors and successors
        actionDSI.unscheduled();

        if (actionDSI.isOnOptimization()) {
            synchronized (gaps) {
                pendingUnschedulings.add(action);
            }
        }
        // Unlock access to current action
        actionDSI.unlock();

        // Unlock access to successors
        for (AllocatableAction<P, T, I> successor : successors) {
            FullGraphSchedulingInformation<P, T, I> successorDSI = (FullGraphSchedulingInformation<P, T, I>) successor.getSchedulingInfo();
            successorDSI.unlock();
        }
        return freeTasks;
    }

    @Override
    public void clear() {
        super.clear();
        gaps.clear();
        addGap(new Gap<P, T, I>(Long.MIN_VALUE, Long.MAX_VALUE, null, myWorker.getDescription().copy(), 0));
    }

    private void scheduleUsingGaps(AllocatableAction<P, T, I> action, LinkedList<Gap<P, T, I>> gaps) {
        long expectedStart = 0;
        // Compute start time due to data dependencies
        for (AllocatableAction<P, T, I> predecessor : action.getDataPredecessors()) {
            FullGraphSchedulingInformation<P, T, I> predDSI = ((FullGraphSchedulingInformation<P, T, I>) predecessor.getSchedulingInfo());
            if (predDSI.isScheduled()) {
                long predEnd = predDSI.getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
            }
        }
        FullGraphSchedulingInformation<P, T, I> schedInfo = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
        I impl = action.getAssignedImplementation();
        Profile p = getProfile(impl);
        ResourceDescription constraints = impl.getRequirements().copy();
        LinkedList<AllocatableAction<P, T, I>> predecessors = new LinkedList<>();

        Iterator<Gap<P, T, I>> gapIt = gaps.descendingIterator();
        boolean fullyCoveredReqs = false;
        // Compute predecessors and update gaps
        // Check gaps before data start
        while (gapIt.hasNext() && !fullyCoveredReqs) {
            Gap<P, T, I> gap = gapIt.next();
            if (gap.getInitialTime() <= expectedStart) {
                AllocatableAction<P, T, I> predecessor = (AllocatableAction<P, T, I>) gap.getOrigin();
                if (predecessor != null) {
                    FullGraphSchedulingInformation<P, T, I> predDSI = ((FullGraphSchedulingInformation<P, T, I>) predecessor
                            .getSchedulingInfo());
                    predDSI.lock();
                    predecessors.add(predecessor);
                }
                ResourceDescription gapResource = gap.getResources();
                ResourceDescription.reduceCommonDynamics(gapResource, constraints);
                if (gapResource.isDynamicUseless()) {
                    gapIt.remove();
                }
                if (constraints.isDynamicUseless()) {
                    fullyCoveredReqs = true;
                }
            }
        }
        // Check gaps after data start
        gapIt = gaps.iterator();
        while (gapIt.hasNext() && !fullyCoveredReqs) {
            Gap<P, T, I> gap = gapIt.next();
            AllocatableAction<P, T, I> predecessor = (AllocatableAction<P, T, I>) gap.getOrigin();
            if (predecessor != null) {
                FullGraphSchedulingInformation<P, T, I> predDSI = ((FullGraphSchedulingInformation<P, T, I>) predecessor
                        .getSchedulingInfo());
                predDSI.lock();
                predecessors.add(predecessor);
            }
            ResourceDescription gapResource = gap.getResources();
            ResourceDescription.reduceCommonDynamics(gapResource, constraints);
            if (gapResource.isDynamicUseless()) {
                gapIt.remove();
            }
            if (constraints.isDynamicUseless()) {
                fullyCoveredReqs = true;
            }
        }

        // Lock acces to the current task
        schedInfo.lock();
        schedInfo.scheduled();

        // Add dependencies
        // Unlock access to predecessor
        for (AllocatableAction<P, T, I> predecessor : predecessors) {
            FullGraphSchedulingInformation<P, T, I> predDSI = ((FullGraphSchedulingInformation<P, T, I>) predecessor.getSchedulingInfo());
            if (predDSI.isScheduled()) {
                long predEnd = predDSI.getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
                schedInfo.addPredecessor(predecessor);
                predDSI.addSuccessor(action);
            }
            predDSI.unlock();
        }

        // Compute end time
        schedInfo.setExpectedStart(expectedStart);
        long expectedEnd = expectedStart;
        if (p != null) {
            expectedEnd += p.getAverageExecutionTime();
        }
        schedInfo.setExpectedEnd(expectedEnd);

        // Unlock access to current task
        schedInfo.unlock();

        // Create new Gap correspondin to the resources released by the action
        addGap(new Gap<P, T, I>(expectedEnd, Long.MAX_VALUE, action, impl.getRequirements().copy(), 0));
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     -------------- Optimization Methods ---------------
     ---------------------------------------------------
     --------------------------------------------------*/
    public PriorityQueue<AllocatableAction<P, T, I>> localOptimization(long updateId,
            Comparator<AllocatableAction<P, T, I>> selectionComparator, Comparator<AllocatableAction<P, T, I>> donorComparator) {

        PriorityQueue<AllocatableAction<P, T, I>> actions = new PriorityQueue<>(1, donorComparator);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction<P, T, I>> readyActions = new PriorityQueue<>(1, getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet<P, T, I> selectableActions = new PriorityActionSet<>(selectionComparator);
        synchronized (gaps) {
            opAction = new OptimizationAction<P, T, I>(orchestrator);
        }
        // No changes in the Gap structure

        // Scan actions: Filters ready and selectable actions
        LinkedList<AllocatableAction<P, T, I>> runningActions = scanActions(readyActions, selectableActions);
        // Gets all the pending schedulings
        LinkedList<AllocatableAction<P, T, I>> newPendingSchedulings = new LinkedList<>();
        LinkedList<AllocatableAction<P, T, I>> pendingSchedulings;
        synchronized (gaps) {
            FullGraphSchedulingInformation<P, T, I> opDSI = (FullGraphSchedulingInformation<P, T, I>) opAction.getSchedulingInfo();
            pendingSchedulings = opDSI.replaceSuccessors(newPendingSchedulings);
        }

        // Classify pending actions: Filters ready and selectable actions
        classifyPendingSchedulings(pendingSchedulings, readyActions, selectableActions, runningActions);
        classifyPendingSchedulings(readyActions, selectableActions, runningActions);
        // ClassifyActions
        LinkedList<Gap<P, T, I>> newGaps = rescheduleTasks(updateId, readyActions, selectableActions, runningActions, actions);

        // Schedules all the pending scheduligns and unblocks the scheduling of new actions
        synchronized (gaps) {
            gaps.clear();
            gaps.addAll(newGaps);
            FullGraphSchedulingInformation<P, T, I> opDSI = (FullGraphSchedulingInformation<P, T, I>) opAction.getSchedulingInfo();
            LinkedList<AllocatableAction<P, T, I>> successors = opDSI.getSuccessors();
            for (AllocatableAction<P, T, I> action : successors) {
                actions.add(action);
                FullGraphSchedulingInformation<P, T, I> actionDSI = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
                actionDSI.lock();
                actionDSI.removePredecessor(opAction);
                if (action != null) {
                    this.scheduleUsingGaps(action, gaps);
                }
                actionDSI.unlock();
            }
            opDSI.clearSuccessors();
            opAction = null;
        }

        return actions;
    }

    // Classifies actions according to their start times. Selectable actions are
    // those that can be selected to run from t=0. Ready actions are those actions
    // that have data dependencies with tasks scheduled in other nodes. Actions
    // with dependencies with actions scheduled in the same node, are not
    // classified in any list since we cannot know the start time.
    public LinkedList<AllocatableAction<P, T, I>> scanActions(PriorityQueue<AllocatableAction<P, T, I>> readyActions,
            PriorityActionSet<P, T, I> selectableActions) {

        LinkedList<AllocatableAction<P, T, I>> runningActions = new LinkedList<>();
        PriorityQueue<AllocatableAction<P, T, I>> actions = new PriorityQueue<>(1, getScanComparator());
        for (Gap<P, T, I> g : gaps) {
            AllocatableAction<P, T, I> gapAction = (AllocatableAction<P, T, I>) g.getOrigin();
            if (gapAction != null) {
                FullGraphSchedulingInformation<P, T, I> dsi = (FullGraphSchedulingInformation<P, T, I>) gapAction.getSchedulingInfo();
                dsi.lock();
                dsi.setOnOptimization(true);
                actions.add(gapAction);
            }
        }
        AllocatableAction<P, T, I> action;
        while ((action = actions.poll()) != null) {
            FullGraphSchedulingInformation<P, T, I> actionDSI = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
            if (!actionDSI.isScheduled()) {
                actionDSI.unlock();
                // Task was already executed. Ignore
                continue;
            }

            // Data Dependencies analysis
            boolean hasInternal = false;
            boolean hasExternal = false;
            long startTime = 0;
            try {
                LinkedList<AllocatableAction<P, T, I>> dPreds = action.getDataPredecessors();
                for (AllocatableAction<P, T, I> dPred : dPreds) {
                    FullGraphSchedulingInformation<P, T, I> dPredDSI = (FullGraphSchedulingInformation<P, T, I>) dPred.getSchedulingInfo();
                    if (dPred.getAssignedResource() == this) {
                        if (dPredDSI.tryToLock()) {
                            if (dPredDSI.isScheduled()) {
                                hasInternal = true;
                                dPredDSI.optimizingSuccessor(action);
                            }
                            dPredDSI.unlock();
                        }
                        // else
                        // The predecessor is trying to be unscheduled but it is
                        // blocked by another successor reschedule.
                    } else {
                        hasExternal = true;
                        startTime = Math.max(startTime, dPredDSI.getExpectedEnd());
                    }
                }
            } catch (ConcurrentModificationException cme) {
                hasInternal = false;
                hasExternal = false;
                startTime = 0;
            }

            // Resource Dependencies analysis
            boolean hasResourcePredecessors = false;
            LinkedList<AllocatableAction<P, T, I>> rPreds = actionDSI.getPredecessors();
            for (AllocatableAction<P, T, I> rPred : rPreds) {
                FullGraphSchedulingInformation<P, T, I> rPredDSI = (FullGraphSchedulingInformation<P, T, I>) rPred.getSchedulingInfo();
                if (rPredDSI.tryToLock()) {
                    if (rPredDSI.isScheduled()) {
                        hasResourcePredecessors = true;
                        if (!rPredDSI.isOnOptimization()) {
                            rPredDSI.setOnOptimization(true);
                            actions.add(rPred);
                        } else {
                            rPredDSI.unlock();
                        }
                    } else {
                        rPredDSI.unlock();
                    }
                }
                // else the predecessor was already executed
            }
            actionDSI.setExpectedStart(startTime);
            actionDSI.setToReschedule(true);
            classifyAction(action, hasInternal, hasExternal, hasResourcePredecessors, startTime, readyActions, selectableActions,
                    runningActions);
            if (hasResourcePredecessors || hasInternal) {
                // The action has a blocked predecessor in the resource that will block its execution
                actionDSI.unlock();
            }
        }
        return runningActions;
    }

    public void classifyPendingSchedulings(LinkedList<AllocatableAction<P, T, I>> pendingSchedulings,
            PriorityQueue<AllocatableAction<P, T, I>> readyActions, PriorityActionSet<P, T, I> selectableActions,
            LinkedList<AllocatableAction<P, T, I>> runningActions) {

        for (AllocatableAction<P, T, I> action : pendingSchedulings) {
            // Action has an artificial resource dependency with the opAction
            FullGraphSchedulingInformation<P, T, I> actionDSI = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
            actionDSI.scheduled();
            actionDSI.setOnOptimization(true);
            actionDSI.setToReschedule(true);
            // Data Dependencies analysis
            boolean hasInternal = false;
            boolean hasExternal = false;
            long startTime = 0;
            try {
                LinkedList<AllocatableAction<P, T, I>> dPreds = action.getDataPredecessors();
                for (AllocatableAction<P, T, I> dPred : dPreds) {
                    FullGraphSchedulingInformation<P, T, I> dPredDSI = (FullGraphSchedulingInformation<P, T, I>) dPred.getSchedulingInfo();
                    if (dPred.getAssignedResource() == this) {
                        if (dPredDSI.tryToLock()) {
                            if (dPredDSI.isScheduled()) {
                                hasInternal = true;
                                dPredDSI.optimizingSuccessor(action);
                            }
                            dPredDSI.unlock();
                        }
                        // else
                        // The predecessor is trying to be unscheduled but it is
                        // blocked by another successor reschedule.
                    } else {
                        hasExternal = true;
                        startTime = Math.max(startTime, dPredDSI.getExpectedEnd());
                    }
                }
            } catch (ConcurrentModificationException cme) {
                hasInternal = false;
                hasExternal = false;
                startTime = 0;
            }

            actionDSI.setExpectedStart(startTime);
            classifyAction(action, hasInternal, hasExternal, true, startTime, readyActions, selectableActions, runningActions);
        }
    }

    public void classifyPendingSchedulings(PriorityQueue<AllocatableAction<P, T, I>> readyActions,
            PriorityActionSet<P, T, I> selectableActions, LinkedList<AllocatableAction<P, T, I>> runningActions) {

        for (AllocatableAction<P, T, I> unscheduledAction : pendingUnschedulings) {
            FullGraphSchedulingInformation<P, T, I> actionDSI = (FullGraphSchedulingInformation<P, T, I>) unscheduledAction
                    .getSchedulingInfo();
            LinkedList<AllocatableAction<P, T, I>> successors = actionDSI.getOptimizingSuccessors();
            for (AllocatableAction<P, T, I> successor : successors) {
                // Data Dependencies analysis
                boolean hasInternal = false;
                boolean hasExternal = false;
                long startTime = 0;
                try {
                    LinkedList<AllocatableAction<P, T, I>> dPreds = successor.getDataPredecessors();
                    for (AllocatableAction<P, T, I> dPred : dPreds) {
                        FullGraphSchedulingInformation<P, T, I> dPredDSI = (FullGraphSchedulingInformation<P, T, I>) dPred
                                .getSchedulingInfo();
                        if (dPred.getAssignedResource() == this) {
                            if (dPredDSI.tryToLock()) {
                                if (dPredDSI.isScheduled()) {
                                    hasInternal = true;
                                    dPredDSI.optimizingSuccessor(successor);
                                }
                                dPredDSI.unlock();
                            }
                            // else
                            // The predecessor is trying to be unscheduled but it is
                            // blocked by another successor reschedule.
                        } else {
                            hasExternal = true;
                            startTime = Math.max(startTime, dPredDSI.getExpectedEnd());
                        }
                    }
                } catch (ConcurrentModificationException cme) {
                    hasInternal = false;
                    hasExternal = false;
                    startTime = 0;
                }

                actionDSI.setExpectedStart(startTime);
                classifyAction(successor, hasInternal, hasExternal, true, startTime, readyActions, selectableActions, runningActions);
            }
        }
        pendingUnschedulings.clear();
    }

    public LinkedList<Gap<P, T, I>> rescheduleTasks(long updateId, PriorityQueue<AllocatableAction<P, T, I>> readyActions,
            PriorityActionSet<P, T, I> selectableActions, LinkedList<AllocatableAction<P, T, I>> runningActions,
            PriorityQueue<AllocatableAction<P, T, I>> rescheduledActions) {
        /*
         * 
         * ReadyActions contains those actions that have no dependencies with other actions scheduled on the node, but
         * they have data dependencies with tasks on other resources. They are sorted by the expected time when these
         * dependencies will be solved.
         * 
         * SelectableActions contains those actions that have no data dependencies with other actions but they wait for
         * resources to be released.
         * 
         * Running actions contains a list of Actions that are executing or potentially executing at the moment.
         * 
         * All Actions that need to be rescheduled have the onOptimization and scheduled flags on.
         * 
         * Those actions that are running or could potentially be started ( no dependencies with other actions in the
         * resource) are already locked to avoid their start without being on the runningActions set.
         */
        LocalOptimizationState<P, T, I> state = new LocalOptimizationState<>(updateId, myWorker.getDescription());

        Gap<P, T, I> gap = state.peekFirstGap();
        ResourceDescription gapResource = gap.getResources();

        PriorityQueue<SchedulingEvent<P, T, I>> schedulingQueue = new PriorityQueue<SchedulingEvent<P, T, I>>();
        // For every running action we create a start event on their real start timeStamp
        for (AllocatableAction<P, T, I> action : runningActions) {
            manageRunningAction(action, state);
            FullGraphSchedulingInformation<P, T, I> actionDSI = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
            schedulingQueue.offer(new SchedulingEvent.End<P, T, I>(actionDSI.getExpectedEnd(), action));
        }
        while (!selectableActions.isEmpty() && !gapResource.isDynamicUseless()) {
            AllocatableAction<P, T, I> top = selectableActions.peek();
            state.replaceAction(top);
            if (state.canActionRun()) {
                selectableActions.poll();
                // Start the current action
                FullGraphSchedulingInformation<P, T, I> topDSI = (FullGraphSchedulingInformation<P, T, I>) top.getSchedulingInfo();
                topDSI.lock();
                topDSI.clearPredecessors();
                manageRunningAction(top, state);
                if (tryToLaunch(top)) {
                    schedulingQueue.offer(new SchedulingEvent.End<P, T, I>(topDSI.getExpectedEnd(), top));
                }
            } else {
                break;
            }
        }

        while (!schedulingQueue.isEmpty() || !readyActions.isEmpty()) {
            // We reschedule as many tasks as possible by processing start and end SchedulingEvents

            while (!schedulingQueue.isEmpty()) {
                SchedulingEvent<P, T, I> e = schedulingQueue.poll();
                /*
                 * Start Event: - sets the expected start and end times - adds resource dependencies with the previous
                 * actions - if there's a gap before the dependency -tries to fill it with other tasks - if all the
                 * resources released by the predecessor are used later - the action is unlocked
                 * 
                 * End Event:
                 */
                LinkedList<SchedulingEvent<P, T, I>> result = e.process(state, this, readyActions, selectableActions, rescheduledActions);
                for (SchedulingEvent<P, T, I> r : result) {
                    schedulingQueue.offer(r);
                }
            }

            if (!readyActions.isEmpty()) {
                AllocatableAction<P, T, I> topAction = readyActions.poll();
                FullGraphSchedulingInformation<P, T, I> topActionDSI = (FullGraphSchedulingInformation<P, T, I>) topAction
                        .getSchedulingInfo();
                topActionDSI.lock();
                topActionDSI.setToReschedule(false);
                schedulingQueue.offer(new SchedulingEvent.Start<P, T, I>(topActionDSI.getExpectedStart(), topAction));
            }
        }

        for (Gap<P, T, I> g : state.getGaps()) {
            state.removeTmpGap(g);
        }

        return state.getGaps();
    }

    private void classifyAction(AllocatableAction<P, T, I> action, boolean hasInternal, boolean hasExternal,
            boolean hasResourcePredecessors, long startTime, PriorityQueue<AllocatableAction<P, T, I>> readyActions,
            PriorityActionSet<P, T, I> selectableActions, LinkedList<AllocatableAction<P, T, I>> runningActions) {

        if (!hasInternal) { // Not needs to wait for some blocked action to end
            if (hasExternal) {
                if (startTime == 0) {
                    selectableActions.offer(action);
                } else {
                    readyActions.add(action);
                }
            } else { // has no dependencies
                if (hasResourcePredecessors) {
                    selectableActions.offer(action);
                } else {
                    runningActions.add(action);
                }
            }
        }
    }

    private void manageRunningAction(AllocatableAction<P, T, I> action, LocalOptimizationState<P, T, I> state) {
        I impl = action.getAssignedImplementation();
        FullGraphSchedulingInformation<P, T, I> actionDSI = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();

        // Set start Time
        Long startTime = action.getStartTime();
        long start;
        if (startTime != null) {
            start = startTime - state.getId();
        } else {
            start = 0;
        }
        actionDSI.setExpectedStart(start);

        // Set End Time
        Profile p = getProfile(impl);
        long endTime = start;
        if (p != null) {
            endTime += p.getAverageExecutionTime();
        }
        if (endTime < 0) {
            endTime = 0;
        }
        actionDSI.setExpectedEnd(endTime);

        actionDSI.clearPredecessors();
        actionDSI.clearSuccessors();
        actionDSI.setToReschedule(false);
        state.reserveResources(impl.getRequirements(), 0);
    }

    private boolean tryToLaunch(AllocatableAction<P, T, I> action) {
        try {
            action.tryToLaunch();
            return true;
        } catch (InvalidSchedulingException ise) {
            LOGGER.error("Exception on tryToLaunch", ise);
            try {
                double actionScore = FullGraphScore.getActionScore(action);
                double dataTime = (new FullGraphScore<P, T, I>(0, 0, 0, 0, 0)).getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new FullGraphScore<P, T, I>(actionScore, dataTime, 0, 0, 0);
                boolean keepTrying = true;
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                    AllocatableAction<P, T, I> pre = action.getConstrainingPredecessors().get(i);
                    action.schedule(pre.getAssignedResource(), aScore);
                    try {
                        action.tryToLaunch();
                        keepTrying = false;
                    } catch (InvalidSchedulingException ise2) {
                        // Try next predecessor
                        keepTrying = true;
                    }
                }

            } catch (BlockedActionException | UnassignedActionException be) {
                // Can not happen since there was an original source
                LOGGER.error("Blocked or unassigned action", ise);
            }
        }
        return false;
    }

    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> Comparator<AllocatableAction<P, T, I>> getScanComparator() {
        return new Comparator<AllocatableAction<P, T, I>>() {

            @Override
            public int compare(AllocatableAction<P, T, I> action1, AllocatableAction<P, T, I> action2) {
                FullGraphSchedulingInformation<P, T, I> action1DSI = (FullGraphSchedulingInformation<P, T, I>) action1.getSchedulingInfo();
                FullGraphSchedulingInformation<P, T, I> action2DSI = (FullGraphSchedulingInformation<P, T, I>) action2.getSchedulingInfo();
                int compare = Long.compare(action2DSI.getExpectedStart(), action1DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action2.getId(), action1.getId());
                }
                return compare;
            }
        };
    }

    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> Comparator<AllocatableAction<P, T, I>> getReadyComparator() {
        return new Comparator<AllocatableAction<P, T, I>>() {

            @Override
            public int compare(AllocatableAction<P, T, I> action1, AllocatableAction<P, T, I> action2) {
                FullGraphSchedulingInformation<P, T, I> action1DSI = (FullGraphSchedulingInformation<P, T, I>) action1.getSchedulingInfo();
                FullGraphSchedulingInformation<P, T, I> action2DSI = (FullGraphSchedulingInformation<P, T, I>) action2.getSchedulingInfo();
                int compare = Long.compare(action1DSI.getExpectedStart(), action2DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                }
                return compare;
            }
        };
    }

    private void addGap(Gap<P, T, I> g) {
        Iterator<Gap<P, T, I>> gapIt = gaps.iterator();
        int index = 0;
        Gap<P, T, I> gap;
        while (gapIt.hasNext() && (gap = gapIt.next()) != null && gap.getInitialTime() <= g.getInitialTime()) {
            index++;
        }
        gaps.add(index, g);
    }

    public long getLastGapExpectedStart() {
        return gaps.peekFirst().getInitialTime();
    }

}
