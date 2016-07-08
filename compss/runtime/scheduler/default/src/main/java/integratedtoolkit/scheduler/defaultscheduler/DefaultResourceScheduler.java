package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.types.Gap;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.DefaultScore;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingEvent;
import integratedtoolkit.types.LocalOptimizationState;
import integratedtoolkit.types.OptimizationAction;
import integratedtoolkit.types.PriorityActionSet;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.Set;

public class DefaultResourceScheduler<P extends Profile, T extends WorkerResourceDescription> extends ResourceScheduler<P, T> {

    public static final long DATA_TRANSFER_DELAY = 200;

    private final LinkedList<Gap> gaps;
    private OptimizationAction opAction;
    private Set<AllocatableAction> pendingUnschedulings = new HashSet<AllocatableAction>();

    public DefaultResourceScheduler(Worker<T> w) {
        super(w);
        gaps = new LinkedList<Gap>();
        addGap(new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, myWorker.getDescription().copy(), 0));
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
    public Score getResourceScore(AllocatableAction<P, T> action, TaskParams params, Score actionScore) {
        long resScore = Score.getLocalityScore(params, myWorker);
        for (AllocatableAction pred : action.getDataPredecessors()) {
            if (pred.isPending() && pred.getAssignedResource() == this) {
                resScore++;
            }
        }
        resScore = params.getParameters().length - resScore;
        long lessTimeStamp = Long.MAX_VALUE;
        Gap g = gaps.peekFirst();
        if (g != null) {
            lessTimeStamp = g.getInitialTime();
            if (lessTimeStamp < 0) {
                lessTimeStamp = 0;
            }
        }
        return new DefaultScore<P, T>((DefaultScore<P, T>) actionScore, resScore * DATA_TRANSFER_DELAY, lessTimeStamp, 0);
    }

    /**
     *
     * @param action
     * @param params
     * @param impl
     * @param resourceScore
     * @return
     */
    public Score getImplementationScore(AllocatableAction<P, T> action, TaskParams params, Implementation<T> impl, Score resourceScore) {
        ResourceDescription rd = impl.getRequirements().copy();
        long resourceFreeTime = 0;
        try {
            for (Gap g : gaps) {
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
        //The data transfer penalty is already included on the datadependency time of the resourceScore
        return new DefaultScore<P, T>((DefaultScore<P, T>) resourceScore, 0, resourceFreeTime, implScore);
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     ---------------- Scheduler Methods ----------------
     ---------------------------------------------------
     --------------------------------------------------*/
    @Override
    public void initialSchedule(AllocatableAction<P, T> action) {
        try {
            synchronized (gaps) {
                if (opAction != null) {
                    ((DefaultSchedulingInformation) opAction.getSchedulingInfo()).addSuccessor(action);
                    ((DefaultSchedulingInformation) action.getSchedulingInfo()).addPredecessor(opAction);
                } else {
                    scheduleUsingGaps(action, gaps);
                }
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    @Override
    public LinkedList<AllocatableAction<P, T>> unscheduleAction(AllocatableAction<P, T> action) {
        LinkedList<AllocatableAction<P, T>> freeTasks = new LinkedList<AllocatableAction<P, T>>();
        DefaultSchedulingInformation<P, T> actionDSI = (DefaultSchedulingInformation<P, T>) action.getSchedulingInfo();
        LinkedList<AllocatableAction> successors = new LinkedList<AllocatableAction>();

        // Create predecessor list
        // Lock access to predecessors
        for (AllocatableAction<P, T> pred : actionDSI.getPredecessors()) {
            DefaultSchedulingInformation<P, T> predDSI = (DefaultSchedulingInformation<P, T>) pred.getSchedulingInfo();
            predDSI.lock();
        }
        // Lock access to the current Action
        actionDSI.lock();
        //Remove action from predecessors
        for (AllocatableAction<P, T> pred : actionDSI.getPredecessors()) {
            DefaultSchedulingInformation<P, T> predDSI = (DefaultSchedulingInformation<P, T>) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        //Create successor list
        //lock access to successors
        for (AllocatableAction<P, T> successor : actionDSI.getSuccessors()) {
            DefaultSchedulingInformation<P, T> succDSI = (DefaultSchedulingInformation<P, T>) successor.getSchedulingInfo();
            succDSI.lock();
            successors.add(successor);
        }

        for (AllocatableAction<P, T> successor : actionDSI.getSuccessors()) {
            DefaultSchedulingInformation<P, T> successorDSI = (DefaultSchedulingInformation<P, T>) successor.getSchedulingInfo();
            //Remove predecessor
            successorDSI.removePredecessor(action);
            //Link with action predecessors
            for (AllocatableAction<P, T> predecessor : actionDSI.getPredecessors()) {
                DefaultSchedulingInformation<P, T> predDSI = (DefaultSchedulingInformation<P, T>) predecessor.getSchedulingInfo();
                if (predDSI.isScheduled()) {
                    successorDSI.addPredecessor(predecessor);
                    predDSI.addSuccessor(successor);
                }
            }
            //Check executability
            if (successorDSI.isExecutable()) {
                freeTasks.add(successor);
            }
        }

        //Unlock access to predecessors
        for (AllocatableAction<P, T> pred : actionDSI.getPredecessors()) {
            DefaultSchedulingInformation<P, T> predDSI = (DefaultSchedulingInformation<P, T>) pred.getSchedulingInfo();
            predDSI.unlock();
        }

        //Clear action predecessors and successors
        actionDSI.unscheduled();

        if (actionDSI.isOnOptimization()) {
            synchronized (gaps) {
                pendingUnschedulings.add(action);
            }
        }
        //Unlock access to current action
        actionDSI.unlock();

        //Unlock access to successors
        for (AllocatableAction<P, T> successor : successors) {
            DefaultSchedulingInformation<P, T> successorDSI = (DefaultSchedulingInformation<P, T>) successor.getSchedulingInfo();
            successorDSI.unlock();
        }
        return freeTasks;
    }

    @Override
    public P generateProfileForAllocatable() {
        return (P) new Profile();
    }

    @Override
    public void clear() {
        super.clear();
        gaps.clear();
        addGap(new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, myWorker.getDescription().copy(), 0));
    }

    private void scheduleUsingGaps(AllocatableAction<P, T> action, LinkedList<Gap> gaps) {
        long expectedStart = 0;
        // Compute start time due to data dependencies
        for (AllocatableAction predecessor : action.getDataPredecessors()) {
            DefaultSchedulingInformation<P, T> predDSI = ((DefaultSchedulingInformation<P, T>) predecessor.getSchedulingInfo());
            if (predDSI.isScheduled()) {
                long predEnd = predDSI.getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
            }
        }
        DefaultSchedulingInformation<P, T> schedInfo = (DefaultSchedulingInformation<P, T>) action.getSchedulingInfo();
        Implementation<T> impl = action.getAssignedImplementation();
        Profile p = getProfile(impl);
        ResourceDescription constraints = impl.getRequirements().copy();
        LinkedList<AllocatableAction> predecessors = new LinkedList();

        Iterator<Gap> gapIt = gaps.descendingIterator();
        boolean fullyCoveredReqs = false;
        // Compute predecessors and update gaps
        // Check gaps before data start
        while (gapIt.hasNext() && !fullyCoveredReqs) {
            Gap gap = gapIt.next();
            if (gap.getInitialTime() <= expectedStart) {
                AllocatableAction<P, T> predecessor = (AllocatableAction<P, T>) gap.getOrigin();
                if (predecessor != null) {
                    DefaultSchedulingInformation<P, T> predDSI = ((DefaultSchedulingInformation<P, T>) predecessor.getSchedulingInfo());
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
            Gap gap = gapIt.next();
            AllocatableAction<P, T> predecessor = (AllocatableAction<P, T>) gap.getOrigin();
            if (predecessor != null) {
                DefaultSchedulingInformation<P, T> predDSI = ((DefaultSchedulingInformation<P, T>) predecessor.getSchedulingInfo());
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
        for (AllocatableAction predecessor : predecessors) {
            DefaultSchedulingInformation<P, T> predDSI = ((DefaultSchedulingInformation<P, T>) predecessor.getSchedulingInfo());
            if (predDSI.isScheduled()) {
                long predEnd = predDSI.getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
                schedInfo.addPredecessor(predecessor);
                predDSI.addSuccessor(action);
            }
            predDSI.unlock();
        }

        //Compute end time
        schedInfo.setExpectedStart(expectedStart);
        long expectedEnd = expectedStart;
        if (p != null) {
            expectedEnd += p.getAverageExecutionTime();
        }
        schedInfo.setExpectedEnd(expectedEnd);

        //Unlock access to current task
        schedInfo.unlock();

        //Create new Gap correspondin to the resources released by the action
        addGap(new Gap(expectedEnd, Long.MAX_VALUE, action, impl.getRequirements().copy(), 0));
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     -------------- Optimization Methods ---------------
     ---------------------------------------------------
     --------------------------------------------------*/
    public PriorityQueue<AllocatableAction> localOptimization(
            long updateId,
            Comparator<AllocatableAction> selectionComparator,
            Comparator<AllocatableAction> donorComparator
    ) {
        PriorityQueue<AllocatableAction> actions = new PriorityQueue<AllocatableAction>(1, donorComparator);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<AllocatableAction>(1, getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(selectionComparator);
        synchronized (gaps) {
            opAction = new OptimizationAction();
        }
        //No changes in the Gap structure

        //Scan actions: Filters ready and selectable actions
        LinkedList<AllocatableAction> runningActions = scanActions(readyActions, selectableActions);
        //Gets all the pending schedulings
        LinkedList<AllocatableAction> newPendingSchedulings = new LinkedList();
        LinkedList<AllocatableAction> pendingSchedulings;
        synchronized (gaps) {
            DefaultSchedulingInformation opDSI = (DefaultSchedulingInformation) opAction.getSchedulingInfo();
            pendingSchedulings = opDSI.replaceSuccessors(newPendingSchedulings);
        }

        //Classify pending actions: Filters ready and selectable actions
        classifyPendingSchedulings(pendingSchedulings, readyActions, selectableActions, runningActions);
        classifyPendingSchedulings(readyActions, selectableActions, runningActions);
        //ClassifyActions
        LinkedList<Gap> newGaps = rescheduleTasks(updateId, readyActions, selectableActions, runningActions, actions);

        //Schedules all the pending scheduligns and unblocks the scheduling of new actions
        synchronized (gaps) {
            gaps.clear();
            gaps.addAll(newGaps);
            DefaultSchedulingInformation opDSI = (DefaultSchedulingInformation) opAction.getSchedulingInfo();
            LinkedList<AllocatableAction> successors = opDSI.getSuccessors();
            for (AllocatableAction action : successors) {
                actions.add(action);
                DefaultSchedulingInformation actionDSI = (DefaultSchedulingInformation) action.getSchedulingInfo();
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
    public LinkedList<AllocatableAction> scanActions(
            PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions) {
        LinkedList<AllocatableAction> runningActions = new LinkedList<AllocatableAction>();
        PriorityQueue<AllocatableAction> actions = new PriorityQueue<AllocatableAction>(1, getScanComparator());
        for (Gap g : gaps) {
            AllocatableAction gapAction = g.getOrigin();
            if (gapAction != null) {
                DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) gapAction.getSchedulingInfo();
                dsi.lock();
                dsi.setOnOptimization(true);
                actions.add(gapAction);
            }
        }
        AllocatableAction action;
        while ((action = actions.poll()) != null) {
            DefaultSchedulingInformation actionDSI = (DefaultSchedulingInformation) action.getSchedulingInfo();
            if (!actionDSI.isScheduled()) {
                actionDSI.unlock();
                //Task was already executed. Ignore
                continue;
            }

            //Data Dependencies analysis
            boolean hasInternal = false;
            boolean hasExternal = false;
            long startTime = 0;
            try {
                LinkedList<AllocatableAction> dPreds = action.getDataPredecessors();
                for (AllocatableAction dPred : dPreds) {
                    DefaultSchedulingInformation dPredDSI = (DefaultSchedulingInformation) dPred.getSchedulingInfo();
                    if (dPred.getAssignedResource() == this) {
                        if (dPredDSI.tryToLock()) {
                            if (dPredDSI.isScheduled()) {
                                hasInternal = true;
                                dPredDSI.optimizingSuccessor(action);
                            }
                            dPredDSI.unlock();
                        }
                        //else 
                        //The predecessor is trying to be unscheduled but it is
                        //blocked by another successor reschedule. 
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

            //Resource Dependencies analysis
            boolean hasResourcePredecessors = false;
            LinkedList<AllocatableAction> rPreds = actionDSI.getPredecessors();
            for (AllocatableAction rPred : rPreds) {
                DefaultSchedulingInformation rPredDSI = (DefaultSchedulingInformation) rPred.getSchedulingInfo();
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
                //else the predecessor was already executed
            }
            actionDSI.setExpectedStart(startTime);
            actionDSI.setToReschedule(true);
            classifyAction(action, hasInternal, hasExternal, hasResourcePredecessors, startTime, readyActions, selectableActions, runningActions);
            if (hasResourcePredecessors || hasInternal) {
                //The action has a blocked predecessor in the resource that will block its execution
                actionDSI.unlock();
            }
        }
        return runningActions;
    }

    public void classifyPendingSchedulings(
            LinkedList<AllocatableAction> pendingSchedulings,
            PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions,
            LinkedList<AllocatableAction> runningActions
    ) {

        for (AllocatableAction action : pendingSchedulings) {
            //Action has an artificial resource dependency with the opAction
            DefaultSchedulingInformation actionDSI = (DefaultSchedulingInformation) action.getSchedulingInfo();
            actionDSI.scheduled();
            actionDSI.setOnOptimization(true);
            actionDSI.setToReschedule(true);
            //Data Dependencies analysis
            boolean hasInternal = false;
            boolean hasExternal = false;
            long startTime = 0;
            try {
                LinkedList<AllocatableAction> dPreds = action.getDataPredecessors();
                for (AllocatableAction dPred : dPreds) {
                    DefaultSchedulingInformation dPredDSI = (DefaultSchedulingInformation) dPred.getSchedulingInfo();
                    if (dPred.getAssignedResource() == this) {
                        if (dPredDSI.tryToLock()) {
                            if (dPredDSI.isScheduled()) {
                                hasInternal = true;
                                dPredDSI.optimizingSuccessor(action);
                            }
                            dPredDSI.unlock();
                        }
                        //else 
                        //The predecessor is trying to be unscheduled but it is
                        //blocked by another successor reschedule. 
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

    public void classifyPendingSchedulings(
            PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions,
            LinkedList<AllocatableAction> runningActions
    ) {
        for (AllocatableAction unscheduledAction : pendingUnschedulings) {
            DefaultSchedulingInformation actionDSI = (DefaultSchedulingInformation) unscheduledAction.getSchedulingInfo();
            LinkedList<AllocatableAction> successors = actionDSI.getOptimizingSuccessors();
            for (AllocatableAction successor : successors) {
                //Data Dependencies analysis
                boolean hasInternal = false;
                boolean hasExternal = false;
                long startTime = 0;
                try {
                    LinkedList<AllocatableAction> dPreds = successor.getDataPredecessors();
                    for (AllocatableAction dPred : dPreds) {
                        DefaultSchedulingInformation dPredDSI = (DefaultSchedulingInformation) dPred.getSchedulingInfo();
                        if (dPred.getAssignedResource() == this) {
                            if (dPredDSI.tryToLock()) {
                                if (dPredDSI.isScheduled()) {
                                    hasInternal = true;
                                    dPredDSI.optimizingSuccessor(successor);
                                }
                                dPredDSI.unlock();
                            }
                            //else 
                            //The predecessor is trying to be unscheduled but it is
                            //blocked by another successor reschedule. 
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

    public LinkedList<Gap> rescheduleTasks(
            long updateId,
            PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions,
            LinkedList<AllocatableAction> runningActions,
            PriorityQueue<AllocatableAction> rescheduledActions
    ) {
        /*
         * 
         * ReadyActions contains those actions that have no dependencies with
         * other actions scheduled on the node, but they have data dependencies
         * with tasks on other resources. They are sorted by the expected time 
         * when these dependencies will be solved.
         *
         * SelectableActions contains those actions that have no data dependencies
         * with other actions but they wait for resources to be released.
         * 
         * Running actions contains a list of Actions that are executing or 
         * potentially executing at the moment.
         * 
         * All Actions that need to be rescheduled have the onOptimization and
         * scheduled flags on.
         * 
         * Those actions that are running or could potentially be started ( no 
         * dependencies with other actions in the resource) are already locked 
         * to avoid their start without being on the runningActions set.
         */
        LocalOptimizationState state = new LocalOptimizationState(updateId, myWorker.getDescription());

        Gap gap = state.peekFirstGap();
        ResourceDescription gapResource = gap.getResources();

        PriorityQueue<SchedulingEvent<P, T>> schedulingQueue = new PriorityQueue<SchedulingEvent<P, T>>();
        //For every running action we create a start event on their real start timeStamp
        for (AllocatableAction action : runningActions) {
            manageRunningAction(action, state);
            DefaultSchedulingInformation actionDSI = (DefaultSchedulingInformation) action.getSchedulingInfo();
            schedulingQueue.offer(new SchedulingEvent.End<P, T>(actionDSI.getExpectedEnd(), action));
        }
        while (!selectableActions.isEmpty() && !gapResource.isDynamicUseless()) {
            AllocatableAction top = selectableActions.peek();
            state.replaceAction(top);
            if (state.canActionRun()) {
                selectableActions.poll();
                //Start the current action
                DefaultSchedulingInformation topDSI = (DefaultSchedulingInformation) top.getSchedulingInfo();
                topDSI.lock();
                topDSI.clearPredecessors();
                manageRunningAction(top, state);
                if (tryToLaunch(top)) {
                    schedulingQueue.offer(new SchedulingEvent.End<P, T>(topDSI.getExpectedEnd(), top));
                }
            } else {
                break;
            }
        }

        while (!schedulingQueue.isEmpty() || !readyActions.isEmpty()) {
            // We reschedule as many tasks as possible by processing start and end SchedulingEvents

            while (!schedulingQueue.isEmpty()) {
                SchedulingEvent<P, T> e = schedulingQueue.poll();
                /*
                 * Start Event:
                 *  - sets the expected start and end times
                 *  - adds resource dependencies with the previous actions
                 *  - if there's a gap before the dependency 
                 *      -tries to fill it with other tasks
                 *  - if all the resources released by the predecessor are used later
                 *      - the action is unlocked
                 *
                 * End Event:
                 * 
                 */
                LinkedList<SchedulingEvent<P, T>> result = e.process(state, this, readyActions, selectableActions, rescheduledActions);
                for (SchedulingEvent<P, T> r : result) {
                    schedulingQueue.offer(r);
                }
            }

            if (!readyActions.isEmpty()) {
                AllocatableAction topAction = readyActions.poll();
                DefaultSchedulingInformation topActionDSI = (DefaultSchedulingInformation) topAction.getSchedulingInfo();
                topActionDSI.lock();
                topActionDSI.setToReschedule(false);
                schedulingQueue.offer(new SchedulingEvent.Start<P, T>(topActionDSI.getExpectedStart(), topAction));
            }
        }

        for (Gap g : state.getGaps()) {
            state.removeTmpGap(g);
        }

        return state.getGaps();
    }

    private void classifyAction(
            AllocatableAction action,
            boolean hasInternal,
            boolean hasExternal,
            boolean hasResourcePredecessors,
            long startTime,
            PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions,
            LinkedList<AllocatableAction> runningActions
    ) {
        if (!hasInternal) { //Not needs to wait for some blocked action to end
            if (hasExternal) {
                if (startTime == 0) {
                    selectableActions.offer(action);
                } else {
                    readyActions.add(action);
                }
            } else { //has no dependencies
                if (hasResourcePredecessors) {
                    selectableActions.offer(action);
                } else {
                    runningActions.add(action);
                }
            }
        }
    }

    private void manageRunningAction(
            AllocatableAction action,
            LocalOptimizationState state
    ) {
        Implementation impl = action.getAssignedImplementation();
        DefaultSchedulingInformation actionDSI = (DefaultSchedulingInformation) action.getSchedulingInfo();

        //Set start Time
        Long startTime = action.getStartTime();
        long start;
        if (startTime != null) {
            start = startTime - state.getId();
        } else {
            start = 0;
        }
        actionDSI.setExpectedStart(start);

        //Set End  Time
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

    private boolean tryToLaunch(AllocatableAction action) {
        try {
            action.tryToLaunch();
            return true;
        } catch (InvalidSchedulingException ise) {
            ise.printStackTrace();
            try {
                long actionScore = DefaultScore.getActionScore(action);
                long dataTime = (new DefaultScore(0, 0, 0, 0)).getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new DefaultScore(actionScore, dataTime, 0, 0);
                action.schedule(action.getConstrainingPredecessor().getAssignedResource(), aScore);
                try {
                    action.tryToLaunch();
                } catch (InvalidSchedulingException ise2) {
                    //Impossible exception. 
                    ise2.printStackTrace();
                }
            } catch (BlockedActionException | UnassignedActionException be) {
                //Can not happen since there was an original source
                be.printStackTrace();
            }
        }
        return false;
    }

    public static final Comparator getScanComparator() {
        return new Comparator<AllocatableAction>() {
            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                DefaultSchedulingInformation action1DSI = (DefaultSchedulingInformation) action1.getSchedulingInfo();
                DefaultSchedulingInformation action2DSI = (DefaultSchedulingInformation) action2.getSchedulingInfo();
                int compare = Long.compare(action2DSI.getExpectedStart(), action1DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action2.getId(), action1.getId());
                }
                return compare;
            }
        };
    }

    public static final Comparator getReadyComparator() {
        return new Comparator<AllocatableAction>() {
            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                DefaultSchedulingInformation action1DSI = (DefaultSchedulingInformation) action1.getSchedulingInfo();
                DefaultSchedulingInformation action2DSI = (DefaultSchedulingInformation) action2.getSchedulingInfo();
                int compare = Long.compare(action1DSI.getExpectedStart(), action2DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                }
                return compare;
            }
        };
    }

    private void addGap(Gap g) {
        Iterator<Gap> gapIt = gaps.iterator();
        int index = 0;
        Gap gap;
        while (gapIt.hasNext()
                && (gap = gapIt.next()) != null
                && gap.getInitialTime() <= g.getInitialTime()) {
            index++;
        }
        gaps.add(index, g);
    }

    public long getLastGapExpectedStart() {
        return gaps.peekFirst().getInitialTime();
    }
}
