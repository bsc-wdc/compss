/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.fullGraphScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.FullGraphScore;
import es.bsc.compss.scheduler.types.Gap;
import es.bsc.compss.scheduler.types.LocalOptimizationState;
import es.bsc.compss.scheduler.types.OptimizationAction;
import es.bsc.compss.scheduler.types.PriorityActionSet;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.scheduler.types.SchedulingEvent;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers the full task graph
 *
 * @param <P>
 * @param <T>
 * @param <I>
 */
public class FullGraphResourceScheduler<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    public static final long DATA_TRANSFER_DELAY = 200L;

    private final ActionOrchestrator orchestrator;
    private final LinkedList<Gap> gaps;
    private final Set<AllocatableAction> pendingUnschedulings;

    private OptimizationAction opAction;


    public FullGraphResourceScheduler(Worker<T> w, JSONObject defaultResource, JSONObject defaultImplementations,
            ActionOrchestrator orchestrator) {

        super(w, defaultResource, defaultImplementations);

        this.orchestrator = orchestrator;
        this.pendingUnschedulings = new HashSet<>();
        this.gaps = new LinkedList<>();
        addGap(new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, myWorker.getDescription().copy(), 0));
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     ------------------ Score Methods ------------------
     ---------------------------------------------------
     --------------------------------------------------*/
    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        LOGGER.debug("[FullGraphScheduler] Generate resource score for action " + action.getId());

        double resScore = Score.calculateDataLocalityScore(params, this.myWorker);
        for (AllocatableAction pred : action.getDataPredecessors()) {
            if (pred.isPending() && pred.getAssignedResource() == this) {
                resScore++;
            }
        }
        resScore = params.getParameters().size() - resScore;
        long lessTimeStamp = Long.MAX_VALUE;
        Gap g = this.gaps.peekFirst();
        if (g != null) {
            lessTimeStamp = g.getInitialTime();
            if (lessTimeStamp < 0) {
                lessTimeStamp = 0;
            }
        }
        return new FullGraphScore((FullGraphScore) actionScore, resScore * DATA_TRANSFER_DELAY, 0, lessTimeStamp, 0);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
            Score resourceScore) {
        LOGGER.debug("[FullGraphScheduler] Generate implementation score for action " + action.getId());

        ResourceDescription rd = impl.getRequirements().copy();
        long resourceFreeTime = 0;
        try {
            for (Gap g : this.gaps) {
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
        return new FullGraphScore((FullGraphScore) resourceScore, 0, 0, resourceFreeTime, implScore);
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     ---------------- Scheduler Methods ----------------
     ---------------------------------------------------
     --------------------------------------------------*/
    @Override
    public void scheduleAction(AllocatableAction action) {
        try {
            synchronized (this.gaps) {
                if (this.opAction != null) {
                    ((FullGraphSchedulingInformation) this.opAction.getSchedulingInfo()).addSuccessor(action);
                    ((FullGraphSchedulingInformation) action.getSchedulingInfo()).addPredecessor(this.opAction);
                } else {
                    scheduleUsingGaps(action, this.gaps);
                }
            }
        } catch (Exception e) {
            LOGGER.error("Exception on initial schedule", e);
        }
    }

    @Override
    public LinkedList<AllocatableAction> unscheduleAction(AllocatableAction action) {
        LinkedList<AllocatableAction> freeTasks = new LinkedList<>();
        FullGraphSchedulingInformation actionDSI = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        LinkedList<AllocatableAction> successors = new LinkedList<>();

        // Create predecessor list
        // Lock access to predecessors
        for (AllocatableAction pred : actionDSI.getPredecessors()) {
            FullGraphSchedulingInformation predDSI = (FullGraphSchedulingInformation) pred.getSchedulingInfo();
            predDSI.lock();
        }
        // Lock access to the current Action
        actionDSI.lock();
        // Remove action from predecessors
        for (AllocatableAction pred : actionDSI.getPredecessors()) {
            FullGraphSchedulingInformation predDSI = (FullGraphSchedulingInformation) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        // Create successor list
        // lock access to successors
        for (AllocatableAction successor : actionDSI.getSuccessors()) {
            FullGraphSchedulingInformation succDSI = (FullGraphSchedulingInformation) successor.getSchedulingInfo();
            succDSI.lock();
            successors.add(successor);
        }

        for (AllocatableAction successor : actionDSI.getSuccessors()) {
            FullGraphSchedulingInformation successorDSI = (FullGraphSchedulingInformation) successor
                    .getSchedulingInfo();
            // Remove predecessor
            successorDSI.removePredecessor(action);
            // Link with action predecessors
            for (AllocatableAction predecessor : actionDSI.getPredecessors()) {
                FullGraphSchedulingInformation predDSI = (FullGraphSchedulingInformation) predecessor
                        .getSchedulingInfo();
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
        for (AllocatableAction pred : actionDSI.getPredecessors()) {
            FullGraphSchedulingInformation predDSI = (FullGraphSchedulingInformation) pred.getSchedulingInfo();
            predDSI.unlock();
        }

        // Clear action predecessors and successors
        actionDSI.unscheduled();

        if (actionDSI.isOnOptimization()) {
            synchronized (this.gaps) {
                this.pendingUnschedulings.add(action);
            }
        }
        // Unlock access to current action
        actionDSI.unlock();

        // Unlock access to successors
        for (AllocatableAction successor : successors) {
            FullGraphSchedulingInformation successorDSI = (FullGraphSchedulingInformation) successor
                    .getSchedulingInfo();
            successorDSI.unlock();
        }
        return freeTasks;
    }

    @Override
    public void clear() {
        super.clear();
        this.gaps.clear();
        addGap(new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, this.myWorker.getDescription().copy(), 0));
    }

    private void scheduleUsingGaps(AllocatableAction action, LinkedList<Gap> gaps) {
        long expectedStart = 0;
        // Compute start time due to data dependencies
        for (AllocatableAction predecessor : action.getDataPredecessors()) {
            FullGraphSchedulingInformation predDSI = ((FullGraphSchedulingInformation) predecessor.getSchedulingInfo());
            if (predDSI.isScheduled()) {
                long predEnd = predDSI.getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
            }
        }
        FullGraphSchedulingInformation schedInfo = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        Implementation impl = action.getAssignedImplementation();
        Profile p = getProfile(impl);
        ResourceDescription constraints = impl.getRequirements().copy();
        LinkedList<AllocatableAction> predecessors = new LinkedList<>();

        Iterator<Gap> gapIt = gaps.descendingIterator();
        boolean fullyCoveredReqs = false;
        // Compute predecessors and update gaps
        // Check gaps before data start
        while (gapIt.hasNext() && !fullyCoveredReqs) {
            Gap gap = gapIt.next();
            if (gap.getInitialTime() <= expectedStart) {
                AllocatableAction predecessor = (AllocatableAction) gap.getOrigin();
                if (predecessor != null) {
                    FullGraphSchedulingInformation predDSI = ((FullGraphSchedulingInformation) predecessor
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
            Gap gap = gapIt.next();
            AllocatableAction predecessor = (AllocatableAction) gap.getOrigin();
            if (predecessor != null) {
                FullGraphSchedulingInformation predDSI = ((FullGraphSchedulingInformation) predecessor
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

        // Lock access to the current task
        schedInfo.lock();
        schedInfo.scheduled();

        // Add dependencies
        // Unlock access to predecessor
        for (AllocatableAction predecessor : predecessors) {
            FullGraphSchedulingInformation predDSI = ((FullGraphSchedulingInformation) predecessor.getSchedulingInfo());
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

        // Create new Gap corresponding to the resources released by the action
        addGap(new Gap(expectedEnd, Long.MAX_VALUE, action, impl.getRequirements().copy(), 0));
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     -------------- Optimization Methods ---------------
     ---------------------------------------------------
     --------------------------------------------------*/
    public PriorityQueue<AllocatableAction> localOptimization(long updateId,
            Comparator<AllocatableAction> selectionComparator, Comparator<AllocatableAction> donorComparator) {

        PriorityQueue<AllocatableAction> actions = new PriorityQueue<>(1, donorComparator);

        // Actions not depending on other actions scheduled on the same resource
        // Sorted by data dependencies release
        PriorityQueue<AllocatableAction> readyActions = new PriorityQueue<>(1, getReadyComparator());

        // Actions that can be selected to be scheduled on the node
        // Sorted by data dependencies release
        PriorityActionSet selectableActions = new PriorityActionSet(selectionComparator);
        synchronized (this.gaps) {
            this.opAction = new OptimizationAction(this.orchestrator);
        }
        // No changes in the Gap structure

        // Scan actions: Filters ready and selectable actions
        LinkedList<AllocatableAction> runningActions = scanActions(readyActions, selectableActions);
        // Gets all the pending schedulings
        LinkedList<AllocatableAction> newPendingSchedulings = new LinkedList<>();
        LinkedList<AllocatableAction> pendingSchedulings;
        synchronized (this.gaps) {
            FullGraphSchedulingInformation opDSI = (FullGraphSchedulingInformation) this.opAction.getSchedulingInfo();
            pendingSchedulings = opDSI.replaceSuccessors(newPendingSchedulings);
        }

        // Classify pending actions: Filters ready and selectable actions
        classifyPendingSchedulings(pendingSchedulings, readyActions, selectableActions, runningActions);
        classifyPendingSchedulings(readyActions, selectableActions, runningActions);
        // ClassifyActions
        LinkedList<Gap> newGaps = rescheduleTasks(updateId, readyActions, selectableActions, runningActions, actions);

        // Schedules all the pending scheduligns and unblocks the scheduling of new actions
        synchronized (this.gaps) {
            this.gaps.clear();
            this.gaps.addAll(newGaps);
            FullGraphSchedulingInformation opDSI = (FullGraphSchedulingInformation) this.opAction.getSchedulingInfo();
            LinkedList<AllocatableAction> successors = opDSI.getSuccessors();
            for (AllocatableAction action : successors) {
                actions.add(action);
                FullGraphSchedulingInformation actionDSI = (FullGraphSchedulingInformation) action.getSchedulingInfo();
                actionDSI.lock();
                actionDSI.removePredecessor(this.opAction);
                if (action != null) {
                    this.scheduleUsingGaps(action, this.gaps);
                }
                actionDSI.unlock();
            }
            opDSI.clearSuccessors();
            this.opAction = null;
        }

        return actions;
    }

    // Classifies actions according to their start times. Selectable actions are
    // those that can be selected to run from t=0. Ready actions are those actions
    // that have data dependencies with tasks scheduled in other nodes. Actions
    // with dependencies with actions scheduled in the same node, are not
    // classified in any list since we cannot know the start time.
    public LinkedList<AllocatableAction> scanActions(PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions) {

        LinkedList<AllocatableAction> runningActions = new LinkedList<>();
        PriorityQueue<AllocatableAction> actions = new PriorityQueue<>(1, getScanComparator());
        for (Gap g : this.gaps) {
            AllocatableAction gapAction = (AllocatableAction) g.getOrigin();
            if (gapAction != null) {
                FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) gapAction.getSchedulingInfo();
                dsi.lock();
                dsi.setOnOptimization(true);
                actions.add(gapAction);
            }
        }
        AllocatableAction action;
        while ((action = actions.poll()) != null) {
            FullGraphSchedulingInformation actionDSI = (FullGraphSchedulingInformation) action.getSchedulingInfo();
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
                List<AllocatableAction> dPreds = action.getDataPredecessors();
                for (AllocatableAction dPred : dPreds) {
                    FullGraphSchedulingInformation dPredDSI = (FullGraphSchedulingInformation) dPred
                            .getSchedulingInfo();
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
            LinkedList<AllocatableAction> rPreds = actionDSI.getPredecessors();
            for (AllocatableAction rPred : rPreds) {
                FullGraphSchedulingInformation rPredDSI = (FullGraphSchedulingInformation) rPred.getSchedulingInfo();
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
            classifyAction(action, hasInternal, hasExternal, hasResourcePredecessors, startTime, readyActions,
                    selectableActions, runningActions);
            if (hasResourcePredecessors || hasInternal) {
                // The action has a blocked predecessor in the resource that will block its execution
                actionDSI.unlock();
            }
        }
        return runningActions;
    }

    public void classifyPendingSchedulings(LinkedList<AllocatableAction> pendingSchedulings,
            PriorityQueue<AllocatableAction> readyActions, PriorityActionSet selectableActions,
            LinkedList<AllocatableAction> runningActions) {

        for (AllocatableAction action : pendingSchedulings) {
            // Action has an artificial resource dependency with the opAction
            FullGraphSchedulingInformation actionDSI = (FullGraphSchedulingInformation) action.getSchedulingInfo();
            actionDSI.scheduled();
            actionDSI.setOnOptimization(true);
            actionDSI.setToReschedule(true);
            // Data Dependencies analysis
            boolean hasInternal = false;
            boolean hasExternal = false;
            long startTime = 0;
            try {
                List<AllocatableAction> dPreds = action.getDataPredecessors();
                for (AllocatableAction dPred : dPreds) {
                    FullGraphSchedulingInformation dPredDSI = (FullGraphSchedulingInformation) dPred
                            .getSchedulingInfo();
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
            classifyAction(action, hasInternal, hasExternal, true, startTime, readyActions, selectableActions,
                    runningActions);
        }
    }

    public void classifyPendingSchedulings(PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions, LinkedList<AllocatableAction> runningActions) {

        for (AllocatableAction unscheduledAction : this.pendingUnschedulings) {
            FullGraphSchedulingInformation actionDSI = (FullGraphSchedulingInformation) unscheduledAction
                    .getSchedulingInfo();
            LinkedList<AllocatableAction> successors = actionDSI.getOptimizingSuccessors();
            for (AllocatableAction successor : successors) {
                // Data Dependencies analysis
                boolean hasInternal = false;
                boolean hasExternal = false;
                long startTime = 0;
                try {
                    List<AllocatableAction> dPreds = successor.getDataPredecessors();
                    for (AllocatableAction dPred : dPreds) {
                        FullGraphSchedulingInformation dPredDSI = (FullGraphSchedulingInformation) dPred
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
                classifyAction(successor, hasInternal, hasExternal, true, startTime, readyActions, selectableActions,
                        runningActions);
            }
        }
        this.pendingUnschedulings.clear();
    }

    public LinkedList<Gap> rescheduleTasks(long updateId, PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions, LinkedList<AllocatableAction> runningActions,
            PriorityQueue<AllocatableAction> rescheduledActions) {
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
        LocalOptimizationState state = new LocalOptimizationState(updateId, this.myWorker.getDescription());

        Gap gap = state.peekFirstGap();
        ResourceDescription gapResource = gap.getResources();

        PriorityQueue<SchedulingEvent<T>> schedulingQueue = new PriorityQueue<>();
        // For every running action we create a start event on their real start timeStamp
        for (AllocatableAction action : runningActions) {
            manageRunningAction(action, state);
            FullGraphSchedulingInformation actionDSI = (FullGraphSchedulingInformation) action.getSchedulingInfo();
            schedulingQueue.offer(new SchedulingEvent.End<T>(actionDSI.getExpectedEnd(), action));
        }
        while (!selectableActions.isEmpty() && !gapResource.isDynamicUseless()) {
            AllocatableAction top = selectableActions.peek();
            state.replaceAction(top);
            if (state.canActionRun()) {
                selectableActions.poll();
                // Start the current action
                FullGraphSchedulingInformation topDSI = (FullGraphSchedulingInformation) top.getSchedulingInfo();
                topDSI.lock();
                topDSI.clearPredecessors();
                manageRunningAction(top, state);
                if (tryToLaunch(top)) {
                    schedulingQueue.offer(new SchedulingEvent.End<T>(topDSI.getExpectedEnd(), top));
                }
            } else {
                break;
            }
        }

        while (!schedulingQueue.isEmpty() || !readyActions.isEmpty()) {
            // We reschedule as many tasks as possible by processing start and end SchedulingEvents

            while (!schedulingQueue.isEmpty()) {
                SchedulingEvent<T> e = schedulingQueue.poll();
                /*
                 * Start Event: - sets the expected start and end times - adds resource dependencies with the previous
                 * actions - if there's a gap before the dependency -tries to fill it with other tasks - if all the
                 * resources released by the predecessor are used later - the action is unlocked
                 * 
                 * End Event:
                 */
                List<SchedulingEvent<T>> result = e.process(state, this, readyActions, selectableActions,
                        rescheduledActions);
                for (SchedulingEvent<T> r : result) {
                    schedulingQueue.offer(r);
                }
            }

            if (!readyActions.isEmpty()) {
                AllocatableAction topAction = readyActions.poll();
                FullGraphSchedulingInformation topActionDSI = (FullGraphSchedulingInformation) topAction
                        .getSchedulingInfo();
                topActionDSI.lock();
                topActionDSI.setToReschedule(false);
                schedulingQueue.offer(new SchedulingEvent.Start<T>(topActionDSI.getExpectedStart(), topAction));
            }
        }

        for (Gap g : state.getGaps()) {
            state.removeTmpGap(g);
        }

        return state.getGaps();
    }

    private void classifyAction(AllocatableAction action, boolean hasInternal, boolean hasExternal,
            boolean hasResourcePredecessors, long startTime, PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions, LinkedList<AllocatableAction> runningActions) {

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

    private void manageRunningAction(AllocatableAction action, LocalOptimizationState state) {
        Implementation impl = action.getAssignedImplementation();
        FullGraphSchedulingInformation actionDSI = (FullGraphSchedulingInformation) action.getSchedulingInfo();

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

    private boolean tryToLaunch(AllocatableAction action) {
        try {
            action.tryToLaunch();
            return true;
        } catch (InvalidSchedulingException ise) {
            LOGGER.error("Exception on tryToLaunch", ise);
            try {
                long actionScore = FullGraphScore.getActionScore(action);
                double dataTime = (new FullGraphScore(0, 0, 0, 0, 0))
                        .getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new FullGraphScore(actionScore, dataTime, 0, 0, 0);
                boolean keepTrying = true;
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                    AllocatableAction pre = action.getConstrainingPredecessors().get(i);
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

    public static Comparator<AllocatableAction> getScanComparator() {
        return new Comparator<AllocatableAction>() {

            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                FullGraphSchedulingInformation action1DSI = (FullGraphSchedulingInformation) action1
                        .getSchedulingInfo();
                FullGraphSchedulingInformation action2DSI = (FullGraphSchedulingInformation) action2
                        .getSchedulingInfo();
                int compare = Long.compare(action2DSI.getExpectedStart(), action1DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action2.getId(), action1.getId());
                }
                return compare;
            }
        };
    }

    public static Comparator<AllocatableAction> getReadyComparator() {
        return new Comparator<AllocatableAction>() {

            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                FullGraphSchedulingInformation action1DSI = (FullGraphSchedulingInformation) action1
                        .getSchedulingInfo();
                FullGraphSchedulingInformation action2DSI = (FullGraphSchedulingInformation) action2
                        .getSchedulingInfo();
                int compare = Long.compare(action1DSI.getExpectedStart(), action2DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                }
                return compare;
            }
        };
    }

    private void addGap(Gap g) {
        Iterator<Gap> gapIt = this.gaps.iterator();
        int index = 0;
        Gap gap;
        while (gapIt.hasNext() && (gap = gapIt.next()) != null && gap.getInitialTime() <= g.getInitialTime()) {
            index++;
        }
        this.gaps.add(index, g);
    }

    public long getLastGapExpectedStart() {
        return this.gaps.peekFirst().getInitialTime();
    }

}
