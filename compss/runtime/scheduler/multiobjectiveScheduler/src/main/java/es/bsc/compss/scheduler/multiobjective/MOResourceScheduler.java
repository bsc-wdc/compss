/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.multiobjective;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.ActionNotFoundException;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.multiobjective.config.MOConfiguration;
import es.bsc.compss.scheduler.multiobjective.types.Gap;
import es.bsc.compss.scheduler.multiobjective.types.LocalOptimizationState;
import es.bsc.compss.scheduler.multiobjective.types.MOProfile;
import es.bsc.compss.scheduler.multiobjective.types.MOScore;
import es.bsc.compss.scheduler.multiobjective.types.OptimizationAction;
import es.bsc.compss.scheduler.multiobjective.types.SchedulingEvent;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.CoreManager;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONException;
import org.json.JSONObject;


public class MOResourceScheduler<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean IS_DEBUG = LOGGER.isDebugEnabled();
    protected static final String LOG_PREFIX = "[MOResourceScheduler] ";

    private final LinkedList<Gap> gaps;
    private double pendingActionsEnergy = 0; // mJ
    private double pendingActionsCost = 0; // Currency*ms/h
    private int[][] implementationsCount;
    private int[][] runningImplementationsCount;
    private double runningActionsEnergy = 0; // mJ
    private double runningActionsCost = 0; // Currency*ms/h

    private double runActionsEnergy = 0; // mJ
    private double runActionsCost = 0; // Currency*ms/h

    private OptimizationAction opAction;
    private final Set<AllocatableAction> pendingUnschedulings = new HashSet<>();
    private AllocatableAction resourceBlockingAction = new OptimizationAction();
    private AllocatableAction dataBlockingAction = new OptimizationAction();
    private long expectedEndTimeRunning;
    private final double idlePower; // W
    private final double idlePrice; // Currency/h


    public MOResourceScheduler(Worker<T> w, JSONObject resourceJSON, JSONObject implsJSON) {
        super(w, resourceJSON, implsJSON);
        gaps = new LinkedList<>();
        addGap(new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, myWorker.getDescription().copy(), 0));
        implementationsCount = new int[CoreManager.getCoreCount()][];
        runningImplementationsCount = new int[CoreManager.getCoreCount()][];
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            implementationsCount[coreId] = new int[CoreManager.getCoreImplementations(coreId).size()];
            runningImplementationsCount[coreId] = new int[CoreManager.getCoreImplementations(coreId).size()];
        }
        expectedEndTimeRunning = 0;
        double idlePower;
        double idlePrice;

        if (resourceJSON != null) {
            try {
                idlePower = resourceJSON.getDouble("idlePower");
            } catch (JSONException je) {
                idlePower = MOConfiguration.DEFAULT_IDLE_POWER;
            }

            try {
                idlePrice = resourceJSON.getDouble("idlePrice");
            } catch (JSONException je) {
                idlePrice = MOConfiguration.DEFAULT_IDLE_PRICE;
            }
        } else {
            idlePower = MOConfiguration.DEFAULT_IDLE_POWER;
            idlePrice = MOConfiguration.DEFAULT_IDLE_PRICE;
        }
        this.idlePower = idlePower;
        this.idlePrice = idlePrice;
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
    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        long resScore = Score.calculateDataLocalityScore(params, myWorker);
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
        long actionPriority = actionScore.getActionScore();
        long expectedDataAvailable = ((MOScore) actionScore).getExpectedDataAvailable() + resScore * MOConfiguration.DATA_TRANSFER_DELAY;
        return new MOScore(actionPriority, expectedDataAvailable, lessTimeStamp, 0, 0, 0);
    }

    /**
     *
     * @param action
     * @param params
     * @param impl
     * @param resourceScore
     * @return
     */
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl, Score resourceScore) {
        long resourceFreeTime = getResourceFreeTime(impl);
        long expectedDataAvailable = ((MOScore) resourceScore).getExpectedDataAvailable();
        long actionPriority = resourceScore.getActionScore();
        return generateMOScore(resourceFreeTime, expectedDataAvailable, actionPriority, impl);
    }

    public MOScore generateMoveImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
            Score resourceScore, long moveTime) {
        long resourceFreeTime = getResourceFreeTime(impl) + moveTime;
        long expectedDataAvailable = ((MOScore) resourceScore).getExpectedDataAvailable() + moveTime;
        long actionPriority = resourceScore.getActionScore();
        return generateMOScore(resourceFreeTime, expectedDataAvailable, actionPriority, impl);
    }

    private long getResourceFreeTime(Implementation impl) {
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
        return resourceFreeTime;
    }

    public MOScore generateCurrentImplementationScore(AllocatableAction action, Implementation impl, Score resourceScore) {
        // Check if it is to be deleted
        long resourceFreeTime = Long.MAX_VALUE;
        Gap g = gaps.peekFirst();
        if (g != null) {
            resourceFreeTime = ((MOSchedulingInformation) action.getSchedulingInfo()).getExpectedStart();
        }
        long expectedDataAvailable = ((MOScore) resourceScore).getExpectedDataAvailable();
        long actionPriority = resourceScore.getActionScore();
        return generateMOScore(resourceFreeTime, expectedDataAvailable, actionPriority, impl);

    }

    public MOScore generateMOScore(long resourceFreeTime, long expectedDataAvailable, long actionPriority, Implementation impl) {
        long implScore = 0;
        double energy = 0;
        double cost = 0;
        MOProfile p = (MOProfile) this.getProfile(impl);
        if (p != null) {
            implScore = p.getAverageExecutionTime();
            long waitingTime = Math.max(resourceFreeTime, expectedDataAvailable);
            if (waitingTime < Long.MAX_VALUE) {
                energy = ((waitingTime + implScore) * getIdlePower()) + (p.getPower() * implScore);
                cost = ((waitingTime + implScore) * getIdlePrice()) + (p.getPrice() * implScore);
            } else {
                energy = Double.MAX_VALUE;
                cost = Double.MAX_VALUE;
            }
        }
        // The data transfer penalty is already included on the datadependency time of the resourceScore
        return new MOScore(actionPriority, expectedDataAvailable, resourceFreeTime, implScore, energy, cost);
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     ---------------- Scheduler Methods ----------------
     ---------------------------------------------------
     --------------------------------------------------*/
    @Override
    public void scheduleAction(AllocatableAction action) {
        synchronized (gaps) {
            if (opAction != null) { // If optimization in progress
                ((MOSchedulingInformation) opAction.getSchedulingInfo()).addSuccessor(action);
                Gap opActionGap = new Gap(0, 0, opAction, action.getAssignedImplementation().getRequirements().copy(), 0);
                ((MOSchedulingInformation) action.getSchedulingInfo()).addPredecessor(opActionGap);
            } else {
                scheduleUsingGaps(action, gaps);
            }
        }
    }

    @Override
    public List<AllocatableAction> unscheduleAction(AllocatableAction action) throws ActionNotFoundException {
        super.unscheduleAction(action);
        List<AllocatableAction> freeActions = new LinkedList<>();

        MOSchedulingInformation actionDSI = (MOSchedulingInformation) action.getSchedulingInfo();
        List<Gap> resources = new LinkedList<>();

        // Block all predecessors
        for (Gap pGap : actionDSI.getPredecessors()) {
            AllocatableAction pred = pGap.getOrigin();
            if (pred != null) {
                MOSchedulingInformation predDSI = (MOSchedulingInformation) pred.getSchedulingInfo();
                predDSI.lock();
            }
        }
        // Block Action
        actionDSI.lock();

        if (!actionDSI.isScheduled() || action.getAssignedResource() != this) {
            for (Gap pGap : actionDSI.getPredecessors()) {
                AllocatableAction pred = pGap.getOrigin();
                if (pred != null) {
                    MOSchedulingInformation predDSI = (MOSchedulingInformation) pred.getSchedulingInfo();
                    predDSI.unlock();
                }
            }
            actionDSI.unscheduled();
            actionDSI.unlock();
            throw new ActionNotFoundException();
        }

        ResourceDescription unassignedResources = action.getAssignedImplementation().getRequirements().copy();
        // For each predecessor consuming resources ->
        // Register resources depending on a predecessor
        // Remove the scheduling dependency on the predecessor
        for (Gap pGap : actionDSI.getPredecessors()) {
            AllocatableAction pred = pGap.getOrigin();
            if (pred != null) {
                if (!(pred instanceof OptimizationAction)) {
                    resources.add(new Gap(pGap.getInitialTime(), Long.MAX_VALUE, pred, pGap.getResources().copy(), 0));
                    unassignedResources.reduceDynamic(pGap.getResources());
                }
                MOSchedulingInformation predDSI = (MOSchedulingInformation) pred.getSchedulingInfo();
                predDSI.removeSuccessor(action);
            }
        }
        resources.add(new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, unassignedResources, 0));
        // Remove all predecessors for
        actionDSI.clearPredecessors();

        // Block all successors
        List<MOSchedulingInformation> successorsDSIs = new LinkedList<MOSchedulingInformation>();
        for (AllocatableAction successor : actionDSI.getSuccessors()) {
            MOSchedulingInformation succDSI = (MOSchedulingInformation) successor.getSchedulingInfo();
            succDSI.lock();
            successorsDSIs.add(succDSI);
        }

        // For each successor look for the resources
        for (AllocatableAction successor : actionDSI.getSuccessors()) {
            MOSchedulingInformation succDSI = (MOSchedulingInformation) successor.getSchedulingInfo();
            // Gets the resources that was supposed to get from the task and remove the dependency
            Gap toCover = succDSI.removePredecessor(action);
            if (toCover != null) {
                ResourceDescription resToCover = toCover.getResources();

                // Scans the resources related to the task to cover its requirements
                Iterator<Gap> gIt = resources.iterator();
                while (gIt.hasNext()) {
                    Gap availableGap = gIt.next();
                    // Takes the resources from a predecessor,
                    ResourceDescription availableDesc = availableGap.getResources();
                    ResourceDescription usedResources = ResourceDescription.reduceCommonDynamics(availableDesc, resToCover);

                    // If it could take some of the resources -> adds a dependency
                    // If all the resources from the predecessor are used -> removes from the list & unlock
                    // If all the resources required for the successor are covered -> move to the next successor
                    if (!usedResources.isDynamicUseless()) {
                        AllocatableAction availableOrigin = availableGap.getOrigin();
                        MOSchedulingInformation availableDSI = null;
                        if (availableOrigin != null) {
                            availableDSI = (MOSchedulingInformation) availableOrigin.getSchedulingInfo();
                            availableDSI.addSuccessor(successor);
                            succDSI.addPredecessor(
                                    new Gap(availableGap.getInitialTime(), Long.MAX_VALUE, availableOrigin, usedResources, 0));
                        }
                        if (availableDesc.isDynamicUseless()) {
                            gIt.remove();
                            if (availableDSI != null) {
                                availableDSI.unlock();
                            }
                        }
                        if (resToCover.isDynamicUseless()) {
                            break;
                        }
                    }
                }
            }

            if (succDSI.isExecutable()) {
                freeActions.add(successor);
            }
        }
        // Clear action's successors
        actionDSI.clearSuccessors();

        // Indicate that the task is fully unsheduled
        actionDSI.unscheduled();

        // Register those resources occupied by the task that haven't been used as free
        synchronized (gaps) {
            if (actionDSI.isOnOptimization()) {
                pendingUnschedulings.add(action);
            }
            Iterator<Gap> gIt = gaps.iterator();
            while (gIt.hasNext()) {
                Gap g = gIt.next();
                if (g.getOrigin() == action) {
                    gIt.remove();
                }
            }
            for (Gap newGap : resources) {
                AllocatableAction gapAction = newGap.getOrigin();
                addGap(newGap);
                if (gapAction != null) {
                    ((MOSchedulingInformation) gapAction.getSchedulingInfo()).unlock();
                }
            }
        }

        Implementation impl = action.getAssignedImplementation();
        MOProfile p = (MOProfile) getProfile(impl);
        if (p != null) {
            long length = actionDSI.getExpectedEnd() - (actionDSI.getExpectedStart() < 0 ? 0 : actionDSI.getExpectedStart());
            pendingActionsCost -= p.getPrice() * length;
            pendingActionsEnergy -= p.getPower() * length;
        }
        actionDSI.unlock();
        for (MOSchedulingInformation successorsDSI : successorsDSIs) {
            successorsDSI.unlock();
        }
        return freeActions;
    }

    @Override
    public void clear() {
        super.clear();
        gaps.clear();
        addGap(new Gap(Long.MIN_VALUE, Long.MAX_VALUE, null, myWorker.getDescription().copy(), 0));
    }

    private void scheduleUsingGaps(AllocatableAction action, List<Gap> gaps) {
        long expectedStart = 0;
        // Compute start time due to data dependencies
        for (AllocatableAction predecessor : action.getDataPredecessors()) {
            MOSchedulingInformation predDSI = ((MOSchedulingInformation) predecessor.getSchedulingInfo());
            if (predDSI.isScheduled()) {
                long predEnd = predDSI.getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
            }
        }
        MOSchedulingInformation schedInfo = (MOSchedulingInformation) action.getSchedulingInfo();

        if (expectedStart == Long.MAX_VALUE) {
            // There is some data dependency with blocked tasks in some resource
            Gap opActionGap = new Gap(0, 0, dataBlockingAction, action.getAssignedImplementation().getRequirements().copy(), 0);
            MOSchedulingInformation dbaDSI = (MOSchedulingInformation) dataBlockingAction.getSchedulingInfo();
            dbaDSI.lock();
            schedInfo.lock();
            dbaDSI.addSuccessor(action);
            schedInfo.addPredecessor(opActionGap);
            schedInfo.setExpectedStart(Long.MAX_VALUE);
            schedInfo.setExpectedEnd(Long.MAX_VALUE);
            schedInfo.scheduled();
            dbaDSI.unlock();
            schedInfo.unlock();
            return;
        }

        Implementation impl = action.getAssignedImplementation();
        MOProfile p = (MOProfile) getProfile(impl);
        ResourceDescription constraints = impl.getRequirements().copy();
        List<Gap> predecessors = new LinkedList<>();
        Iterator<Gap> gapIt = ((LinkedList<Gap>) gaps).descendingIterator();
        boolean fullyCoveredReqs = false;
        // Compute predecessors and update gaps
        // Check gaps before data start
        while (gapIt.hasNext() && !fullyCoveredReqs) {
            Gap gap = gapIt.next();
            if (gap.getInitialTime() <= expectedStart) {
                useGap(gap, constraints, predecessors);
                fullyCoveredReqs = constraints.isDynamicUseless();
                if (gap.getResources().isDynamicUseless()) {
                    gapIt.remove();
                }
            }
        }
        // Check gaps after data start
        gapIt = gaps.iterator();
        while (gapIt.hasNext() && !fullyCoveredReqs) {
            Gap gap = gapIt.next();
            if (gap.getInitialTime() > expectedStart) {
                if (gap.getInitialTime() < Long.MAX_VALUE) {
                    useGap(gap, constraints, predecessors);
                    fullyCoveredReqs = constraints.isDynamicUseless();
                    if (gap.getResources().isDynamicUseless()) {
                        gapIt.remove();
                    }
                }
            }
        }

        if (!fullyCoveredReqs) {
            // Action gets blocked due to lack of resources
            for (Gap pGap : predecessors) {
                addGap(pGap);
                AllocatableAction predecessor = (AllocatableAction) pGap.getOrigin();
                if (predecessor != null) {
                    MOSchedulingInformation predDSI = ((MOSchedulingInformation) predecessor.getSchedulingInfo());
                    predDSI.unlock();
                }
            }
            Gap opActionGap = new Gap(0, 0, resourceBlockingAction, action.getAssignedImplementation().getRequirements(), 0);
            MOSchedulingInformation rbaDSI = (MOSchedulingInformation) resourceBlockingAction.getSchedulingInfo();
            rbaDSI.lock();
            schedInfo.lock();
            rbaDSI.addSuccessor(action);
            schedInfo.addPredecessor(opActionGap);
            schedInfo.scheduled();
            schedInfo.setExpectedStart(Long.MAX_VALUE);
            schedInfo.setExpectedEnd(Long.MAX_VALUE);
            rbaDSI.unlock();
            schedInfo.unlock();
            return;
        }

        // Lock acces to the current task
        schedInfo.lock();
        schedInfo.scheduled();

        // Add dependencies
        // Unlock access to predecessor
        StringBuilder sb = null;
        if (IS_DEBUG) {
            sb = new StringBuilder("Predecessors: ");
        }
        for (Gap pGap : predecessors) {
            AllocatableAction predecessor = pGap.getOrigin();
            if (predecessor != null) {
                MOSchedulingInformation predDSI = ((MOSchedulingInformation) predecessor.getSchedulingInfo());
                if (predDSI.isScheduled()) {
                    long predEnd = predDSI.getExpectedEnd();
                    expectedStart = Math.max(expectedStart, predEnd);
                    predDSI.addSuccessor(action);
                }
                predDSI.unlock();
            }
            schedInfo.addPredecessor(pGap);
            if (IS_DEBUG) {
                sb.append(pGap.getOrigin()).append(" with ").append(pGap.getResources().getDynamicDescription()).append(", ");
            }
        }

        // Compute end time
        schedInfo.setExpectedStart(expectedStart);
        long expectedEnd = expectedStart;
        if (p != null) {
            expectedEnd += p.getAverageExecutionTime();
            pendingActionsCost += p.getPrice() * p.getAverageExecutionTime();
            pendingActionsEnergy += p.getPower() * p.getAverageExecutionTime();
        }
        schedInfo.setExpectedEnd(expectedEnd);
        // Unlock access to current task
        schedInfo.unlock();
        if (action.isToReleaseResources()) {// Create new Gap correspondin to the resources released by the action
            addGap(new Gap(expectedEnd, Long.MAX_VALUE, action, impl.getRequirements().copy(), 0));
        } else {
            addGap(new Gap(Long.MAX_VALUE, Long.MAX_VALUE, action, impl.getRequirements().copy(), 0));
        }
        if (IS_DEBUG) {
            LOGGER.debug(LOG_PREFIX + "Scheduled " + action.toString() + ". Interval [ " + expectedStart + " - " + expectedEnd + "] "
                    + sb.toString());
        }
    }

    private void useGap(Gap gap, ResourceDescription resources, List<Gap> predecessors) {
        AllocatableAction predecessor = (AllocatableAction) gap.getOrigin();
        ResourceDescription gapResource = gap.getResources();
        ResourceDescription usedResources = ResourceDescription.reduceCommonDynamics(gapResource, resources);
        if (usedResources.isDynamicConsuming()) {
            if (predecessor != null) {
                MOSchedulingInformation predDSI = ((MOSchedulingInformation) predecessor.getSchedulingInfo());
                predDSI.lock();
            }
            Gap g = new Gap(gap.getInitialTime(), Long.MAX_VALUE, predecessor, usedResources, 0);
            predecessors.add(g);
        }
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     -------------- Optimization Methods ---------------
     ---------------------------------------------------
     --------------------------------------------------*/
    @SuppressWarnings("unchecked")
    public PriorityQueue<AllocatableAction> localOptimization(long updateId, Comparator<AllocatableAction> selectionComparator,
            Comparator<AllocatableAction> donorComparator) {
        // System.out.println("Local Optimization for " + this.getName() + " starts");
        LocalOptimizationState state = new LocalOptimizationState(updateId, (MOResourceScheduler<WorkerResourceDescription>) this,
                getReadyComparator(), selectionComparator);
        PriorityQueue<AllocatableAction> actions = new PriorityQueue<AllocatableAction>(1, donorComparator);

        synchronized (gaps) {
            opAction = new OptimizationAction();
        }
        // No changes in the Gap structure

        // Scan actions: Filters ready and selectable actions
        LOGGER.debug(LOG_PREFIX + "Scanning current actions");
        List<AllocatableAction> lockedActions = scanActions(state);
        // Gets all the pending schedulings
        List<AllocatableAction> newPendingSchedulings = new LinkedList<>();
        List<AllocatableAction> pendingSchedulings;
        synchronized (gaps) {
            MOSchedulingInformation opDSI = (MOSchedulingInformation) opAction.getSchedulingInfo();
            pendingSchedulings = opDSI.replaceSuccessors(newPendingSchedulings);
        }
        // Classify pending actions: Filters ready and selectable actions
        LOGGER.debug(LOG_PREFIX + "Classify Pending Scheduling/Unscheduling actions");
        classifyPendingSchedulings(pendingSchedulings, state);
        classifyPendingUnschedulings(state);

        // ClassifyActions
        LOGGER.debug(LOG_PREFIX + "Reschedule pending actions");
        List<Gap> newGaps = rescheduleTasks(state, actions);
        // Ensuring there are no locked actions after rescheduling
        for (AllocatableAction action : lockedActions) {
            MOSchedulingInformation actionDSI = (MOSchedulingInformation) action.getSchedulingInfo();
            try {
                actionDSI.unlock();
            } catch (IllegalMonitorStateException e) {
                LOGGER.debug(LOG_PREFIX + "Illegal Monitor Exception when releasing locked actions. Ignoring...");
            }
        }
        /*
         * System.out.println("\t is running: "); for (AllocatableAction aa : state.getRunningActions()) {
         * System.out.println("\t\t" + aa + " with implementation " + ((aa.getAssignedImplementation() == null) ? "null"
         * : aa .getAssignedImplementation().getImplementationId()) + " started " + ((aa.getStartTime() == null) ? "-" :
         * (System .currentTimeMillis() - aa.getStartTime())));
         * 
         * }
         * 
         * System.out.println(this.getName() + " has no resources for: "); for (AllocatableAction aa :
         * this.resourceBlockingAction .getDataSuccessors()) { System.out .println("\t" + aa + " with" +
         * " implementation " + ((aa.getAssignedImplementation() == null) ? "null" : aa.getAssignedImplementation()
         * .getImplementationId())); } System.out .println(this.getName() +
         * " will wait for data producers to be rescheduled for actions:"); for (AllocatableAction aa :
         * this.dataBlockingAction.getDataSuccessors()) { System.out .println("\t" + aa + " with" + " implementation " +
         * ((aa.getAssignedImplementation() == null) ? "null" : aa.getAssignedImplementation() .getImplementationId()));
         * }
         */

        // Schedules all the pending scheduligns and unblocks the scheduling of new actions
        LOGGER.debug(LOG_PREFIX + "Manage new gaps");
        synchronized (gaps) {
            gaps.clear();
            gaps.addAll(newGaps);
            MOSchedulingInformation opDSI = (MOSchedulingInformation) opAction.getSchedulingInfo();
            List<AllocatableAction> successors = opDSI.getSuccessors();
            for (AllocatableAction action : successors) {
                actions.add(action);
                MOSchedulingInformation actionDSI = (MOSchedulingInformation) action.getSchedulingInfo();
                actionDSI.lock();
                actionDSI.removePredecessor(opAction);
                this.scheduleUsingGaps(action, gaps);
                actionDSI.unlock();
            }
            opDSI.clearSuccessors();
            opAction = null;
        }
        // System.out.println("Local Optimization for " + this.getName() + " ends");
        return actions;
    }

    // Classifies actions according to their start times. Selectable actions are
    // those that can be selected to run from t=0. Ready actions are those actions
    // that have data dependencies with tasks scheduled in other nodes. Actions
    // with dependencies with actions scheduled in the same node, are not
    // classified in any list since we cannot know the start time.
    public List<AllocatableAction> scanActions(LocalOptimizationState state) {
        List<AllocatableAction> pendingToUnlockActions = new LinkedList<AllocatableAction>();
        PriorityQueue<AllocatableAction> actions = new PriorityQueue<AllocatableAction>(1, getScanComparator());
        MOSchedulingInformation blockSI = (MOSchedulingInformation) dataBlockingAction.getSchedulingInfo();
        List<AllocatableAction> blockActions = blockSI.getSuccessors();
        for (AllocatableAction gapAction : blockActions) {
            MOSchedulingInformation dsi = (MOSchedulingInformation) gapAction.getSchedulingInfo();
            dsi.lock();
            dsi.setOnOptimization(true);
            actions.add(gapAction);
        }

        blockSI = (MOSchedulingInformation) resourceBlockingAction.getSchedulingInfo();
        blockActions = blockSI.getSuccessors();
        for (AllocatableAction gapAction : blockActions) {
            MOSchedulingInformation dsi = (MOSchedulingInformation) gapAction.getSchedulingInfo();
            dsi.lock();
            dsi.setOnOptimization(true);
            actions.add(gapAction);
        }
        LinkedList<AllocatableAction> modified = new LinkedList<>();
        while (true) {
            try {
                for (Gap g : gaps) {
                    AllocatableAction gapAction = g.getOrigin();
                    if (gapAction != null) {
                        MOSchedulingInformation dsi = (MOSchedulingInformation) gapAction.getSchedulingInfo();
                        dsi.lock();
                        dsi.setOnOptimization(true);
                        modified.add(gapAction);
                    }
                }
                break;
            } catch (ConcurrentModificationException cme) {
                for (AllocatableAction action : modified) {
                    MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
                    dsi.setOnOptimization(false);
                    dsi.unlock();
                }
                modified.clear();
            }
        }
        actions.addAll(modified);

        AllocatableAction action;
        while ((action = actions.poll()) != null) {
            MOSchedulingInformation actionDSI = (MOSchedulingInformation) action.getSchedulingInfo();
            // System.out.println(" ** Evaluating Action "+ action + " with SI: "+ actionDSI.hashCode());
            if (!actionDSI.isScheduled()) {
                try {
                    actionDSI.unlock();
                } catch (IllegalMonitorStateException e) {
                    LOGGER.debug(LOG_PREFIX + "Illegal Monitor Exception scaning actions. Ignoring...");
                }
                // Task was already executed. Ignore
                // System.out.println(" ** End Action "+ action + " with SI: "+ actionDSI.hashCode()+ " because not
                // scheduled ");
                continue;
            }

            // Data Dependencies analysis
            boolean hasInternal = false;
            boolean hasExternal = false;
            long startTime = 0;
            while (true) {
                List<MOSchedulingInformation> managedDSIs = new LinkedList<>();
                try {
                    List<AllocatableAction> dPreds = action.getDataPredecessors();
                    for (AllocatableAction dPred : dPreds) {
                        MOSchedulingInformation dPredDSI = (MOSchedulingInformation) dPred.getSchedulingInfo();
                        if (dPred.getAssignedResource() == this) {
                            if (dPredDSI.tryToLock()) {
                                if (dPredDSI.isScheduled()) {
                                    hasInternal = true;
                                    dPredDSI.addOptimizingSuccessor(action);
                                    managedDSIs.add(dPredDSI);
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
                    break;
                } catch (ConcurrentModificationException cme) {
                    for (MOSchedulingInformation dsi : managedDSIs) {
                        dsi.removeOptimizingSuccessor(action);
                    }
                }
            }
            // Resource Dependencies analysis
            boolean hasResourcePredecessors = false;
            List<Gap> rPredGaps = actionDSI.getPredecessors();
            for (Gap rPredGap : rPredGaps) {
                AllocatableAction rPred = rPredGap.getOrigin();
                if (rPred != null) {
                    MOSchedulingInformation rPredDSI = (MOSchedulingInformation) rPred.getSchedulingInfo();
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
                }
                // else the predecessor was already executed
            }
            actionDSI.setExpectedStart(startTime);
            actionDSI.setToReschedule(true);
            // System.out.println(" ** Classifiying Action "+ action + " with SI: "+ actionDSI.hashCode());
            state.classifyAction(action, hasInternal, hasExternal, hasResourcePredecessors, startTime);
            if (hasResourcePredecessors || hasInternal) {
                // The action has a blocked predecessor in the resource that will block its execution
                actionDSI.unlock();
            } else {
                pendingToUnlockActions.add(action);
            }
            // System.out.println(" ** END evaluation Action "+ action + " with SI: "+ actionDSI.hashCode());
        }
        return pendingToUnlockActions;
    }

    public void classifyPendingSchedulings(List<AllocatableAction> pendingSchedulings, LocalOptimizationState state) {
        for (AllocatableAction action : pendingSchedulings) {
            // Action has an artificial resource dependency with the opAction
            MOSchedulingInformation actionDSI = (MOSchedulingInformation) action.getSchedulingInfo();
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
                    MOSchedulingInformation dPredDSI = (MOSchedulingInformation) dPred.getSchedulingInfo();
                    if (dPred.getAssignedResource() == this) {
                        if (dPredDSI.tryToLock()) {
                            if (dPredDSI.isScheduled()) {
                                hasInternal = true;
                                dPredDSI.addOptimizingSuccessor(action);
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
            state.classifyAction(action, hasInternal, hasExternal, true, startTime);
        }
    }

    public void classifyPendingUnschedulings(LocalOptimizationState state) {
        for (AllocatableAction unscheduledAction : pendingUnschedulings) {
            MOSchedulingInformation actionDSI = (MOSchedulingInformation) unscheduledAction.getSchedulingInfo();
            List<AllocatableAction> successors = actionDSI.getOptimizingSuccessors();
            for (AllocatableAction successor : successors) {
                // Data Dependencies analysis
                boolean hasInternal = false;
                boolean hasExternal = false;
                long startTime = 0;
                try {
                    List<AllocatableAction> dPreds = successor.getDataPredecessors();
                    for (AllocatableAction dPred : dPreds) {
                        MOSchedulingInformation dPredDSI = (MOSchedulingInformation) dPred.getSchedulingInfo();
                        if (dPred.getAssignedResource() == this) {
                            if (dPredDSI.tryToLock()) {
                                if (dPredDSI.isScheduled()) {
                                    hasInternal = true;
                                    dPredDSI.addOptimizingSuccessor(successor);
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
                state.classifyAction(successor, hasInternal, hasExternal, true, startTime);
            }
        }
        pendingUnschedulings.clear();
    }

    @SuppressWarnings("unchecked")
    public List<Gap> rescheduleTasks(LocalOptimizationState state, PriorityQueue<AllocatableAction> rescheduledActions) {
        this.runActionsCost = 0;
        this.runActionsEnergy = 0;
        for (int coreId = 0; coreId < CoreManager.getCoreCount(); coreId++) {
            for (int implId = 0; implId < CoreManager.getNumberCoreImplementations(coreId); implId++) {
                MOProfile profile = (MOProfile) this.getProfile(coreId, implId);
                runActionsEnergy += profile.getPower() * profile.getExecutionCount() * profile.getAverageExecutionTime();
                runActionsCost += profile.getPrice() * profile.getExecutionCount() * profile.getAverageExecutionTime();
            }
        }

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
        Gap gap = state.peekFirstGap();
        ResourceDescription gapResource = gap.getResources();
        PriorityQueue<SchedulingEvent> schedulingQueue = new PriorityQueue<>();
        // For every running action we create a start event on their real start timeStamp
        for (AllocatableAction action : state.getRunningActions()) {
            manageRunningAction(action, state);
            MOSchedulingInformation actionDSI = (MOSchedulingInformation) action.getSchedulingInfo();
            schedulingQueue.offer(new SchedulingEvent.End(actionDSI.getExpectedEnd(), action));
        }
        while (state.areRunnableActions() && !gapResource.isDynamicUseless()) {
            AllocatableAction top = state.getMostPrioritaryRunnableAction();
            state.replaceAction(top);
            if (state.canActionRun()) {
                state.removeMostPrioritaryRunnableAction();
                // Start the current action
                MOSchedulingInformation topDSI = (MOSchedulingInformation) top.getSchedulingInfo();
                topDSI.lock();
                topDSI.clearPredecessors();
                manageRunningAction(top, state);
                if (tryToLaunch(top)) {
                    schedulingQueue.offer(new SchedulingEvent.End(topDSI.getExpectedEnd(), top));
                }
            } else {
                break;
            }
        }
        while (!schedulingQueue.isEmpty() || state.areActionsToBeRescheduled()) {
            // We reschedule as many tasks as possible by processing start and end SchedulingEvents

            while (!schedulingQueue.isEmpty()) {
                SchedulingEvent e = schedulingQueue.poll();
                /*
                 * Start Event: - sets the expected start and end times - adds resource dependencies with the previous
                 * actions - if there's a gap before the dependency -tries to fill it with other tasks - if all the
                 * resources released by the predecessor are used later - the action is unlocked
                 *
                 * End Event:
                 * 
                 */
                List<SchedulingEvent> result = e.process(state, (MOResourceScheduler<WorkerResourceDescription>) this, rescheduledActions);
                for (SchedulingEvent r : result) {
                    schedulingQueue.offer(r);
                }
            }

            if (state.areActionsToBeRescheduled()) {
                AllocatableAction topAction = state.getEarliestActionToBeRescheduled();
                MOSchedulingInformation topActionDSI = (MOSchedulingInformation) topAction.getSchedulingInfo();
                topActionDSI.lock();
                topActionDSI.setToReschedule(false);
                schedulingQueue.offer(new SchedulingEvent.Start(topActionDSI.getExpectedStart(), topAction));
            }
        }
        for (Gap g : state.getGaps()) {
            state.removeTmpGap(g);
        }
        this.pendingActionsCost = state.getTotalCost();
        this.pendingActionsEnergy = state.getTotalEnergy();
        this.implementationsCount = state.getImplementationsCount();
        this.expectedEndTimeRunning = state.getEndRunningTime();
        this.runningImplementationsCount = state.getRunningImplementations();
        this.runningActionsEnergy = state.getRunningEnergy();
        this.runningActionsCost = state.getRunningCost();
        this.resourceBlockingAction = state.getResourceBlockingAction();
        this.dataBlockingAction = state.getDataBlockingAction();

        return state.getGaps();
    }

    private void manageRunningAction(AllocatableAction action, LocalOptimizationState state) {

        Implementation impl = action.getAssignedImplementation();
        MOSchedulingInformation actionDSI = (MOSchedulingInformation) action.getSchedulingInfo();

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
        MOProfile p = (MOProfile) getProfile(impl);
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
        state.runningAction(impl, p, endTime);
    }

    private boolean tryToLaunch(AllocatableAction action) {
        boolean launched = false;
        try {
            action.tryToLaunch();
            launched = true;
        } catch (InvalidSchedulingException ise) {

        }
        if (!launched) {
            long actionScore = MOScore.getActionScore(action);
            Score aScore = new MOScore(actionScore, 0, 0, 0, 0, 0);
            try {
                action.schedule(aScore);
                try {
                    action.tryToLaunch();
                } catch (InvalidSchedulingException ise2) {
                    // Impossible exception.
                    LOGGER.error(ise2);
                }
            } catch (BlockedActionException | UnassignedActionException be) {
                // Can not happen since there was an original source
                LOGGER.error(be);
            }
        }
        return launched;
    }

    public static final Comparator<AllocatableAction> getScanComparator() {
        return new Comparator<AllocatableAction>() {

            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                MOSchedulingInformation action1DSI = (MOSchedulingInformation) action1.getSchedulingInfo();
                MOSchedulingInformation action2DSI = (MOSchedulingInformation) action2.getSchedulingInfo();
                int compare = Long.compare(action2DSI.getExpectedStart(), action1DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action2.getId(), action1.getId());
                }
                return compare;
            }
        };
    }

    public static final Comparator<AllocatableAction> getReadyComparator() {
        return new Comparator<AllocatableAction>() {

            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                MOSchedulingInformation action1DSI = (MOSchedulingInformation) action1.getSchedulingInfo();
                MOSchedulingInformation action2DSI = (MOSchedulingInformation) action2.getSchedulingInfo();
                int compare = Long.compare(action1DSI.getExpectedStart(), action2DSI.getExpectedStart());
                if (compare == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                }
                return compare;
            }
        };
    }

    private void addGap(Gap g) {

        AllocatableAction gapAction = g.getOrigin();
        ResourceDescription releasedResources = g.getResources();
        boolean merged = false;
        for (Gap registeredGap : gaps) {
            if (registeredGap.getOrigin() == gapAction) {
                ResourceDescription registeredResources = registeredGap.getResources();
                registeredResources.increaseDynamic(releasedResources);
                merged = true;
                break;
            }
        }
        if (!merged) {
            Iterator<Gap> gapIt = gaps.iterator();
            int index = 0;
            Gap gap;
            while (gapIt.hasNext() && (gap = gapIt.next()) != null && gap.getInitialTime() <= g.getInitialTime()) {
                index++;
            }
            gaps.add(index, g);
        }
    }

    public long getFirstGapExpectedStart() {
        Gap g = gaps.peekFirst();
        if (g == null) {
            return 0;
        }
        return g.getInitialTime();
    }

    public long getLastGapExpectedStart() {
        Gap g = gaps.peekLast();
        if (g == null) {
            return 0;
        }
        return g.getInitialTime();
    }

    @Override
    public Profile generateProfileForImplementation(Implementation impl, JSONObject jsonImpl) {
        return new MOProfile(jsonImpl);
    }

    @Override
    public Profile generateProfileForRun(AllocatableAction action) {
        return new MOProfile(action.getAssignedImplementation(), myWorker);
    }

    public double getIdlePower() {
        return idlePower;
    }

    public double getRunActionsEnergy() {
        return this.runActionsEnergy;
    }

    public double getRunningActionsEnergy() {
        return this.runningActionsEnergy;
    }

    public double getScheduledActionsEnergy() {
        return this.pendingActionsEnergy;
    }

    public double getIdlePrice() {
        return idlePrice;
    }

    public double getRunActionsCost() {
        return this.runActionsCost;
    }

    public double getRunningActionsCost() {
        return this.runningActionsCost;
    }

    public double getScheduledActionsCost() {
        return this.pendingActionsCost;
    }

    public int getSimultaneousCapacity(Implementation impl) {
        return myWorker.fitCount(impl);
    }

    public int[][] getImplementationCounts() {
        return this.implementationsCount;
    }

    public long getExpectedEndTimeRunning() {
        return this.expectedEndTimeRunning;
    }

    public int[][] getRunningImplementationCounts() {
        return this.runningImplementationsCount;
    }

    @Override
    public PriorityQueue<AllocatableAction> getBlockedActions() {
        PriorityQueue<AllocatableAction> blockedActions = new PriorityQueue<>(20, new Comparator<AllocatableAction>() {

            @Override
            public int compare(AllocatableAction a1, AllocatableAction a2) {
                Score score1 = generateBlockedScore(a1);
                Score score2 = generateBlockedScore(a2);
                return score1.compareTo(score2);
            }
        });

        blockedActions.addAll(super.getBlockedActions());

        MOSchedulingInformation baDSI = (MOSchedulingInformation) dataBlockingAction.getSchedulingInfo();
        baDSI.lock();
        blockedActions.addAll(baDSI.getSuccessors());
        baDSI.unlock();

        baDSI = (MOSchedulingInformation) resourceBlockingAction.getSchedulingInfo();
        baDSI.lock();
        blockedActions.addAll(baDSI.getSuccessors());
        baDSI.unlock();

        return blockedActions;
    }

    @Override
    public JSONObject toJSONObject() {
        JSONObject json = super.toJSONObject();
        json.put("idlePower", idlePower);
        json.put("idlePrice", idlePrice);
        return json;
    }

    @Override
    public JSONObject updateJSON(JSONObject oldResource) {
        JSONObject difference = super.updateJSON(oldResource);
        double diff = idlePower;
        if (oldResource.has("idlePower")) {
            diff -= oldResource.getDouble("idlePower");
        }
        difference.put("idlePower", diff);
        oldResource.put("idlePower", idlePower);

        diff = idlePrice;
        if (oldResource.has("idlePrice")) {
            diff -= oldResource.getDouble("idlePrice");
        }
        difference.put("idlePrice", diff);
        oldResource.put("idlePrice", idlePrice);
        return difference;
    }
}
