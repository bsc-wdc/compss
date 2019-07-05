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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;


public abstract class SchedulingEvent<T extends WorkerResourceDescription> implements Comparable<SchedulingEvent<T>> {

    protected long expectedTimeStamp;
    protected AllocatableAction action;


    public SchedulingEvent(long timeStamp, AllocatableAction action) {
        this.expectedTimeStamp = timeStamp;
        this.action = action;
    }

    @Override
    public int compareTo(SchedulingEvent<T> e) {
        int time = Long.compare(this.expectedTimeStamp, e.expectedTimeStamp);
        if (time == 0) {
            return (getPriority() - e.getPriority());
        }
        return time;
    }

    public AllocatableAction getAction() {
        return this.action;
    }

    protected abstract int getPriority();

    public abstract List<SchedulingEvent<T>> process(LocalOptimizationState state, FullGraphResourceScheduler<T> worker,
            PriorityQueue<AllocatableAction> readyActions, PriorityActionSet selectableActions,
            PriorityQueue<AllocatableAction> rescheduledActions);


    public static class Start<T extends WorkerResourceDescription> extends SchedulingEvent<T> {

        public Start(long timeStamp, AllocatableAction action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 1;
        }

        @Override
        public String toString() {
            return this.action + " start @ " + this.expectedTimeStamp;
        }

        @Override
        public List<SchedulingEvent<T>> process(LocalOptimizationState state, FullGraphResourceScheduler<T> worker,
                PriorityQueue<AllocatableAction> readyActions, PriorityActionSet selectableActions,
                PriorityQueue<AllocatableAction> rescheduledActions) {

            List<SchedulingEvent<T>> enabledEvents = new LinkedList<>();
            FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) this.action.getSchedulingInfo();

            // Set the expected Start time and endTime of the action
            dsi.setExpectedStart(this.expectedTimeStamp);
            long expectedEndTime = getExpectedEnd(this.action, worker, this.expectedTimeStamp);
            dsi.setExpectedEnd(expectedEndTime);
            // Add corresponding end event
            SchedulingEvent<T> endEvent = new End<>(expectedEndTime, this.action);
            enabledEvents.add(endEvent);

            // Remove resources from the state and fill the gaps before its execution
            dsi.clearPredecessors();
            dsi.clearSuccessors();
            List<Gap> tmpGaps = state.reserveResources(this.action.getAssignedImplementation().getRequirements(),
                    this.expectedTimeStamp);

            for (Gap tmpGap : tmpGaps) {
                AllocatableAction gapAction = (AllocatableAction) tmpGap.getOrigin();
                if (tmpGap.getInitialTime() == tmpGap.getEndTime()) {
                    if (gapAction != null) {
                        FullGraphSchedulingInformation gapActionDSI = (FullGraphSchedulingInformation) gapAction
                                .getSchedulingInfo();
                        gapActionDSI.addSuccessor(this.action);
                        dsi.addPredecessor(gapAction);
                        state.removeTmpGap(tmpGap);
                    }
                } else {
                    PriorityQueue<Gap> outGaps = fillGap(worker, tmpGap, readyActions, selectableActions,
                            rescheduledActions, state);
                    for (Gap outGap : outGaps) {
                        AllocatableAction pred = (AllocatableAction) outGap.getOrigin();
                        if (pred != null) {
                            FullGraphSchedulingInformation predDSI = (FullGraphSchedulingInformation) pred
                                    .getSchedulingInfo();
                            predDSI.addSuccessor(this.action);
                            dsi.addPredecessor(pred);
                        }
                        state.removeTmpGap(outGap);
                    }
                }
            }
            rescheduledActions.offer(this.action);
            return enabledEvents;
        }

        private PriorityQueue<Gap> fillGap(FullGraphResourceScheduler<T> worker, Gap gap,
                PriorityQueue<AllocatableAction> readyActions, PriorityActionSet selectableActions,
                PriorityQueue<AllocatableAction> rescheduledActions, LocalOptimizationState state) {

            // Find selected action predecessors
            PriorityQueue<Gap> availableGaps = new PriorityQueue<>(1, new Comparator<Gap>() {

                @Override
                public int compare(Gap g1, Gap g2) {
                    return Long.compare(g1.getInitialTime(), g2.getInitialTime());
                }

            });

            AllocatableAction gapAction = pollActionForGap(gap, worker, selectableActions);

            if (gapAction != null) {
                // Compute method start
                FullGraphSchedulingInformation gapActionDSI = (FullGraphSchedulingInformation) gapAction
                        .getSchedulingInfo();
                gapActionDSI.setToReschedule(false);
                long gapActionStart = Math.max(gapActionDSI.getExpectedStart(), gap.getInitialTime());

                // Fill previous gap space
                if (gap.getInitialTime() != gapActionStart) {
                    Gap previousGap = new Gap(gap.getInitialTime(), gapActionStart, gap.getOrigin(), gap.getResources(),
                            0);
                    state.replaceTmpGap(gap, previousGap);
                    availableGaps = fillGap(worker, previousGap, readyActions, selectableActions, rescheduledActions,
                            state);
                } else {
                    availableGaps.add(gap);
                }

                gapActionDSI.lock();
                // Update Information
                gapActionDSI.setExpectedStart(gapActionStart);
                long expectedEnd = getExpectedEnd(gapAction, worker, gapActionStart);
                gapActionDSI.setExpectedEnd(expectedEnd);
                gapActionDSI.clearPredecessors();

                ResourceDescription desc = gapAction.getAssignedImplementation().getRequirements().copy();
                while (!desc.isDynamicUseless()) {
                    Gap peekGap = availableGaps.peek();
                    AllocatableAction peekAction = (AllocatableAction) peekGap.getOrigin();
                    if (peekAction != null) {
                        FullGraphSchedulingInformation predActionDSI = (FullGraphSchedulingInformation) peekAction
                                .getSchedulingInfo();
                        gapActionDSI.addPredecessor(peekAction);
                        predActionDSI.addSuccessor(gapAction);
                    }
                    ResourceDescription.reduceCommonDynamics(desc, peekGap.getResources());
                    if (peekGap.getResources().isDynamicUseless()) {
                        availableGaps.poll();
                        state.removeTmpGap(gap);
                    }
                }

                List<Gap> extendedGaps = new LinkedList<>();
                // Fill Concurrent
                for (Gap g : availableGaps) {
                    Gap extendedGap = new Gap(g.getInitialTime(), gap.getEndTime(), g.getOrigin(), g.getResources(),
                            g.getCapacity());
                    state.replaceTmpGap(extendedGap, gap);
                    extendedGaps.add(extendedGap);
                }

                availableGaps.clear();
                for (Gap eg : extendedGaps) {
                    availableGaps
                            .addAll(fillGap(worker, eg, readyActions, selectableActions, rescheduledActions, state));
                }

                gapActionDSI.clearSuccessors();
                rescheduledActions.add(gapAction);

                gapActionDSI.setOnOptimization(false);
                // Release Data Successors
                releaseSuccessors(gapActionDSI, worker, readyActions, selectableActions, expectedEnd);

                // Fill Post action gap space
                Gap actionGap = new Gap(expectedEnd, gap.getEndTime(), gapAction,
                        gapAction.getAssignedImplementation().getRequirements(), 0);
                state.addTmpGap(actionGap);
                availableGaps
                        .addAll(fillGap(worker, actionGap, readyActions, selectableActions, rescheduledActions, state));
            } else {
                availableGaps.add(gap);
            }
            return availableGaps;
        }

        private long getExpectedEnd(AllocatableAction action, FullGraphResourceScheduler<T> worker,
                long expectedStart) {

            Implementation impl = action.getAssignedImplementation();
            Profile p = worker.getProfile(impl);
            long endTime = expectedStart;
            if (p != null) {
                endTime += p.getAverageExecutionTime();
            }
            if (endTime < 0) {
                endTime = 0;
            }
            return endTime;
        }

        private AllocatableAction pollActionForGap(Gap gap, FullGraphResourceScheduler<T> worker,
                PriorityActionSet selectableActions) {

            AllocatableAction gapAction = null;
            PriorityQueue<AllocatableAction> peeks = selectableActions.peekAll();
            // Get Main action to fill the gap
            while (!peeks.isEmpty() && gapAction == null) {
                AllocatableAction candidate = peeks.poll();
                // Check times
                FullGraphSchedulingInformation candidateDSI = (FullGraphSchedulingInformation) candidate
                        .getSchedulingInfo();
                long start = candidateDSI.getExpectedStart();
                if (start > gap.getEndTime()) {
                    continue;
                }
                Implementation impl = candidate.getAssignedImplementation();
                Profile p = worker.getProfile(impl);
                long expectedLength = p.getAverageExecutionTime();
                if ((gap.getEndTime() - gap.getInitialTime()) < expectedLength) {
                    continue;
                }
                if ((start + expectedLength) > gap.getEndTime()) {
                    continue;
                }

                // Check description
                if (gap.getResources().canHostDynamic(impl)) {
                    selectableActions.removeFirst(candidate.getCoreId());
                    gapAction = candidate;
                }
            }
            return gapAction;

        }
    }

    public static class End<T extends WorkerResourceDescription> extends SchedulingEvent<T> {

        public End(long timeStamp, AllocatableAction action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 0;
        }

        @Override
        public List<SchedulingEvent<T>> process(LocalOptimizationState state, FullGraphResourceScheduler<T> worker,
                PriorityQueue<AllocatableAction> readyActions, PriorityActionSet selectableActions,
                PriorityQueue<AllocatableAction> rescheduledActions) {

            List<SchedulingEvent<T>> enabledEvents = new LinkedList<>();
            FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) this.action.getSchedulingInfo();
            dsi.setOnOptimization(false);

            // Move from readyActions to Ready
            while (readyActions.size() > 0) {
                AllocatableAction top = readyActions.peek();
                FullGraphSchedulingInformation topDSI = (FullGraphSchedulingInformation) top.getSchedulingInfo();
                long start = topDSI.getExpectedStart();
                if (start > this.expectedTimeStamp) {
                    break;
                }
                readyActions.poll();
                selectableActions.offer(top);
            }

            // Detect released Actions
            releaseSuccessors(dsi, worker, readyActions, selectableActions, this.expectedTimeStamp);

            // Get Top Action
            AllocatableAction currentTop = selectableActions.peek();

            if (state.getAction() != currentTop) {
                state.replaceAction(currentTop);
            }
            state.releaseResources(this.expectedTimeStamp, this.action);

            while (state.canActionRun()) {
                selectableActions.removeFirst(currentTop.getCoreId());
                FullGraphSchedulingInformation topDSI = (FullGraphSchedulingInformation) currentTop.getSchedulingInfo();
                topDSI.lock();
                topDSI.setToReschedule(false);
                SchedulingEvent<T> se = new Start<>(state.getActionStartTime(), currentTop);
                enabledEvents.addAll(se.process(state, worker, readyActions, selectableActions, rescheduledActions));

                currentTop = selectableActions.peek();
                state.replaceAction(currentTop);
            }
            return enabledEvents;
        }

        public String toString() {
            return this.action + " end @ " + this.expectedTimeStamp;
        }
    }


    public void releaseSuccessors(FullGraphSchedulingInformation dsi, FullGraphResourceScheduler<T> worker,
            PriorityQueue<AllocatableAction> readyActions, PriorityActionSet selectableActions, long timeLimit) {

        List<AllocatableAction> successors = dsi.getOptimizingSuccessors();
        for (AllocatableAction successor : successors) {
            FullGraphSchedulingInformation successorDSI = (FullGraphSchedulingInformation) successor
                    .getSchedulingInfo();
            int missingParams = 0;
            long startTime = 0;
            boolean retry = true;
            while (retry) {
                try {
                    List<AllocatableAction> predecessors = successor.getDataPredecessors();
                    for (AllocatableAction predecessor : predecessors) {
                        FullGraphSchedulingInformation predDSI = ((FullGraphSchedulingInformation) predecessor
                                .getSchedulingInfo());
                        if (predecessor.getAssignedResource() != worker) {
                            startTime = Math.max(startTime, predDSI.getExpectedEnd());
                        } else {
                            if (predDSI.isOnOptimization()) {
                                missingParams++;
                            } else {
                                startTime = Math.max(startTime, predDSI.getExpectedEnd());
                            }
                        }
                    }
                    retry = false;
                } catch (ConcurrentModificationException cme) {
                    missingParams = 0;
                    startTime = 0;
                }
            }
            successorDSI.setExpectedStart(startTime);
            if (missingParams == 0) {
                if (successorDSI.getExpectedStart() <= timeLimit) {
                    selectableActions.offer(successor);
                } else {
                    readyActions.add(successor);
                }
            }
        }
        dsi.clearOptimizingSuccessors();
    }

}
