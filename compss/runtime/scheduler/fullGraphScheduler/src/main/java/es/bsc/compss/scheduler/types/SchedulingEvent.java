package es.bsc.compss.scheduler.types;

import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphResourceScheduler;
import es.bsc.es.bsc.compss.scheduler.fullGraphScheduler.FullGraphSchedulingInformation;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.PriorityQueue;


public abstract class SchedulingEvent<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
        implements Comparable<SchedulingEvent<P, T, I>> {

    protected long expectedTimeStamp;
    protected AllocatableAction<P, T, I> action;


    public SchedulingEvent(long timeStamp, AllocatableAction<P, T, I> action) {
        this.expectedTimeStamp = timeStamp;
        this.action = action;
    }

    @Override
    public int compareTo(SchedulingEvent<P, T, I> e) {
        int time = Long.compare(expectedTimeStamp, e.expectedTimeStamp);
        if (time == 0) {
            return (getPriority() - e.getPriority());
        }
        return time;
    }

    public AllocatableAction<P, T, I> getAction() {
        return action;
    }

    protected abstract int getPriority();

    public abstract LinkedList<SchedulingEvent<P, T, I>> process(LocalOptimizationState<P, T, I> state,
            FullGraphResourceScheduler<P, T, I> worker, PriorityQueue<AllocatableAction<P, T, I>> readyActions,
            PriorityActionSet<P, T, I> selectableActions, PriorityQueue<AllocatableAction<P, T, I>> rescheduledActions);


    public static class Start<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
            extends SchedulingEvent<P, T, I> {

        public Start(long timeStamp, AllocatableAction<P, T, I> action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 1;
        }

        @Override
        public String toString() {
            return action + " start @ " + expectedTimeStamp;
        }

        @Override
        public LinkedList<SchedulingEvent<P, T, I>> process(LocalOptimizationState<P, T, I> state,
                FullGraphResourceScheduler<P, T, I> worker, PriorityQueue<AllocatableAction<P, T, I>> readyActions,
                PriorityActionSet<P, T, I> selectableActions, PriorityQueue<AllocatableAction<P, T, I>> rescheduledActions) {

            LinkedList<SchedulingEvent<P, T, I>> enabledEvents = new LinkedList<>();
            FullGraphSchedulingInformation<P, T, I> dsi = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();

            // Set the expected Start time and endTime of the action
            dsi.setExpectedStart(expectedTimeStamp);
            long expectedEndTime = getExpectedEnd(action, worker, expectedTimeStamp);
            dsi.setExpectedEnd(expectedEndTime);
            // Add corresponding end event
            SchedulingEvent<P, T, I> endEvent = new End<>(expectedEndTime, action);
            enabledEvents.add(endEvent);

            // Remove resources from the state and fill the gaps before its execution
            dsi.clearPredecessors();
            dsi.clearSuccessors();
            LinkedList<Gap<P, T, I>> tmpGaps = state.reserveResources(action.getAssignedImplementation().getRequirements(),
                    expectedTimeStamp);

            for (Gap<P, T, I> tmpGap : tmpGaps) {
                AllocatableAction<P, T, I> gapAction = (AllocatableAction<P, T, I>) tmpGap.getOrigin();
                if (tmpGap.getInitialTime() == tmpGap.getEndTime()) {
                    if (gapAction != null) {
                        FullGraphSchedulingInformation<P, T, I> gapActionDSI = (FullGraphSchedulingInformation<P, T, I>) gapAction
                                .getSchedulingInfo();
                        gapActionDSI.addSuccessor(action);
                        dsi.addPredecessor(gapAction);
                        state.removeTmpGap(tmpGap);
                    }
                } else {
                    PriorityQueue<Gap<P, T, I>> outGaps = fillGap(worker, tmpGap, readyActions, selectableActions, rescheduledActions,
                            state);
                    for (Gap<P, T, I> outGap : outGaps) {
                        AllocatableAction<P, T, I> pred = (AllocatableAction<P, T, I>) outGap.getOrigin();
                        if (pred != null) {
                            FullGraphSchedulingInformation<P, T, I> predDSI = (FullGraphSchedulingInformation<P, T, I>) pred
                                    .getSchedulingInfo();
                            predDSI.addSuccessor(action);
                            dsi.addPredecessor(pred);
                        }
                        state.removeTmpGap(outGap);
                    }
                }
            }
            rescheduledActions.offer(action);
            return enabledEvents;
        }

        private PriorityQueue<Gap<P, T, I>> fillGap(FullGraphResourceScheduler<P, T, I> worker, Gap<P, T, I> gap,
                PriorityQueue<AllocatableAction<P, T, I>> readyActions, PriorityActionSet<P, T, I> selectableActions,
                PriorityQueue<AllocatableAction<P, T, I>> rescheduledActions, LocalOptimizationState<P, T, I> state) {

            // Find selected action predecessors
            PriorityQueue<Gap<P, T, I>> availableGaps = new PriorityQueue<>(1, new Comparator<Gap<P, T, I>>() {

                @Override
                public int compare(Gap<P, T, I> g1, Gap<P, T, I> g2) {
                    return Long.compare(g1.getInitialTime(), g2.getInitialTime());
                }

            });

            AllocatableAction<P, T, I> gapAction = pollActionForGap(gap, worker, selectableActions);

            if (gapAction != null) {
                // Compute method start
                FullGraphSchedulingInformation<P, T, I> gapActionDSI = (FullGraphSchedulingInformation<P, T, I>) gapAction
                        .getSchedulingInfo();
                gapActionDSI.setToReschedule(false);
                long gapActionStart = Math.max(gapActionDSI.getExpectedStart(), gap.getInitialTime());

                // Fill previous gap space
                if (gap.getInitialTime() != gapActionStart) {
                    Gap<P, T, I> previousGap = new Gap<>(gap.getInitialTime(), gapActionStart, gap.getOrigin(), gap.getResources(), 0);
                    state.replaceTmpGap(gap, previousGap);
                    availableGaps = fillGap(worker, previousGap, readyActions, selectableActions, rescheduledActions, state);
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
                    Gap<P, T, I> peekGap = availableGaps.peek();
                    AllocatableAction<P, T, I> peekAction = (AllocatableAction<P, T, I>) peekGap.getOrigin();
                    if (peekAction != null) {
                        FullGraphSchedulingInformation<P, T, I> predActionDSI = (FullGraphSchedulingInformation<P, T, I>) peekAction
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

                LinkedList<Gap<P, T, I>> extendedGaps = new LinkedList<Gap<P, T, I>>();
                // Fill Concurrent
                for (Gap<P, T, I> g : availableGaps) {
                    Gap<P, T, I> extendedGap = new Gap<>(g.getInitialTime(), gap.getEndTime(), g.getOrigin(), g.getResources(),
                            g.getCapacity());
                    state.replaceTmpGap(extendedGap, gap);
                    extendedGaps.add(extendedGap);
                }

                availableGaps.clear();
                for (Gap<P, T, I> eg : extendedGaps) {
                    availableGaps.addAll(fillGap(worker, eg, readyActions, selectableActions, rescheduledActions, state));
                }

                gapActionDSI.clearSuccessors();
                rescheduledActions.add(gapAction);

                gapActionDSI.setOnOptimization(false);
                // Release Data Successors
                releaseSuccessors(gapActionDSI, worker, readyActions, selectableActions, expectedEnd);

                // Fill Post action gap space
                Gap<P, T, I> actionGap = new Gap<>(expectedEnd, gap.getEndTime(), gapAction,
                        gapAction.getAssignedImplementation().getRequirements(), 0);
                state.addTmpGap(actionGap);
                availableGaps.addAll(fillGap(worker, actionGap, readyActions, selectableActions, rescheduledActions, state));
            } else {
                availableGaps.add(gap);
            }
            return availableGaps;
        }

        private long getExpectedEnd(AllocatableAction<P, T, I> action, FullGraphResourceScheduler<P, T, I> worker, long expectedStart) {
            I impl = action.getAssignedImplementation();
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

        private AllocatableAction<P, T, I> pollActionForGap(Gap<P, T, I> gap, FullGraphResourceScheduler<P, T, I> worker,
                PriorityActionSet<P, T, I> selectableActions) {

            AllocatableAction<P, T, I> gapAction = null;
            PriorityQueue<AllocatableAction<P, T, I>> peeks = selectableActions.peekAll();
            // Get Main action to fill the gap
            while (!peeks.isEmpty() && gapAction == null) {
                AllocatableAction<P, T, I> candidate = peeks.poll();
                // Check times
                FullGraphSchedulingInformation<P, T, I> candidateDSI = (FullGraphSchedulingInformation<P, T, I>) candidate
                        .getSchedulingInfo();
                long start = candidateDSI.getExpectedStart();
                if (start > gap.getEndTime()) {
                    continue;
                }
                I impl = candidate.getAssignedImplementation();
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

    public static class End<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>>
            extends SchedulingEvent<P, T, I> {

        public End(long timeStamp, AllocatableAction<P, T, I> action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 0;
        }

        @Override
        public LinkedList<SchedulingEvent<P, T, I>> process(LocalOptimizationState<P, T, I> state,
                FullGraphResourceScheduler<P, T, I> worker, PriorityQueue<AllocatableAction<P, T, I>> readyActions,
                PriorityActionSet<P, T, I> selectableActions, PriorityQueue<AllocatableAction<P, T, I>> rescheduledActions) {

            LinkedList<SchedulingEvent<P, T, I>> enabledEvents = new LinkedList<>();
            FullGraphSchedulingInformation<P, T, I> dsi = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
            dsi.setOnOptimization(false);

            // Move from readyActions to Ready
            while (readyActions.size() > 0) {
                AllocatableAction<P, T, I> top = readyActions.peek();
                FullGraphSchedulingInformation<P, T, I> topDSI = (FullGraphSchedulingInformation<P, T, I>) top.getSchedulingInfo();
                long start = topDSI.getExpectedStart();
                if (start > expectedTimeStamp) {
                    break;
                }
                readyActions.poll();
                selectableActions.offer(top);
            }

            // Detect released Actions
            releaseSuccessors(dsi, worker, readyActions, selectableActions, expectedTimeStamp);

            // Get Top Action
            AllocatableAction<P, T, I> currentTop = selectableActions.peek();

            if (state.getAction() != currentTop) {
                state.replaceAction(currentTop);
            }
            state.releaseResources(expectedTimeStamp, action);

            while (state.canActionRun()) {
                selectableActions.removeFirst(currentTop.getCoreId());
                FullGraphSchedulingInformation<P, T, I> topDSI = (FullGraphSchedulingInformation<P, T, I>) currentTop.getSchedulingInfo();
                topDSI.lock();
                topDSI.setToReschedule(false);
                SchedulingEvent<P, T, I> se = new Start<>(state.getActionStartTime(), currentTop);
                enabledEvents.addAll(se.process(state, worker, readyActions, selectableActions, rescheduledActions));

                currentTop = selectableActions.peek();
                state.replaceAction(currentTop);
            }
            return enabledEvents;
        }

        public String toString() {
            return action + " end @ " + expectedTimeStamp;
        }
    }


    public void releaseSuccessors(FullGraphSchedulingInformation<P, T, I> dsi, FullGraphResourceScheduler<P, T, I> worker,
            PriorityQueue<AllocatableAction<P, T, I>> readyActions, PriorityActionSet<P, T, I> selectableActions, long timeLimit) {

        LinkedList<AllocatableAction<P, T, I>> successors = dsi.getOptimizingSuccessors();
        for (AllocatableAction<P, T, I> successor : successors) {
            FullGraphSchedulingInformation<P, T, I> successorDSI = (FullGraphSchedulingInformation<P, T, I>) successor.getSchedulingInfo();
            int missingParams = 0;
            long startTime = 0;
            boolean retry = true;
            while (retry) {
                try {
                    LinkedList<AllocatableAction<P, T, I>> predecessors = successor.getDataPredecessors();
                    for (AllocatableAction<P, T, I> predecessor : predecessors) {
                        FullGraphSchedulingInformation<P, T, I> predDSI = ((FullGraphSchedulingInformation<P, T, I>) predecessor
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
