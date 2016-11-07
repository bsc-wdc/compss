package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultResourceScheduler;
import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.LinkedList;
import java.util.PriorityQueue;


public abstract class SchedulingEvent<P extends Profile, T extends WorkerResourceDescription> implements Comparable<SchedulingEvent<P, T>> {

    protected long expectedTimeStamp;
    protected AllocatableAction<P, T> action;


    public SchedulingEvent(long timeStamp, AllocatableAction<P, T> action) {
        this.expectedTimeStamp = timeStamp;
        this.action = action;
    }

    @Override
    public int compareTo(SchedulingEvent<P, T> e) {
        int time = Long.compare(expectedTimeStamp, e.expectedTimeStamp);
        if (time == 0) {
            return (getPriority() - e.getPriority());
        }
        return time;
    }

    public AllocatableAction<P, T> getAction() {
        return action;
    }

    protected abstract int getPriority();

    public abstract LinkedList<SchedulingEvent<P, T>> process(LocalOptimizationState state, DefaultResourceScheduler<P, T> worker,
            PriorityQueue<AllocatableAction<P, T>> readyActions, PriorityActionSet<P, T> selectableActions,
            PriorityQueue<AllocatableAction<P, T>> rescheduledActions);


    public static class Start<P extends Profile, T extends WorkerResourceDescription> extends SchedulingEvent<P, T> {

        public Start(long timeStamp, AllocatableAction<P, T> action) {
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
        public LinkedList<SchedulingEvent<P, T>> process(LocalOptimizationState state, DefaultResourceScheduler<P, T> worker,
                PriorityQueue<AllocatableAction<P, T>> readyActions, PriorityActionSet<P, T> selectableActions,
                PriorityQueue<AllocatableAction<P, T>> rescheduledActions) {
            
            LinkedList<SchedulingEvent<P, T>> enabledEvents = new LinkedList<>();
            DefaultSchedulingInformation<P, T> dsi = (DefaultSchedulingInformation<P, T>) action.getSchedulingInfo();

            // Set the expected Start time and endTime of the action
            dsi.setExpectedStart(expectedTimeStamp);
            long expectedEndTime = getExpectedEnd(action, worker, expectedTimeStamp);
            dsi.setExpectedEnd(expectedEndTime);
            // Add corresponding end event
            SchedulingEvent<P, T> endEvent = new End<>(expectedEndTime, action);
            enabledEvents.add(endEvent);

            // Remove resources from the state and fill the gaps before its execution
            dsi.clearPredecessors();
            dsi.clearSuccessors();
            LinkedList<Gap> tmpGaps = state.reserveResources(action.getAssignedImplementation().getRequirements(), expectedTimeStamp);

            for (Gap tmpGap : tmpGaps) {
                AllocatableAction<P, T> gapAction = (AllocatableAction<P, T>) tmpGap.getOrigin();
                if (tmpGap.getInitialTime() == tmpGap.getEndTime()) {
                    if (gapAction != null) {
                        DefaultSchedulingInformation<P, T> gapActionDSI = (DefaultSchedulingInformation<P, T>) gapAction
                                .getSchedulingInfo();
                        gapActionDSI.addSuccessor(action);
                        dsi.addPredecessor(gapAction);
                        state.removeTmpGap(tmpGap);
                    }
                } else {
                    PriorityQueue<Gap> outGaps = fillGap(worker, tmpGap, readyActions, selectableActions, rescheduledActions, state);
                    for (Gap outGap : outGaps) {
                        AllocatableAction<P, T> pred = (AllocatableAction<P, T>) outGap.getOrigin();
                        if (pred != null) {
                            DefaultSchedulingInformation<P, T> predDSI = (DefaultSchedulingInformation<P, T>) pred.getSchedulingInfo();
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

        private PriorityQueue<Gap> fillGap(DefaultResourceScheduler<P, T> worker, Gap gap,
                PriorityQueue<AllocatableAction<P, T>> readyActions, PriorityActionSet<P, T> selectableActions,
                PriorityQueue<AllocatableAction<P, T>> rescheduledActions, LocalOptimizationState state) {
            
            // Find selected action predecessors
            PriorityQueue<Gap> availableGaps = new PriorityQueue<>(1, new Comparator<Gap>() {

                @Override
                public int compare(Gap g1, Gap g2) {
                    return Long.compare(g1.getInitialTime(), g2.getInitialTime());
                }

            });

            AllocatableAction<P, T> gapAction = pollActionForGap(gap, worker, selectableActions);

            if (gapAction != null) {
                // Compute method start
                DefaultSchedulingInformation<P, T> gapActionDSI = (DefaultSchedulingInformation<P, T>) gapAction.getSchedulingInfo();
                gapActionDSI.setToReschedule(false);
                long gapActionStart = Math.max(gapActionDSI.getExpectedStart(), gap.getInitialTime());

                // Fill previous gap space
                if (gap.getInitialTime() != gapActionStart) {
                    Gap previousGap = new Gap(gap.getInitialTime(), gapActionStart, gap.getOrigin(), gap.getResources(), 0);
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
                    Gap peekGap = availableGaps.peek();
                    AllocatableAction<P, T> peekAction = (AllocatableAction<P, T>) peekGap.getOrigin();
                    if (peekAction != null) {
                        DefaultSchedulingInformation<P, T> predActionDSI = (DefaultSchedulingInformation<P, T>) peekAction
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

                LinkedList<Gap> extendedGaps = new LinkedList<Gap>();
                // Fill Concurrent
                for (Gap g : availableGaps) {
                    Gap extendedGap = new Gap(g.getInitialTime(), gap.getEndTime(), g.getOrigin(), g.getResources(), g.getCapacity());
                    state.replaceTmpGap(extendedGap, gap);
                    extendedGaps.add(extendedGap);
                }

                availableGaps.clear();
                for (Gap eg : extendedGaps) {
                    availableGaps.addAll(fillGap(worker, eg, readyActions, selectableActions, rescheduledActions, state));
                }

                gapActionDSI.clearSuccessors();
                rescheduledActions.add(gapAction);

                gapActionDSI.setOnOptimization(false);
                // Release Data Successors
                releaseSuccessors(gapActionDSI, worker, readyActions, selectableActions, expectedEnd);

                // Fill Post action gap space
                Gap actionGap = new Gap(expectedEnd, gap.getEndTime(), gapAction, gapAction.getAssignedImplementation().getRequirements(),
                        0);
                state.addTmpGap(actionGap);
                availableGaps.addAll(fillGap(worker, actionGap, readyActions, selectableActions, rescheduledActions, state));
            } else {
                availableGaps.add(gap);
            }
            return availableGaps;
        }

        private long getExpectedEnd(AllocatableAction<P, T> action, DefaultResourceScheduler<P, T> worker, long expectedStart) {
            Implementation<T> impl = action.getAssignedImplementation();
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

        private AllocatableAction<P, T> pollActionForGap(Gap gap, DefaultResourceScheduler<P, T> worker,
                PriorityActionSet<P, T> selectableActions) {

            AllocatableAction<P, T> gapAction = null;
            PriorityQueue<AllocatableAction<P, T>> peeks = selectableActions.peekAll();
            // Get Main action to fill the gap
            while (!peeks.isEmpty() && gapAction == null) {
                AllocatableAction<P, T> candidate = peeks.poll();
                // Check times
                DefaultSchedulingInformation<P, T> candidateDSI = (DefaultSchedulingInformation<P, T>) candidate.getSchedulingInfo();
                long start = candidateDSI.getExpectedStart();
                if (start > gap.getEndTime()) {
                    continue;
                }
                Implementation<T> impl = candidate.getAssignedImplementation();
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

    public static class End<P extends Profile, T extends WorkerResourceDescription> extends SchedulingEvent<P, T> {

        public End(long timeStamp, AllocatableAction<P, T> action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 0;
        }

        @Override
        public LinkedList<SchedulingEvent<P, T>> process(LocalOptimizationState state, DefaultResourceScheduler<P, T> worker,
                PriorityQueue<AllocatableAction<P, T>> readyActions, PriorityActionSet<P, T> selectableActions,
                PriorityQueue<AllocatableAction<P, T>> rescheduledActions) {
            
            LinkedList<SchedulingEvent<P, T>> enabledEvents = new LinkedList<>();
            DefaultSchedulingInformation<P, T> dsi = (DefaultSchedulingInformation<P, T>) action.getSchedulingInfo();
            dsi.setOnOptimization(false);

            // Move from readyActions to Ready
            while (readyActions.size() > 0) {
                AllocatableAction<P, T> top = readyActions.peek();
                DefaultSchedulingInformation<P, T> topDSI = (DefaultSchedulingInformation<P, T>) top.getSchedulingInfo();
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
            AllocatableAction<P, T> currentTop = selectableActions.peek();

            if (state.getAction() != currentTop) {
                state.replaceAction(currentTop);
            }
            state.releaseResources(expectedTimeStamp, action);

            while (state.canActionRun()) {
                selectableActions.removeFirst(currentTop.getCoreId());
                DefaultSchedulingInformation<P, T> topDSI = (DefaultSchedulingInformation<P, T>) currentTop.getSchedulingInfo();
                topDSI.lock();
                topDSI.setToReschedule(false);
                SchedulingEvent<P, T> se = new Start(state.getActionStartTime(), currentTop);
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


    public void releaseSuccessors(DefaultSchedulingInformation<P, T> dsi, DefaultResourceScheduler<P, T> worker,
            PriorityQueue<AllocatableAction<P, T>> readyActions, PriorityActionSet<P, T> selectableActions, long timeLimit) {

        LinkedList<AllocatableAction<P, T>> successors = dsi.getOptimizingSuccessors();
        for (AllocatableAction<P, T> successor : successors) {
            DefaultSchedulingInformation<P, T> successorDSI = (DefaultSchedulingInformation<P, T>) successor.getSchedulingInfo();
            int missingParams = 0;
            long startTime = 0;
            boolean retry = true;
            while (retry) {
                try {
                    LinkedList<AllocatableAction<P, T>> predecessors = successor.getDataPredecessors();
                    for (AllocatableAction<P, T> predecessor : predecessors) {
                        DefaultSchedulingInformation<P, T> predDSI = ((DefaultSchedulingInformation<P, T>) predecessor.getSchedulingInfo());
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
