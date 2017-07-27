package es.bsc.compss.scheduler.multiobjective.types;

import es.bsc.compss.scheduler.multiobjective.MOResourceScheduler;
import es.bsc.compss.scheduler.multiobjective.MOSchedulingInformation;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Comparator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;


public abstract class SchedulingEvent implements Comparable<SchedulingEvent> {

    protected long expectedTimeStamp;
    protected AllocatableAction action;


    public SchedulingEvent(long timeStamp, AllocatableAction action) {
        this.expectedTimeStamp = timeStamp;
        this.action = action;
    }

    @Override
    public int compareTo(SchedulingEvent e) {
        int time = Long.compare(expectedTimeStamp, e.expectedTimeStamp);
        if (time == 0) {
            return (getPriority() - e.getPriority());
        }
        return time;
    }

    public AllocatableAction getAction() {
        return action;
    }

    protected abstract int getPriority();

    public abstract List<SchedulingEvent> process(LocalOptimizationState state, MOResourceScheduler<WorkerResourceDescription> worker,
            PriorityQueue<AllocatableAction> rescheduledActions);


    public static class Start extends SchedulingEvent {

        public Start(long timeStamp, AllocatableAction action) {
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
        public List<SchedulingEvent> process(LocalOptimizationState state, MOResourceScheduler<WorkerResourceDescription> worker,
                PriorityQueue<AllocatableAction> rescheduledActions) {

            List<SchedulingEvent> enabledEvents = new LinkedList<>();
            MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();

            // Set the expected Start time and endTime of the action
            dsi.setExpectedStart(expectedTimeStamp);
            long expectedEndTime = getExpectedEnd(action, worker, expectedTimeStamp);
            dsi.setExpectedEnd(expectedEndTime);
            // Add corresponding end event
            SchedulingEvent endEvent = new End(expectedEndTime, action);
            enabledEvents.add(endEvent);

            // Remove resources from the state and fill the gaps before its execution
            dsi.clearPredecessors();
            dsi.clearSuccessors();
            List<Gap> tmpGaps = state.reserveResources(action.getAssignedImplementation().getRequirements(), expectedTimeStamp);

            for (Gap tmpGap : tmpGaps) {
                AllocatableAction gapAction = tmpGap.getOrigin();
                if (expectedTimeStamp == tmpGap.getEndTime()) {
                    if (gapAction != null) {
                        MOSchedulingInformation gapActionDSI = (MOSchedulingInformation) gapAction.getSchedulingInfo();
                        gapActionDSI.addSuccessor(action);
                        dsi.addPredecessor(tmpGap);
                        state.removeTmpGap(tmpGap);
                    }
                } else {
                    PriorityQueue<Gap> outGaps = fillGap(worker, tmpGap, rescheduledActions, state);
                    for (Gap outGap : outGaps) {
                        AllocatableAction pred = outGap.getOrigin();
                        if (pred != null) {
                            MOSchedulingInformation predDSI = (MOSchedulingInformation) pred.getSchedulingInfo();
                            predDSI.addSuccessor(action);
                            dsi.addPredecessor(outGap);
                        }
                        state.removeTmpGap(outGap);
                    }
                }
            }
            rescheduledActions.offer(action);
            return enabledEvents;
        }

        private PriorityQueue<Gap> fillGap(MOResourceScheduler<WorkerResourceDescription> worker, Gap gap,
                PriorityQueue<AllocatableAction> rescheduledActions, LocalOptimizationState state) {
            // Find selected action predecessors
            PriorityQueue<Gap> availableGaps = new PriorityQueue<Gap>(1, new Comparator<Gap>() {

                @Override
                public int compare(Gap g1, Gap g2) {
                    return Long.compare(g1.getInitialTime(), g2.getInitialTime());
                }

            });

            AllocatableAction gapAction = state.pollActionForGap(gap);

            if (gapAction != null) {
                // Compute method start
                MOSchedulingInformation gapActionDSI = (MOSchedulingInformation) gapAction.getSchedulingInfo();
                gapActionDSI.setToReschedule(false);
                long gapActionStart = Math.max(gapActionDSI.getExpectedStart(), gap.getInitialTime());

                // Fill previous gap space
                if (gap.getInitialTime() != gapActionStart) {
                    Gap previousGap = new Gap(gap.getInitialTime(), gapActionStart, gap.getOrigin(), gap.getResources().copy(), 0);
                    state.replaceTmpGap(gap, previousGap);
                    availableGaps = fillGap(worker, previousGap, rescheduledActions, state);
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
                    if (peekGap != null) {
                        AllocatableAction peekAction = peekGap.getOrigin();
                        if (peekAction != null) {
                            MOSchedulingInformation predActionDSI = (MOSchedulingInformation) peekAction.getSchedulingInfo();
                            gapActionDSI.addPredecessor(peekGap);
                            predActionDSI.addSuccessor(gapAction);
                        }
                        ResourceDescription.reduceCommonDynamics(desc, peekGap.getResources());
                        if (peekGap.getResources().isDynamicUseless()) {
                            availableGaps.poll();
                            state.removeTmpGap(gap);
                        }
                    } else {
                        // I have added this if not if remains in the while
                        break;
                    }
                }

                List<Gap> extendedGaps = new LinkedList<>();
                // Fill Concurrent
                for (Gap g : availableGaps) {
                    Gap extendedGap = new Gap(g.getInitialTime(), gap.getEndTime(), g.getOrigin(), g.getResources(), g.getCapacity());
                    state.replaceTmpGap(extendedGap, gap);
                    extendedGaps.add(extendedGap);
                }

                availableGaps.clear();
                for (Gap eg : extendedGaps) {
                    availableGaps.addAll(fillGap(worker, eg, rescheduledActions, state));
                }

                gapActionDSI.clearSuccessors();
                rescheduledActions.add(gapAction);

                gapActionDSI.setOnOptimization(false);
                // Release Data Successors
                state.releaseDataSuccessors(gapActionDSI, expectedEnd);

                // Fill Post action gap space
                Gap actionGap = new Gap(expectedEnd, gap.getEndTime(), gapAction, gapAction.getAssignedImplementation().getRequirements(),
                        0);
                state.addTmpGap(actionGap);
                availableGaps.addAll(fillGap(worker, actionGap, rescheduledActions, state));
            } else {
                availableGaps.add(gap);
            }
            return availableGaps;
        }

        private long getExpectedEnd(AllocatableAction action, MOResourceScheduler<WorkerResourceDescription> worker, long expectedStart) {
            long theoreticalEnd;
            if (action.isToReleaseResources()) {
                Implementation impl = action.getAssignedImplementation();
                Profile p = worker.getProfile(impl);
                long endTime = expectedStart;
                if (p != null) {
                    endTime += p.getAverageExecutionTime();
                }
                if (endTime < 0) {
                    endTime = 0;
                }
                theoreticalEnd = endTime;
            } else {
                theoreticalEnd = Long.MAX_VALUE;
            }
            if (theoreticalEnd < expectedStart) {
                return Long.MAX_VALUE;
            } else {
                return theoreticalEnd;
            }
        }

    }

    public static class End extends SchedulingEvent {

        public End(long timeStamp, AllocatableAction action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 0;
        }

        @Override
        public List<SchedulingEvent> process(LocalOptimizationState state, MOResourceScheduler<WorkerResourceDescription> worker,
                PriorityQueue<AllocatableAction> rescheduledActions) {

            List<SchedulingEvent> enabledEvents = new LinkedList<>();
            MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
            dsi.setOnOptimization(false);

            // Move from readyActions to selectable
            state.progressOnTime(expectedTimeStamp);

            // Detect released Actions
            state.releaseDataSuccessors(dsi, expectedTimeStamp);

            // Get Top Action
            AllocatableAction currentTop = state.getMostPrioritaryRunnableAction();
            if (state.getAction() != currentTop) {
                state.replaceAction(currentTop);
            }
            state.releaseResources(expectedTimeStamp, action);
            state.updateConsumptions(action);

            while (state.canActionRun()) {
                state.removeMostPrioritaryRunnableAction(currentTop.getCoreId());
                MOSchedulingInformation topDSI = (MOSchedulingInformation) currentTop.getSchedulingInfo();
                topDSI.lock();
                topDSI.setToReschedule(false);
                if (action.isToReleaseResources()) {
                    SchedulingEvent se = new Start(state.getActionStartTime(), currentTop);
                    enabledEvents.addAll(se.process(state, worker, rescheduledActions));
                } else {
                    SchedulingEvent se = new ResourceBlocked(state.getActionStartTime(), currentTop);
                    enabledEvents.addAll(se.process(state, worker, rescheduledActions));
                }

                currentTop = state.getMostPrioritaryRunnableAction();
                state.replaceAction(currentTop);
            }
            return enabledEvents;
        }

        @Override
        public String toString() {
            return action + " end @ " + expectedTimeStamp;
        }
    }

    public static class ResourceBlocked extends SchedulingEvent {

        public ResourceBlocked(long timeStamp, AllocatableAction action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 0;
        }

        @Override
        public List<SchedulingEvent> process(LocalOptimizationState state, MOResourceScheduler<WorkerResourceDescription> worker,
                PriorityQueue<AllocatableAction> rescheduledActions) {
            MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
            dsi.setOnOptimization(false);
            dsi.clearPredecessors();
            dsi.clearSuccessors();
            dsi.setExpectedStart(Long.MAX_VALUE);
            dsi.setExpectedEnd(Long.MAX_VALUE);
            // Actions is registered as blocked because of lack of resources
            state.resourceBlockedAction(action);

            // Register all successors as Blocked Actions
            state.blockDataSuccessors(dsi);
            dsi.unlock();
            rescheduledActions.add(action);
            return new LinkedList<>();
        }

        @Override
        public String toString() {
            return action + " resourceBlocked";
        }
    }

}
