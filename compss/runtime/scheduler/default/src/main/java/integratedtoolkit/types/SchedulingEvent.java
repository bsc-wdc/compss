package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultResourceScheduler;
import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;

import java.util.LinkedList;
import java.util.PriorityQueue;

public abstract class SchedulingEvent<P extends Profile, T extends WorkerResourceDescription> implements Comparable<SchedulingEvent<P, T>> {

    public long expectedTimeStamp;
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

    public abstract LinkedList<SchedulingEvent<P, T>> process(
            LocalOptimizationState state,
            DefaultResourceScheduler<P, T> worker,
            PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions,
            PriorityQueue<AllocatableAction> rescheduledActions
    );

    public static class Start<P extends Profile, T extends WorkerResourceDescription> extends SchedulingEvent<P, T> {

        public Start(long timeStamp, AllocatableAction<P, T> action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 1;
        }

        public String toString() {
            return action + " start @ " + expectedTimeStamp;
        }

        @Override
        public LinkedList<SchedulingEvent<P, T>> process(
                LocalOptimizationState state,
                DefaultResourceScheduler<P, T> worker,
                PriorityQueue<AllocatableAction> readyActions,
                PriorityActionSet selectableActions,
                PriorityQueue<AllocatableAction> rescheduledActions
        ) {
            LinkedList<SchedulingEvent<P, T>> enabledEvents = new LinkedList<SchedulingEvent<P, T>>();
            DefaultSchedulingInformation<P, T> dsi = (DefaultSchedulingInformation<P, T>) action.getSchedulingInfo();

            //Set the expected Start time and endTime of the action
            dsi.setExpectedStart(expectedTimeStamp);
            Implementation<T> impl = action.getAssignedImplementation();
            Profile p = worker.getProfile(impl);
            long endTime = expectedTimeStamp;
            if (p != null) {
                endTime += p.getAverageExecutionTime();
            }
            if (endTime < 0) {
                endTime = 0;
            }
            dsi.setExpectedEnd(endTime);
            //Add corresponding end event
            SchedulingEvent<P, T> endEvent = new End<P, T>(endTime, action);
            enabledEvents.add(endEvent);

            //Remove resources from the state and fill the gaps before its execution
            dsi.clearPredecessors();
            ResourceDescription constraints = impl.getRequirements().copy();

            Gap gap;
            boolean hasNoPredecessors = true;
            while ((gap = state.peekFirstGap()) != null) {
                ResourceDescription gapResource = gap.getResources();
                AllocatableAction gapAction = gap.getOrigin();
                DefaultSchedulingInformation gapActionDSI = null;

                //Remove resources from the first gap
                ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(gapResource, constraints);
                if (gap.getInitialTime() > 0 && gap.getInitialTime() < expectedTimeStamp) {
                    //There's an empty space before the task start that can be filled with other tasks
                    Gap g = new Gap(gap.getInitialTime(), expectedTimeStamp, gapAction, reduction, 0);
                    if (gapAction != null) {
                        gapActionDSI = ((DefaultSchedulingInformation) gapAction.getSchedulingInfo());
                        gapActionDSI.addGap();
                    }
                    PriorityQueue<Gap> outGaps = fillGap(worker, g, readyActions, selectableActions, rescheduledActions);
                    for (Gap outGap : outGaps) {
                        AllocatableAction pred = outGap.getOrigin();
                        if (pred != null) {
                            DefaultSchedulingInformation predDSI = (DefaultSchedulingInformation) pred.getSchedulingInfo();
                            predDSI.addSuccessor(action);
                            dsi.addPredecessor(pred);
                            predDSI.removeGap();
                            if (!predDSI.hasGaps()) {
                                predDSI.unlock();
                            }
                            hasNoPredecessors = false;
                        }
                    }
                } else {
                    if (gapAction != null) {
                        gapActionDSI = (DefaultSchedulingInformation) gapAction.getSchedulingInfo();
                        gapActionDSI.addSuccessor(action);
                        dsi.addPredecessor(gapAction);
                        hasNoPredecessors = false;
                    }
                }

                //If the gap has been fully used
                if (gapResource.isDynamicUseless()) {
                    //Remove the gap
                    state.pollGap();
                    if (gapActionDSI != null) {
                        gapActionDSI.removeGap();
                        //If there are no more gaps for the origin action  
                        if (!gapActionDSI.hasGaps()) {
                            //unlock it since it won't have more successors
                            gapActionDSI.unlock();
                        }
                    }
                }

                if (constraints.isDynamicUseless()) {
                    //Task has all the needed resources.
                    break;
                }
            }
            rescheduledActions.offer(action);
            return enabledEvents;
        }

        private PriorityQueue<Gap> fillGap(
                DefaultResourceScheduler<P, T> worker,
                Gap gap,
                PriorityQueue<AllocatableAction> readyActions,
                PriorityActionSet selectableActions,
                PriorityQueue<AllocatableAction> rescheduledActions
        ) {

            //Find  selected action predecessors
            PriorityQueue<Gap> availableGaps = new PriorityQueue(1, new Comparator<Gap>() {
                @Override
                public int compare(Gap g1, Gap g2) {
                    return Long.compare(g1.getInitialTime(), g2.getInitialTime());
                }

            });

            PriorityQueue<AllocatableAction> peeks = selectableActions.peekAll();
            AllocatableAction gapAction = null;

            //Get Main action to fill the gap
            while (!peeks.isEmpty() && gapAction == null) {
                AllocatableAction candidate = peeks.poll();
                //Check times
                DefaultSchedulingInformation candidateDSI = (DefaultSchedulingInformation) candidate.getSchedulingInfo();
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

                //Check description
                if (gap.getResources().canHostDynamic(impl)) {
                    gapAction = candidate;
                }
            }

            if (gapAction != null) {
                selectableActions.removePeek(gapAction.getCoreId());

                //Compute method start
                DefaultSchedulingInformation gapActionDSI = (DefaultSchedulingInformation) gapAction.getSchedulingInfo();

                long gapActionStart = Math.max(gapActionDSI.getExpectedStart(), gap.getInitialTime());

                //Fill previous gap space
                if (gap.getInitialTime() != gapActionStart) {
                    Gap previousGap = new Gap(gap.getInitialTime(), gapActionStart, gap.getOrigin(), gap.getResources(), 0);
                    if (gap.getOrigin() != null) {
                        DefaultSchedulingInformation gapOriginDSI = (DefaultSchedulingInformation) gap.getOrigin().getSchedulingInfo();
                        gapOriginDSI.addGap();
                    }
                    availableGaps = fillGap(worker, previousGap, readyActions, selectableActions, rescheduledActions);
                } else {
                    availableGaps.add(gap);
                }

                gapActionDSI.lock();
                //Update Information
                gapActionDSI.setExpectedStart(gapActionStart);
                Implementation<T> impl = gapAction.getAssignedImplementation();
                Profile p = worker.getProfile(impl);
                long expectedLength = p.getAverageExecutionTime();
                gapActionDSI.setExpectedEnd(gapActionStart + expectedLength);
                gapActionDSI.clearPredecessors();
                ResourceDescription desc = impl.getRequirements().copy();
                while (!desc.isDynamicUseless()) {
                    Gap peekGap = availableGaps.peek();
                    AllocatableAction peekAction = peekGap.getOrigin();
                    if (peekAction != null) {
                        DefaultSchedulingInformation predActionDSI = (DefaultSchedulingInformation) peekAction.getSchedulingInfo();
                        gapActionDSI.addPredecessor(peekAction);
                        predActionDSI.addSuccessor(gapAction);
                    }
                    ResourceDescription.reduceCommonDynamics(desc, peekGap.getResources());
                    if (peekGap.getResources().isDynamicUseless()) {
                        availableGaps.poll();
                        if (peekAction != null) {
                            DefaultSchedulingInformation predActionDSI = (DefaultSchedulingInformation) peekAction.getSchedulingInfo();
                            predActionDSI.removeGap();
                            if (!predActionDSI.hasGaps()) {
                                predActionDSI.unlock();
                            }
                        }
                    }
                }

                //Fill Concurrent 
                for (Gap g : availableGaps) {
                    availableGaps = fillGap(worker, g, readyActions, selectableActions, rescheduledActions);
                }

                gapActionDSI.clearSuccessors();
                rescheduledActions.add(gapAction);
                //Release Data Successors
                releaseSuccessors(gapActionDSI, worker, readyActions, selectableActions, gapActionStart + expectedLength);

                //Fill Post action gap space
                Gap actionGap = new Gap(gapActionStart + expectedLength, gap.getEndTime(), gapAction, gap.getResources(), 0);
                gapActionDSI.addGap();
                availableGaps.addAll(fillGap(worker, actionGap, readyActions, selectableActions, rescheduledActions));
            } else {
                availableGaps.add(gap);
            }
            return availableGaps;
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
        public LinkedList<SchedulingEvent<P, T>> process(
                LocalOptimizationState state,
                DefaultResourceScheduler<P, T> worker,
                PriorityQueue<AllocatableAction> readyActions,
                PriorityActionSet selectableActions,
                PriorityQueue<AllocatableAction> rescheduledActions
        ) {
            LinkedList<SchedulingEvent<P, T>> enabledEvents = new LinkedList<SchedulingEvent<P, T>>();

            DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) action.getSchedulingInfo();
            dsi.clearSuccessors();
            dsi.setOnOptimization(false);
            Gap g = new Gap(expectedTimeStamp, Long.MAX_VALUE, action, action.getAssignedImplementation().getRequirements(), 0);
            dsi.addGap();
            state.addGap(g);

            //Move from readyActions to Ready
            while (readyActions.size() > 0) {
                AllocatableAction top = readyActions.peek();
                DefaultSchedulingInformation topDSI = (DefaultSchedulingInformation) top.getSchedulingInfo();
                long start = topDSI.getExpectedStart();
                if (start > expectedTimeStamp) {
                    break;
                }
                readyActions.poll();
                selectableActions.offer(top);
            }

            //Detect released Actions
            releaseSuccessors(dsi, worker, readyActions, selectableActions, expectedTimeStamp);

            //Include new Gap on the available resources
            while (true) {
                //Check the previous gaps with the current Top
                AllocatableAction currentTop = selectableActions.peek();
                //Check if there is a new peek.
                if (state.getTopAction() != currentTop) {
                    //Check if the new top fits in all resources
                    state.setTopAction(currentTop);
                } else {
                    //Check if the gap is enough for the missing top requirements
                    state.updatedResources(g);
                }

                if (state.canTopRun()) {
                    selectableActions.poll();
                    //Start the current action
                    DefaultSchedulingInformation succDSI = (DefaultSchedulingInformation) currentTop.getSchedulingInfo();
                    succDSI.lock();
                    SchedulingEvent se = new Start(state.getTopStartTime(), currentTop);
                    enabledEvents.addAll(se.process(state, worker, readyActions, selectableActions, rescheduledActions));
                } else {
                    break;
                }
            }
            return enabledEvents;
        }

        public String toString() {
            return action + " end @ " + expectedTimeStamp;
        }
    }

    public void releaseSuccessors(
            DefaultSchedulingInformation dsi,
            DefaultResourceScheduler worker,
            PriorityQueue<AllocatableAction> readyActions,
            PriorityActionSet selectableActions,
            long timeLimit) {

        LinkedList<AllocatableAction<P, T>> successors = dsi.getOptimizingSuccessors();
        for (AllocatableAction successor : successors) {
            DefaultSchedulingInformation<P, T> successorDSI = (DefaultSchedulingInformation<P, T>) successor.getSchedulingInfo();
            int missingParams = 0;
            long startTime = 0;
            boolean retry = true;
            while (retry) {
                try {
                    LinkedList<AllocatableAction> predecessors = successor.getDataPredecessors();
                    for (AllocatableAction predecessor : predecessors) {
                        DefaultSchedulingInformation predDSI = ((DefaultSchedulingInformation) predecessor.getSchedulingInfo());
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
