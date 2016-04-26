package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultResourceScheduler;
import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.ResourceDescription;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
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
        int time = (int) (expectedTimeStamp - e.expectedTimeStamp);
        if (time == 0) {
            return (getPriority() - e.getPriority());
        }
        return time;
    }

    protected abstract int getPriority();

    public abstract LinkedList<SchedulingEvent> process(long updateId, DefaultResourceScheduler worker, LinkedList<Gap> resources, TokensWrapper capacity, LinkedList<Gap> gaps, PriorityQueue[] actions);

    public static class StartEvent extends SchedulingEvent {

        public StartEvent(long timeStamp, AllocatableAction action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 1;
        }

        @Override
        public LinkedList<SchedulingEvent> process(long updateId, DefaultResourceScheduler worker, LinkedList<Gap> resources, TokensWrapper capacity, LinkedList<Gap> gaps, PriorityQueue[] actions) {
            LinkedList<SchedulingEvent> enabledEvents = new LinkedList<SchedulingEvent>();

            DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) action.getSchedulingInfo();
            Implementation impl = action.getAssignedImplementation();

            Profile p = worker.getProfile(impl);
            long endime = expectedTimeStamp + p.getAverageExecutionTime();
            dsi.setExpectedStart(expectedTimeStamp);
            Integer coreId = action.getCoreId();
            if (coreId != null) {
                actions[coreId].offer(new OptimizationElement(action, expectedTimeStamp));
            }
            dsi.setExpectedEnd(endime);

            //Find usable gaps.
            capacity.addTask();
            ResourceDescription constraints = impl.getRequirements().copy();
            Iterator<Gap> gapIt = resources.iterator();
            while (gapIt.hasNext()) {
                Gap gap = gapIt.next();
                ResourceDescription gapResource = gap.getResources();
                ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(gapResource, constraints);
                if (gap.getInitialTime() != expectedTimeStamp) {
                    Gap g = new Gap(gap.getInitialTime(), expectedTimeStamp, gap.getOrigin(), reduction, capacity.getFree());
                    gaps.add(g);
                }
                if (gapResource.isDynamicUseless()) {
                    gapIt.remove();
                }
                if (constraints.isDynamicUseless()) {
                    break;
                }
            }

            SchedulingEvent endEvent = new EndEvent(endime, action);
            enabledEvents.add(endEvent);

            return enabledEvents;
        }

        public String toString() {
            return action + " start @ " + expectedTimeStamp;
        }
    }

    public static class EndEvent extends SchedulingEvent {

        public EndEvent(long timeStamp, AllocatableAction action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 0;
        }

        @Override
        public LinkedList<SchedulingEvent> process(long updateId, DefaultResourceScheduler worker, LinkedList<Gap> freeResources, TokensWrapper capacity, LinkedList<Gap> gaps, PriorityQueue[] actions) {
            LinkedList<SchedulingEvent> enabledEvents = new LinkedList<SchedulingEvent>();

            DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) action.getSchedulingInfo();
            dsi.setLastUpdate(updateId);

            //Enable start events for those successor whose predecessors have run
            try {
                for (AllocatableAction successor : dsi.getSuccessors()) {
                    boolean launchable = true;
                    DefaultSchedulingInformation successorDSI = (DefaultSchedulingInformation) successor.getSchedulingInfo();
                    for (AllocatableAction predecessor : successorDSI.getPredecessors()) {
                        DefaultSchedulingInformation predecessorDSI = (DefaultSchedulingInformation) predecessor.getSchedulingInfo();
                        boolean predDone = predecessorDSI.getLastUpdate() == updateId;
                        launchable = launchable && predDone;
                    }
                    if (launchable) {
                        long startTime = expectedTimeStamp;
                        for (AllocatableAction predecessor : successor.getDataPredecessors()) {
                            long predEnd = ((DefaultSchedulingInformation) predecessor.getSchedulingInfo()).getExpectedEnd();
                            startTime = Math.max(startTime, predEnd);
                        }
                        enabledEvents.add(new StartEvent(startTime, successor));
                    }
                }
            } catch (ConcurrentModificationException cme) {
                enabledEvents.clear();
            }

            Implementation impl = action.getAssignedImplementation();
            capacity.removeTask();
            Gap g = new Gap(expectedTimeStamp, action, impl.getRequirements().copy(), capacity.getFree());
            freeResources.addFirst(g);
            return enabledEvents;
        }

        public String toString() {
            return action + " end @ " + expectedTimeStamp;
        }
    }

}
