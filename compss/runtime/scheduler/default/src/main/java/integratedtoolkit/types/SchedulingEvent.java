package integratedtoolkit.types;

import integratedtoolkit.scheduler.defaultscheduler.DefaultResourceScheduler;
import integratedtoolkit.scheduler.defaultscheduler.DefaultSchedulingInformation;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.WorkerResourceDescription;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;


public abstract class SchedulingEvent<P extends Profile, T extends WorkerResourceDescription> implements Comparable<SchedulingEvent<P,T>> {

    protected long expectedTimeStamp;
    protected AllocatableAction<P,T> action;

    public SchedulingEvent(long timeStamp, AllocatableAction<P,T> action) {
        this.expectedTimeStamp = timeStamp;
        this.action = action;
    }

    @Override
    public int compareTo(SchedulingEvent<P,T> e) {
        int time = (int) (expectedTimeStamp - e.expectedTimeStamp);
        if (time == 0) {
            return (getPriority() - e.getPriority());
        }
        return time;
    }

    protected abstract int getPriority();

    public abstract LinkedList<SchedulingEvent<P,T>> process(long updateId, DefaultResourceScheduler<P,T> worker, 
    		LinkedList<Gap> resources, TokensWrapper capacity, LinkedList<Gap> gaps, PriorityQueue[] actions);

    
    public static class StartEvent<P extends Profile, T extends WorkerResourceDescription> extends SchedulingEvent<P,T> {

        public StartEvent(long timeStamp, AllocatableAction<P,T> action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 1;
        }

        @Override
        public LinkedList<SchedulingEvent<P,T>> process(long updateId, DefaultResourceScheduler<P,T> worker, 
        		LinkedList<Gap> resources, TokensWrapper capacity, LinkedList<Gap> gaps, PriorityQueue[] actions) {
        	
            LinkedList<SchedulingEvent<P,T>> enabledEvents = new LinkedList<SchedulingEvent<P,T>>();

            DefaultSchedulingInformation<P,T> dsi = (DefaultSchedulingInformation<P,T>) action.getSchedulingInfo();
            Implementation<T> impl = action.getAssignedImplementation();

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

            SchedulingEvent<P,T> endEvent = new EndEvent<P,T>(endime, action);
            enabledEvents.add(endEvent);

            return enabledEvents;
        }

        public String toString() {
            return action + " start @ " + expectedTimeStamp;
        }
    }

    public static class EndEvent<P extends Profile, T extends WorkerResourceDescription> extends SchedulingEvent<P,T> {

        public EndEvent(long timeStamp, AllocatableAction<P,T> action) {
            super(timeStamp, action);
        }

        @Override
        protected int getPriority() {
            return 0;
        }

        @Override
        public LinkedList<SchedulingEvent<P,T>> process(long updateId, DefaultResourceScheduler<P,T> worker, 
        		LinkedList<Gap> freeResources, TokensWrapper capacity, LinkedList<Gap> gaps, PriorityQueue[] actions) {
        	
            LinkedList<SchedulingEvent<P,T>> enabledEvents = new LinkedList<SchedulingEvent<P,T>>();

            DefaultSchedulingInformation<P,T> dsi = (DefaultSchedulingInformation<P,T>) action.getSchedulingInfo();
            dsi.setLastUpdate(updateId);

            //Enable start events for those successor whose predecessors have run
            try {
                for (AllocatableAction<P,T> successor : dsi.getSuccessors()) {
                    boolean launchable = true;
                    DefaultSchedulingInformation<P,T> successorDSI = (DefaultSchedulingInformation<P,T>) successor.getSchedulingInfo();
                    for (AllocatableAction<P,T> predecessor : successorDSI.getPredecessors()) {
                        DefaultSchedulingInformation<P,T> predecessorDSI = (DefaultSchedulingInformation<P,T>) predecessor.getSchedulingInfo();
                        boolean predDone = predecessorDSI.getLastUpdate() == updateId;
                        launchable = launchable && predDone;
                    }
                    if (launchable) {
                        long startTime = expectedTimeStamp;
                        for (AllocatableAction<P,T> predecessor : successor.getDataPredecessors()) {
                            long predEnd = ((DefaultSchedulingInformation<P,T>) predecessor.getSchedulingInfo()).getExpectedEnd();
                            startTime = Math.max(startTime, predEnd);
                        }
                        enabledEvents.add(new StartEvent<P,T>(startTime, successor));
                    }
                }
            } catch (ConcurrentModificationException cme) {
                enabledEvents.clear();
            }

            Implementation<?> impl = action.getAssignedImplementation();
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
