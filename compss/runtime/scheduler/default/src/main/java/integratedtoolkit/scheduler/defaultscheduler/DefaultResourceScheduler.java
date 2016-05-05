package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.types.Gap;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.DefaultScore;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.OptimizationElement;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.SchedulingEvent;
import integratedtoolkit.types.SchedulingEvent.StartEvent;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.TaskParams;
import integratedtoolkit.types.TokensWrapper;
import integratedtoolkit.types.resources.ResourceDescription;
import integratedtoolkit.types.resources.Worker;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceScheduler;

import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

public class DefaultResourceScheduler<P extends Profile, T extends WorkerResourceDescription> extends ResourceScheduler<P,T> {

    public static final long DATA_TRANSFER_DELAY = 200;
    private LinkedList<Gap> gaps;

    public DefaultResourceScheduler(Worker<T> w) {
        super(w);
        gaps = new LinkedList<Gap>();
        TokensWrapper capacity = new TokensWrapper(myWorker.getMaxTaskCount());
        gaps.add(new Gap(0, Long.MAX_VALUE, null, myWorker.getDescription().copy(), capacity.getFree()));
    }

    public Score getResourceScore(AllocatableAction<P,T> action, TaskParams params, Score actionScore) {
        long resScore = Score.getLocalityScore(params, myWorker);
        long lessTimeStamp = Long.MAX_VALUE;
        for (Gap g : gaps) {
            if (lessTimeStamp > g.getInitialTime()) {
                lessTimeStamp = g.getInitialTime();
            }
        }
        return new DefaultScore<P,T>((DefaultScore<P,T>) actionScore, resScore * DATA_TRANSFER_DELAY, lessTimeStamp, 0);
    }

    public Score getImplementationScore(AllocatableAction<P,T> action, TaskParams params, Implementation<T> impl, Score resourceScore) {
        ResourceDescription rd = impl.getRequirements().copy();
        long resourceFreeTime = 0;
        for (Gap g : gaps) {
            rd.reduceDynamic(g.getResources());
            if (rd.isDynamicUseless()) {
                resourceFreeTime = g.getInitialTime();
                break;
            }
        }
        long implScore = this.getProfile(impl).getAverageExecutionTime();
        //The data transfer penalty is already included on the datadependency time of the resourceScore
        return new DefaultScore<P,T>((DefaultScore<P,T>) resourceScore, 0, resourceFreeTime, implScore);
    }

    @Override
    public void initialSchedule(AllocatableAction<P,T> action, Implementation<T> impl) {
        Iterator<Gap> gapIt = gaps.iterator();
        ResourceDescription constraints = impl.getRequirements().copy();
        long expectedStart = 0;

        while (gapIt.hasNext()) {
            Gap gap = gapIt.next();
            AllocatableAction<P,T> predecessor = (AllocatableAction<P,T>) gap.getOrigin();
            if (predecessor != null) {
                long predEnd = ((DefaultSchedulingInformation<P,T>) predecessor.getSchedulingInfo()).getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
                addSchedulingDependency(predecessor, action);
            }
            ResourceDescription gapResource = gap.getResources();
            //ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(gapResource, constraints);
            ResourceDescription.reduceCommonDynamics(gapResource, constraints);
            if (gapResource.isDynamicUseless()) {
                gapIt.remove();
            }
            if (constraints.isDynamicUseless()) {
                break;
            }
        }

        DefaultSchedulingInformation<P,T> schedInfo = (DefaultSchedulingInformation<P,T>) action.getSchedulingInfo();
        Profile p = getProfile(impl);
        schedInfo.setExpectedStart(expectedStart);
        long expectedEnd = expectedStart + p.getAverageExecutionTime();
        schedInfo.setExpectedEnd(expectedEnd);
        Gap g = new Gap(expectedEnd, Long.MAX_VALUE, action, impl.getRequirements().copy(), 0);
        gapIt = gaps.iterator();
        int index = 0;
        while (gapIt.hasNext()) {
            Gap gap = gapIt.next();
            if (gap.getInitialTime() <= g.getInitialTime()) {
                index++;
            }
        }
        gaps.add(index, g);

    }

    public void addSchedulingDependency(AllocatableAction<P,T> predecessor, AllocatableAction<P,T> successor) {
        DefaultSchedulingInformation<P,T> dsiPred = (DefaultSchedulingInformation<P,T>) predecessor.getSchedulingInfo();
        DefaultSchedulingInformation<P,T> dsiSucc = (DefaultSchedulingInformation<P,T>) successor.getSchedulingInfo();
        if (predecessor.isPending()) {
            dsiSucc.addPredecessor(predecessor);
            dsiPred.addSuccessor(successor);
        }
    }

    @Override
    public LinkedList<AllocatableAction<P,T>> unscheduleAction(AllocatableAction<P,T> action) {
        LinkedList<AllocatableAction<P,T>> freeTasks = new LinkedList<AllocatableAction<P,T>>();
        DefaultSchedulingInformation<P,T> actionDSI = (DefaultSchedulingInformation<P,T>) action.getSchedulingInfo();

        //Remove action from predecessors
        for (AllocatableAction<P,T> pred : actionDSI.getPredecessors()) {
            DefaultSchedulingInformation<P,T> predDSI = (DefaultSchedulingInformation<P,T>) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        for (AllocatableAction<P,T> successor : actionDSI.getSuccessors()) {
            DefaultSchedulingInformation<P,T> successorDSI = (DefaultSchedulingInformation<P,T>) successor.getSchedulingInfo();
            //Remove predecessor
            successorDSI.removePredecessor(action);
            //Link with action predecessors
            for (AllocatableAction<P,T> predecessor : actionDSI.getPredecessors()) {
                addSchedulingDependency(predecessor, successor);
            }
            //Check executability
            if (successorDSI.isExecutable()) {
                freeTasks.add(successor);
            }
        }
        actionDSI.clearPredecessors();
        actionDSI.clearSuccessors();

        return freeTasks;
    }

    @Override
    public P generateProfileForAllocatable() {
        return (P) new Profile();
    }

    public PriorityQueue<OptimizationElement<?>>[] seekGaps(long updateId, LinkedList<Gap> gaps) {
        PriorityQueue<OptimizationElement<?>>[] actions = new PriorityQueue[CoreManager.getCoreCount()];
        for (int i = 0; i < actions.length; i++) {
            actions[i] = new PriorityQueue<OptimizationElement<?>>();
        }
        PriorityQueue<SchedulingEvent<P,T>> pq = new PriorityQueue<SchedulingEvent<P,T>>();
        LinkedList<Gap> resources = new LinkedList<Gap>();
        TokensWrapper capacity = new TokensWrapper(myWorker.getMaxTaskCount());
        resources.add(new Gap(0, null, myWorker.getDescription().copy(), capacity.getFree()));
        boolean retry = true;
        while (retry) {
            try {
                for (AllocatableAction<P,T> action : getHostedActions()) {
                    pq.offer(new StartEvent<P,T>(0, action));
                }
                retry = false;
            } catch (ConcurrentModificationException cme) {
                pq.clear();
            }
        }
        while (!pq.isEmpty()) {
            SchedulingEvent<P,T> e = pq.poll();
            LinkedList<SchedulingEvent<P,T>> result = e.process(updateId, this, resources, capacity, gaps, actions);
            for (SchedulingEvent<P,T> r : result) {
                pq.offer(r);
            }
        }
        this.gaps = new LinkedList<Gap>();
        for (Gap g : resources) {
            g.setEndTime(Long.MAX_VALUE);
            this.gaps.add(g);
            gaps.add(g);
        }
        return actions;
    }

    public long getPerformanceIndicator() {
        long maxTimeStamp = 0;
        for (Gap g : gaps) {
            if (maxTimeStamp < g.getInitialTime()) {
                maxTimeStamp = g.getInitialTime();
            }
        }
        return maxTimeStamp;
    }

    @Override
    public void clear() {
        super.clear();
        gaps.clear();
        TokensWrapper capacity = new TokensWrapper(myWorker.getMaxTaskCount());
        gaps.add(new Gap(0, Long.MAX_VALUE, null, myWorker.getDescription().copy(), capacity.getFree()));
    }

}
