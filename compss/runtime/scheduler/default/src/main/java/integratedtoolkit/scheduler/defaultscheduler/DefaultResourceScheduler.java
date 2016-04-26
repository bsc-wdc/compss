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
import integratedtoolkit.util.CoreManager;
import integratedtoolkit.util.ResourceScheduler;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;

public class DefaultResourceScheduler extends ResourceScheduler<Profile> {

    public static final long DATA_TRANSFER_DELAY = 200;
    private LinkedList<Gap> gaps;

    public DefaultResourceScheduler(Worker w) {
        super(w);
        gaps = new LinkedList<Gap>();
        TokensWrapper capacity = new TokensWrapper(myWorker.getMaxTaskCount());
        gaps.add(new Gap(0, Long.MAX_VALUE, null, myWorker.getDescription().copy(), capacity.getFree()));
    }

    public Score getResourceScore(AllocatableAction action, TaskParams params, Score actionScore) {
        long resScore = Score.getLocalityScore(params, myWorker);
        long lessTimeStamp = Long.MAX_VALUE;
        for (Gap g : gaps) {
            if (lessTimeStamp > g.getInitialTime()) {
                lessTimeStamp = g.getInitialTime();
            }
        }
        return new DefaultScore((DefaultScore) actionScore, resScore * DATA_TRANSFER_DELAY, lessTimeStamp, 0);
    }

    public Score getImplementationScore(AllocatableAction action, TaskParams params, Implementation impl, Score resourceScore) {
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
        return new DefaultScore((DefaultScore) resourceScore, 0, resourceFreeTime, implScore);
    }

    @Override
    public void initialSchedule(AllocatableAction action, Implementation impl) {
        Iterator<Gap> gapIt = gaps.iterator();
        ResourceDescription constraints = impl.getRequirements().copy();
        long expectedStart = 0;

        while (gapIt.hasNext()) {
            Gap gap = gapIt.next();
            AllocatableAction predecessor = gap.getOrigin();
            if (predecessor != null) {
                long predEnd = ((DefaultSchedulingInformation) predecessor.getSchedulingInfo()).getExpectedEnd();
                expectedStart = Math.max(expectedStart, predEnd);
                addSchedulingDependency(predecessor, action);
            }
            ResourceDescription gapResource = gap.getResources();
            ResourceDescription reduction = ResourceDescription.reduceCommonDynamics(gapResource, constraints);
            if (gapResource.isDynamicUseless()) {
                gapIt.remove();
            }
            if (constraints.isDynamicUseless()) {
                break;
            }
        }

        DefaultSchedulingInformation schedInfo = (DefaultSchedulingInformation) action.getSchedulingInfo();
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

    public void addSchedulingDependency(AllocatableAction predecessor, AllocatableAction successor) {
        DefaultSchedulingInformation dsiPred = (DefaultSchedulingInformation) predecessor.getSchedulingInfo();
        DefaultSchedulingInformation dsiSucc = (DefaultSchedulingInformation) successor.getSchedulingInfo();
        if (predecessor.isPending()) {
            dsiSucc.addPredecessor(predecessor);
            dsiPred.addSuccessor(successor);
        }
    }

    @Override
    public LinkedList<AllocatableAction> unscheduleAction(AllocatableAction action) {
        LinkedList<AllocatableAction> freeTasks = new LinkedList<AllocatableAction>();
        DefaultSchedulingInformation actionDSI = (DefaultSchedulingInformation) action.getSchedulingInfo();

        //Remove action from predecessors
        for (AllocatableAction pred : actionDSI.getPredecessors()) {
            DefaultSchedulingInformation predDSI = (DefaultSchedulingInformation) pred.getSchedulingInfo();
            predDSI.removeSuccessor(action);
        }

        for (AllocatableAction successor : actionDSI.getSuccessors()) {
            DefaultSchedulingInformation successorDSI = (DefaultSchedulingInformation) successor.getSchedulingInfo();
            //Remove predecessor
            successorDSI.removePredecessor(action);
            //Link with action predecessors
            for (AllocatableAction predecessor : actionDSI.getPredecessors()) {
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
    public Profile generateProfileForAllocatable() {
        return new Profile();
    }

    public PriorityQueue[] seekGaps(long updateId, LinkedList<Gap> gaps) {
        PriorityQueue[] actions = new PriorityQueue[CoreManager.getCoreCount()];
        for (int i = 0; i < actions.length; i++) {
            actions[i] = new PriorityQueue<OptimizationElement>();
        }
        PriorityQueue<SchedulingEvent> pq = new PriorityQueue();
        LinkedList<Gap> resources = new LinkedList<Gap>();
        TokensWrapper capacity = new TokensWrapper(myWorker.getMaxTaskCount());
        resources.add(new Gap(0, null, myWorker.getDescription().copy(), capacity.getFree()));
        boolean retry = true;
        while (retry) {
            try {
                for (AllocatableAction action : getHostedActions()) {
                    pq.offer(new StartEvent(0, action));
                }
                retry = false;
            } catch (ConcurrentModificationException cme) {
                pq.clear();
            }
        }
        while (!pq.isEmpty()) {
            SchedulingEvent e = pq.poll();
            LinkedList<SchedulingEvent> result = e.process(updateId, this, resources, capacity, gaps, actions);
            for (SchedulingEvent r : result) {
                pq.offer(r);
            }
        }
        this.gaps = new LinkedList();
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
