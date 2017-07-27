package es.bsc.compss.scheduler.fullGraphScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.es.bsc.compss.scheduler.types.FullGraphScore;
import es.bsc.es.bsc.compss.scheduler.types.OptimizationWorker;
import es.bsc.es.bsc.compss.scheduler.types.Profile;
import es.bsc.es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;


public class ScheduleOptimizer<P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> extends Thread {

    private static final long OPTIMIZATION_THRESHOLD = 5_000;

    private FullGraphScheduler<P, T, I> scheduler;
    private boolean stop = false;
    private Semaphore sem = new Semaphore(0);

    private FullGraphScore<P, T, I> dummyScore = new FullGraphScore<>(0, 0, 0, 0, 0);


    /**
     * Construct a new scheduler optimizer for a given scheduler
     * 
     * @param scheduler
     */
    public ScheduleOptimizer(FullGraphScheduler<P, T, I> scheduler) {
        this.setName("ScheduleOptimizer");
        this.scheduler = scheduler;
    }

    @Override
    public void run() {
        long lastUpdate = System.currentTimeMillis();
        try {
            Thread.sleep(500);
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
        while (!stop) {
            long optimizationTS = System.currentTimeMillis();
            Collection<ResourceScheduler<P, T, I>> workers = scheduler.getWorkers();
            globalOptimization(optimizationTS, workers);
            waitForNextIteration(lastUpdate);
            lastUpdate = optimizationTS;
        }
        sem.release();
    }

    /**
     * Shutdown the process
     * 
     * @throws InterruptedException
     */
    public void shutdown() throws InterruptedException {
        stop = true;
        this.interrupt();
        sem.acquire();
    }

    private void waitForNextIteration(long lastUpdate) {
        long difference = OPTIMIZATION_THRESHOLD - (System.currentTimeMillis() - lastUpdate);
        if (difference > 0) {
            try {
                Thread.sleep(difference);
            } catch (InterruptedException ie) {
                // Do nothing. Wake up in case of shutdown received
            }
        }
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     --------------- Local  optimization ---------------
     ---------------------------------------------------
     --------------------------------------------------*/
    @SuppressWarnings("unchecked")
    public void globalOptimization(long optimizationTS, Collection<ResourceScheduler<P, T, I>> workers) {
        int workersCount = workers.size();
        if (workersCount == 0) {
            return;
        }
        OptimizationWorker<P, T, I>[] optimizedWorkers = new OptimizationWorker[workersCount];
        LinkedList<OptimizationWorker<P, T, I>> receivers = new LinkedList<>();

        int index = 0;
        for (ResourceScheduler<P, T, I> rs : workers) {
            optimizedWorkers[index] = new OptimizationWorker<>((FullGraphResourceScheduler<P, T, I>) rs);
            index = index + 1;
        }

        boolean hasDonated = true;
        while (hasDonated) {
            optimizationTS = System.currentTimeMillis();
            hasDonated = false;

            // Perform local optimizations
            for (int i = 0; i < workersCount; i++) {
                optimizedWorkers[i].localOptimization(optimizationTS);
            }

            OptimizationWorker<P, T, I> donor = determineDonorAndReceivers(optimizedWorkers, receivers);

            while (!hasDonated) {
                AllocatableAction<P, T, I> candidate = donor.pollDonorAction();
                if (candidate == null) {
                    break;
                }
                Iterator<OptimizationWorker<P, T, I>> recIt = receivers.iterator();
                while (recIt.hasNext()) {
                    OptimizationWorker<P, T, I> receiver = recIt.next();
                    if (move(candidate, donor, receiver)) {
                        hasDonated = true;
                    }
                }
            }

        }
    }

    public OptimizationWorker<P, T, I> determineDonorAndReceivers(OptimizationWorker<P, T, I>[] workers,
            LinkedList<OptimizationWorker<P, T, I>> receivers) {

        receivers.clear();
        PriorityQueue<OptimizationWorker<P, T, I>> receiversPQ = new PriorityQueue<>(1, getReceptionComparator());
        long topIndicator = Long.MIN_VALUE;
        OptimizationWorker<P, T, I> top = null;

        for (OptimizationWorker<P, T, I> ow : workers) {
            long indicator = ow.getDonationIndicator();
            if (topIndicator > indicator) {
                receiversPQ.add(ow);
            } else {
                topIndicator = indicator;
                if (top != null) {
                    receiversPQ.add(top);
                }
                top = ow;
            }
        }
        OptimizationWorker<P, T, I> ow;
        while ((ow = receiversPQ.poll()) != null) {
            receivers.add(ow);
        }
        return top;
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     ----------- Comparators  optimization -------------
     ---------------------------------------------------
     --------------------------------------------------*/
    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> Comparator<AllocatableAction<P, T, I>> getSelectionComparator() {
        return new Comparator<AllocatableAction<P, T, I>>() {

            @Override
            public int compare(AllocatableAction<P, T, I> action1, AllocatableAction<P, T, I> action2) {
                int priority = Integer.compare(action1.getPriority(), action2.getPriority());
                if (priority == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                } else {
                    return -priority;
                }
            }
        };
    }

    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> Comparator<AllocatableAction<P, T, I>> getDonationComparator() {
        return new Comparator<AllocatableAction<P, T, I>>() {

            @Override
            public int compare(AllocatableAction<P, T, I> action1, AllocatableAction<P, T, I> action2) {
                FullGraphSchedulingInformation<P, T, I> action1DSI = (FullGraphSchedulingInformation<P, T, I>) action1.getSchedulingInfo();
                FullGraphSchedulingInformation<P, T, I> action2DSI = (FullGraphSchedulingInformation<P, T, I>) action2.getSchedulingInfo();
                int priority = Long.compare(action2DSI.getExpectedEnd(), action1DSI.getExpectedEnd());
                if (priority == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                } else {
                    return priority;
                }
            }
        };
    }

    public static <P extends Profile, T extends WorkerResourceDescription, I extends Implementation<T>> Comparator<OptimizationWorker<P, T, I>> getReceptionComparator() {
        return new Comparator<OptimizationWorker<P, T, I>>() {

            @Override
            public int compare(OptimizationWorker<P, T, I> worker1, OptimizationWorker<P, T, I> worker2) {
                return Long.compare(worker1.getDonationIndicator(), worker2.getDonationIndicator());
            }
        };
    }

    private boolean move(AllocatableAction<P, T, I> action, OptimizationWorker<P, T, I> donor, OptimizationWorker<P, T, I> receiver) {
        LinkedList<AllocatableAction<P, T, I>> dataPreds = action.getDataPredecessors();
        long dataAvailable = 0;
        try {
            for (AllocatableAction<P, T, I> dataPred : dataPreds) {
                FullGraphSchedulingInformation<P, T, I> dsi = (FullGraphSchedulingInformation<P, T, I>) dataPred.getSchedulingInfo();
                dataAvailable = Math.max(dataAvailable, dsi.getExpectedEnd());
            }
        } catch (ConcurrentModificationException cme) {
            dataAvailable = 0;
            dataPreds = action.getDataPredecessors();
        }

        I bestImpl = null;
        long bestTime = Long.MAX_VALUE;

        LinkedList<I> impls = action.getCompatibleImplementations(receiver.getResource());
        for (I impl : impls) {
            Profile p = receiver.getResource().getProfile(impl);
            long avgTime = p.getAverageExecutionTime();
            if (avgTime < bestTime) {
                bestTime = avgTime;
                bestImpl = impl;
            }
        }

        FullGraphSchedulingInformation<P, T, I> dsi = (FullGraphSchedulingInformation<P, T, I>) action.getSchedulingInfo();
        long currentEnd = dsi.getExpectedEnd();

        if (bestImpl != null && currentEnd > receiver.getResource().getLastGapExpectedStart() + bestTime) {
            unschedule(action);
            schedule(action, bestImpl, receiver);
            return true;
        }
        return false;
    }

    public void schedule(AllocatableAction<P, T, I> action, I impl, OptimizationWorker<P, T, I> ow) {
        try {
            action.schedule(ow.getResource(), impl);
            action.tryToLaunch();
        } catch (InvalidSchedulingException ise) {
            try {
                long actionScore = FullGraphScore.getActionScore(action);
                long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new FullGraphScore<P, T, I>(actionScore, dataTime, 0, 0, 0);
                boolean keepTrying = true;
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                    AllocatableAction<P, T, I> pre = action.getConstrainingPredecessors().get(i);
                    action.schedule(pre.getAssignedResource(), aScore);
                    try {
                        action.tryToLaunch();
                        keepTrying = false;
                    } catch (InvalidSchedulingException ise2) {
                        // Try next predecessor
                        keepTrying = true;
                    }
                }
            } catch (BlockedActionException | UnassignedActionException be) {
                // Can not happen since there was an original source
            }
        } catch (BlockedActionException | UnassignedActionException be) {
            // Can not happen since there was an original source
        }
    }

    public void unschedule(AllocatableAction<P, T, I> action) {
        FullGraphResourceScheduler<P, T, I> resource = (FullGraphResourceScheduler<P, T, I>) action.getAssignedResource();
        resource.unscheduleAction(action);
    }

}
