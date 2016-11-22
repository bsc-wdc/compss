package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.DefaultScore;
import integratedtoolkit.types.OptimizationWorker;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import integratedtoolkit.util.ResourceScheduler;

import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;


public class ScheduleOptimizer<P extends Profile, T extends WorkerResourceDescription> extends Thread {

    private static final long OPTIMIZATION_THRESHOLD = 5_000;

    private DefaultScheduler<?, ?> scheduler;
    private boolean stop = false;
    private Semaphore sem = new Semaphore(0);

    private DefaultScore<P, T> dummyScore = new DefaultScore<>(0, 0, 0, 0, 0);


    public ScheduleOptimizer(DefaultScheduler<?, ?> scheduler) {
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
            ResourceScheduler<?, ?>[] workers = scheduler.getWorkers();
            globalOptimization(optimizationTS, workers);
            waitForNextIteration(lastUpdate);
            lastUpdate = optimizationTS;
        }
        sem.release();
    }

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
    public void globalOptimization(long optimizationTS, ResourceScheduler<?, ?>[] workers) {
        int workersCount = workers.length;
        if (workersCount == 0) {
            return;
        }
        OptimizationWorker[] optimizedWorkers = new OptimizationWorker[workersCount];
        LinkedList<OptimizationWorker> receivers = new LinkedList<>();

        for (int i = 0; i < workersCount; i++) {
            optimizedWorkers[i] = new OptimizationWorker((DefaultResourceScheduler<?, ?>) workers[i]);
        }

        boolean hasDonated = true;
        while (hasDonated) {
            optimizationTS = System.currentTimeMillis();
            hasDonated = false;

            // Perform local optimizations
            for (int i = 0; i < workersCount; i++) {
                optimizedWorkers[i].localOptimization(optimizationTS);
            }

            OptimizationWorker donor = determineDonorAndReceivers(optimizedWorkers, receivers);

            while (!hasDonated) {
                AllocatableAction<P, T> candidate = donor.pollDonorAction();
                if (candidate == null) {
                    break;
                }
                Iterator<OptimizationWorker> recIt = receivers.iterator();
                while (recIt.hasNext()) {
                    OptimizationWorker receiver = recIt.next();
                    if (move(candidate, donor, receiver)) {
                        hasDonated = true;
                    }
                }
            }

        }
    }

    public static OptimizationWorker determineDonorAndReceivers(OptimizationWorker[] workers, LinkedList<OptimizationWorker> receivers) {

        receivers.clear();
        PriorityQueue<OptimizationWorker> receiversPQ = new PriorityQueue<>(1, getReceptionComparator());
        long topIndicator = Long.MIN_VALUE;
        OptimizationWorker top = null;

        for (OptimizationWorker ow : workers) {
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
        OptimizationWorker ow;
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
    public static Comparator<AllocatableAction<?, ?>> getSelectionComparator() {
        return new Comparator<AllocatableAction<?, ?>>() {

            @Override
            public int compare(AllocatableAction<?, ?> action1, AllocatableAction<?, ?> action2) {
                int priority = Integer.compare(action1.getPriority(), action2.getPriority());
                if (priority == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                } else {
                    return -priority;
                }
            }
        };
    }

    public static Comparator<AllocatableAction<?, ?>> getDonationComparator() {
        return new Comparator<AllocatableAction<?, ?>>() {

            @Override
            public int compare(AllocatableAction<?, ?> action1, AllocatableAction<?, ?> action2) {
                DefaultSchedulingInformation<?, ?> action1DSI = (DefaultSchedulingInformation<?, ?>) action1.getSchedulingInfo();
                DefaultSchedulingInformation<?, ?> action2DSI = (DefaultSchedulingInformation<?, ?>) action2.getSchedulingInfo();
                int priority = Long.compare(action2DSI.getExpectedEnd(), action1DSI.getExpectedEnd());
                if (priority == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                } else {
                    return priority;
                }
            }
        };
    }

    public static final Comparator<OptimizationWorker> getReceptionComparator() {
        return new Comparator<OptimizationWorker>() {

            @Override
            public int compare(OptimizationWorker worker1, OptimizationWorker worker2) {
                return Long.compare(worker1.getDonationIndicator(), worker2.getDonationIndicator());
            }
        };
    }

    private boolean move(AllocatableAction<P, T> action, OptimizationWorker donor, OptimizationWorker receiver) {
        LinkedList<AllocatableAction<P, T>> dataPreds = action.getDataPredecessors();
        long dataAvailable = 0;
        try {
            for (AllocatableAction<?, ?> dataPred : dataPreds) {
                DefaultSchedulingInformation<?, ?> dsi = (DefaultSchedulingInformation<?, ?>) dataPred.getSchedulingInfo();
                dataAvailable = Math.max(dataAvailable, dsi.getExpectedEnd());
            }
        } catch (ConcurrentModificationException cme) {
            dataAvailable = 0;
            dataPreds = action.getDataPredecessors();
        }

        Implementation<T> bestImpl = null;
        long bestTime = Long.MAX_VALUE;

        LinkedList<Implementation<T>> impls = action.getCompatibleImplementations(receiver.getResource());
        for (Implementation<T> impl : impls) {
            Profile p = receiver.getResource().getProfile(impl);
            long avgTime = p.getAverageExecutionTime();
            if (avgTime < bestTime) {
                bestTime = avgTime;
                bestImpl = impl;
            }
        }

        DefaultSchedulingInformation<P, T> dsi = (DefaultSchedulingInformation<P, T>) action.getSchedulingInfo();
        long currentEnd = dsi.getExpectedEnd();

        if (bestImpl != null && currentEnd > receiver.getResource().getLastGapExpectedStart() + bestTime) {
            unschedule(action);
            schedule(action, bestImpl, receiver);
            return true;
        }
        return false;
    }

    public void schedule(AllocatableAction<P, T> action, Implementation<T> impl, OptimizationWorker ow) {
        try {
            action.schedule(ow.getResource(), impl);
            action.tryToLaunch();
        } catch (InvalidSchedulingException ise) {
            try {
                long actionScore = DefaultScore.getActionScore(action);
                long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new DefaultScore<P, T>(actionScore, dataTime, 0, 0, 0);                
                boolean keepTrying = true;
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                    AllocatableAction<P,T> pre = action.getConstrainingPredecessors().get(i);
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

    public void unschedule(AllocatableAction<P, T> action) {
        DefaultResourceScheduler<P, T> resource = (DefaultResourceScheduler<P, T>) action.getAssignedResource();
        resource.unscheduleAction(action);
    }

}
