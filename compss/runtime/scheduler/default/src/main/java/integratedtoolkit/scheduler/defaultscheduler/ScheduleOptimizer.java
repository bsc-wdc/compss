package integratedtoolkit.scheduler.defaultscheduler;

import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.types.DefaultScore;
import integratedtoolkit.types.Implementation;
import integratedtoolkit.types.OptimizationWorker;
import integratedtoolkit.types.Profile;
import integratedtoolkit.types.Score;
import integratedtoolkit.util.ResourceScheduler;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;

public class ScheduleOptimizer extends Thread {

    private static long OPTIMIZATION_THRESHOLD = 5_000;
    private DefaultScheduler<?, ?> scheduler;
    private boolean stop = false;
    private Semaphore sem = new Semaphore(0);

    public ScheduleOptimizer(DefaultScheduler<?, ?> scheduler) {
        this.setName("ScheduleOptimizer");
        this.scheduler = scheduler;
    }

    public void run() {
        long lastUpdate = System.currentTimeMillis();
        try {
            Thread.sleep(500);
        } catch (InterruptedException ie) {
            //Do nothing
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
                //Do nothing. Wake up in case of shutdown received
            }
        }
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     --------------- Local  optimization ---------------
     ---------------------------------------------------
     --------------------------------------------------*/
    public void globalOptimization(long optimizationTS,
            ResourceScheduler<?, ?>[] workers
    ) {
        int workersCount = workers.length;
        if (workersCount == 0) {
            return;
        }
        OptimizationWorker[] optimizedWorkers = new OptimizationWorker[workersCount];
        LinkedList<OptimizationWorker> receivers = new LinkedList();

        for (int i = 0; i < workersCount; i++) {
            optimizedWorkers[i] = new OptimizationWorker((DefaultResourceScheduler) workers[i]);
        }

        boolean hasDonated = true;
        while (hasDonated) {
            optimizationTS = System.currentTimeMillis();
            hasDonated = false;

            //Perform local optimizations
            for (int i = 0; i < workersCount; i++) {
                optimizedWorkers[i].localOptimization(optimizationTS);
            }

            OptimizationWorker donor = determineDonorAndReceivers(optimizedWorkers, receivers);

            while (!hasDonated) {
                AllocatableAction candidate = donor.pollDonorAction();
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

    public static OptimizationWorker determineDonorAndReceivers(
            OptimizationWorker[] workers,
            LinkedList<OptimizationWorker> receivers
    ) {
        receivers.clear();
        PriorityQueue<OptimizationWorker> receiversPQ = new PriorityQueue<OptimizationWorker>(1, getReceptionComparator());
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
    public static Comparator<AllocatableAction> getSelectionComparator() {
        return new Comparator<AllocatableAction>() {
            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                int priority = Integer.compare(action1.getPriority(), action2.getPriority());
                if (priority == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                } else {
                    return -priority;
                }
            }
        };
    }

    public static Comparator<AllocatableAction> getDonationComparator() {
        return new Comparator<AllocatableAction>() {
            @Override
            public int compare(AllocatableAction action1, AllocatableAction action2) {
                DefaultSchedulingInformation action1DSI = (DefaultSchedulingInformation) action1.getSchedulingInfo();
                DefaultSchedulingInformation action2DSI = (DefaultSchedulingInformation) action2.getSchedulingInfo();
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

    private boolean move(AllocatableAction action, OptimizationWorker donor, OptimizationWorker receiver) {
        LinkedList<AllocatableAction> dataPreds = action.getDataPredecessors();
        long dataAvailable = 0;
        try {
            for (AllocatableAction dataPred : dataPreds) {
                DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) dataPred.getSchedulingInfo();
                dataAvailable = Math.max(dataAvailable, dsi.getExpectedEnd());
            }
        } catch (ConcurrentModificationException cme) {
            dataAvailable = 0;
            dataPreds = action.getDataPredecessors();
        }

        Implementation bestImpl = null;
        long bestTime = Long.MAX_VALUE;

        LinkedList<Implementation> impls = action.getCompatibleImplementations(receiver.getResource());
        for (Implementation impl : impls) {
            Profile p = receiver.getResource().getProfile(impl);
            long avgTime = p.getAverageExecutionTime();
            if (avgTime < bestTime) {
                bestTime = avgTime;
                bestImpl = impl;
            }
        }

        DefaultSchedulingInformation dsi = (DefaultSchedulingInformation) action.getSchedulingInfo();
        long currentEnd = dsi.getExpectedEnd();

        if (bestImpl != null && currentEnd > receiver.getResource().getLastGapExpectedStart() + bestTime) {
            unschedule(action);
            schedule(action, bestImpl, receiver);
            return true;
        }
        return false;
    }

    private DefaultScore dummyScore = new DefaultScore(0, 0, 0, 0);

    public void schedule(AllocatableAction action, Implementation impl, OptimizationWorker ow) {
        try {
            action.schedule(ow.getResource(), impl);
            action.tryToLaunch();
        } catch (InvalidSchedulingException ise) {
            try {
                long actionScore = DefaultScore.getActionScore(action);
                long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new DefaultScore(actionScore, dataTime, 0, 0);
                action.schedule(action.getConstrainingPredecessor().getAssignedResource(), aScore);
                try {
                    action.tryToLaunch();
                } catch (InvalidSchedulingException ise2) {
                    //Impossible exception. 
                }
            } catch (BlockedActionException | UnassignedActionException be) {
                //Can not happen since there was an original source
            }
        } catch (BlockedActionException | UnassignedActionException be) {
            //Can not happen since there was an original source
        }
    }

    public void unschedule(AllocatableAction action) {
        DefaultResourceScheduler resource = (DefaultResourceScheduler) action.getAssignedResource();
        resource.unscheduleAction(action);
    }
}
