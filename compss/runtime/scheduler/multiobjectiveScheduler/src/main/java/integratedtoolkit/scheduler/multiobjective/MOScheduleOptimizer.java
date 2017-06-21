package integratedtoolkit.scheduler.multiobjective;

import integratedtoolkit.components.impl.ResourceScheduler;
import integratedtoolkit.scheduler.exceptions.ActionNotFoundException;
import integratedtoolkit.scheduler.exceptions.BlockedActionException;
import integratedtoolkit.scheduler.exceptions.InvalidSchedulingException;
import integratedtoolkit.scheduler.exceptions.UnassignedActionException;
import integratedtoolkit.scheduler.multiobjective.types.MOProfile;
import integratedtoolkit.scheduler.multiobjective.types.MOScore;
import integratedtoolkit.scheduler.multiobjective.types.OptimizationWorker;
import integratedtoolkit.scheduler.types.AllocatableAction;
import integratedtoolkit.scheduler.types.Score;
import integratedtoolkit.types.implementations.Implementation;
import integratedtoolkit.types.resources.WorkerResourceDescription;
import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;

public class MOScheduleOptimizer extends Thread {

    private static long OPTIMIZATION_THRESHOLD = 5_000;
    private MOScheduler scheduler;
    private boolean stop = false;
    private Semaphore sem = new Semaphore(0);

    public MOScheduleOptimizer(MOScheduler scheduler) {
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
        /*while (!stop) {
            long optimizationTS = System.currentTimeMillis();
            Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers = scheduler.getWorkers();
            globalOptimization(optimizationTS, workers);
            lastUpdate = optimizationTS;
            waitForNextIteration(lastUpdate);
        }*/
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
            Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers
    ) {
        int workersCount = workers.size();
        if (workersCount == 0) {
            return;
        }
        OptimizationWorker[] optimizedWorkers = new OptimizationWorker[workersCount];
        LinkedList<OptimizationWorker> receivers = new LinkedList();
        int i = 0;
        for (ResourceScheduler<? extends WorkerResourceDescription> worker : workers) {
            optimizedWorkers[i] = new OptimizationWorker((MOResourceScheduler<WorkerResourceDescription>) worker);
            i++;
        }

        boolean hasDonated = true;
        while (hasDonated) {
            optimizationTS = System.currentTimeMillis();
            hasDonated = false;
            System.out.println("-----------------------------------------");
            //Perform local optimizations
            for (OptimizationWorker ow : optimizedWorkers) {
                ow.localOptimization(optimizationTS);
                System.out.println(ow.getName() + " will end at " + ow.getDonationIndicator());
            }

            LinkedList<OptimizationWorker> donors = determineDonorAndReceivers(optimizedWorkers, receivers);

            while (!hasDonated && !donors.isEmpty()) {
                OptimizationWorker donor = donors.remove();
                AllocatableAction candidate = donor.pollDonorAction();
                if (candidate == null) {
                    break;
                }
                Iterator<OptimizationWorker> recIt = receivers.iterator();
                while (recIt.hasNext()) {
                    OptimizationWorker receiver = recIt.next();
                    if (move(candidate, donor, receiver)) {
                        hasDonated = true;
                        break;
                    }
                }
            }
            System.out.println("-----------------------------------------");
        }
    }

    public static LinkedList<OptimizationWorker> determineDonorAndReceivers(
            OptimizationWorker[] workers,
            LinkedList<OptimizationWorker> receivers
    ) {
        receivers.clear();
        PriorityQueue<OptimizationWorker> receiversPQ = new PriorityQueue<OptimizationWorker>(1, getReceptionComparator());
        long topIndicator = Long.MIN_VALUE;
        LinkedList<OptimizationWorker> top = new LinkedList();

        for (OptimizationWorker ow : workers) {
            long indicator = ow.getDonationIndicator();
            if (topIndicator > indicator) {
                receiversPQ.add(ow);
            } else {
                if (indicator > topIndicator) {
                    topIndicator = indicator;
                    for (OptimizationWorker extop : top) {
                        receiversPQ.add(extop);
                    }
                    top.clear();
                }
                top.add(ow);
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
                MOSchedulingInformation action1DSI = (MOSchedulingInformation) action1.getSchedulingInfo();
                MOSchedulingInformation action2DSI = (MOSchedulingInformation) action2.getSchedulingInfo();
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
        System.out.println("Trying to move " + action + " from " + donor + " to " + "receiver");
        LinkedList<AllocatableAction> dataPreds = action.getDataPredecessors();
        long dataAvailable = 0;
        try {
            for (AllocatableAction dataPred : dataPreds) {
                MOSchedulingInformation dsi = (MOSchedulingInformation) dataPred.getSchedulingInfo();
                dataAvailable = Math.max(dataAvailable, dsi.getExpectedEnd());
            }
        } catch (ConcurrentModificationException cme) {
            dataAvailable = 0;
            dataPreds = action.getDataPredecessors();
        }

        Implementation bestImpl = null;
        long bestTime = Long.MAX_VALUE;

        LinkedList<Implementation> impls = action.getCompatibleImplementations(receiver.getResource());

        MOScore bestScore = null;
        for (Implementation impl : impls) {
            MOProfile p = (MOProfile) receiver.getResource().getProfile(impl);
            long implScore = 0;
            if (p != null) {
                implScore = p.getAverageExecutionTime();
            } else {
                implScore = 0;
            }
            double energy = p.getPower() * implScore;
            double cost = p.getPrice();
            MOScore score = new MOScore(0, 0, 0, implScore, energy, cost);
            if (Score.isBetter(score, bestScore)) {
                bestImpl = impl;
                bestScore = score;
                bestTime = p.getAverageExecutionTime();
            }
        }

        MOSchedulingInformation dsi = (MOSchedulingInformation) action.getSchedulingInfo();
        long currentEnd = dsi.getExpectedEnd();

        if (bestImpl != null && currentEnd > receiver.getResource().getFirstGapExpectedStart() + bestTime) {
            try {
                System.out.println("Moving " + action + " from " + donor.getName() + " to " + receiver.getName());
                unscheduleFromWorker(action);
                scheduleOnWorker(action, bestImpl, receiver);
            } catch (ActionNotFoundException anfe) {
                //Action was already moved from the resource. Recompute Optimizations!!!
            }
            return true;
        }
        return false;
    }

    private MOScore dummyScore = new MOScore(0, 0, 0, 0, 0, 0);

    public void scheduleOnWorker(AllocatableAction action, Implementation impl, OptimizationWorker ow) {
        boolean failedSpecificScheduling = false;
        try {
            action.schedule(ow.getResource(), impl);
            try {
                action.tryToLaunch();
            } catch (InvalidSchedulingException ise) {
                failedSpecificScheduling = true;
            }
        } catch (BlockedActionException bae) {
            //Can not happen since there was an original source
        } catch (UnassignedActionException be) {
            failedSpecificScheduling = true;
        }

        if (failedSpecificScheduling) {
            try {
                long actionScore = MOScore.getActionScore(action);
                long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new MOScore(actionScore, dataTime, 0, 0, 0, 0);
                action.schedule(aScore);
                try {
                    action.tryToLaunch();
                } catch (InvalidSchedulingException ise2) {
                    //Impossible exception if schedule method on action is ok.
                }
            } catch (BlockedActionException | UnassignedActionException be) {
                //Can not happen since there was an original source
            }
        }
    }

    public void unscheduleFromWorker(AllocatableAction action) throws ActionNotFoundException {
        MOResourceScheduler resource = (MOResourceScheduler) action.getAssignedResource();
        resource.unscheduleAction(action);
    }
}
