/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 *
 */
package es.bsc.compss.scheduler.fullGraphScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.FullGraphScore;
import es.bsc.compss.scheduler.types.OptimizationWorker;
import es.bsc.compss.scheduler.types.Profile;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;

import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;


public class ScheduleOptimizer extends Thread {

    private static final long OPTIMIZATION_THRESHOLD = 5_000;

    private FullGraphScheduler scheduler;
    private boolean stop = false;
    private Semaphore sem = new Semaphore(0);

    private FullGraphScore dummyScore = new FullGraphScore(0, 0, 0, 0, 0);


    /**
     * Construct a new scheduler optimizer for a given scheduler
     * 
     * @param scheduler
     */
    public ScheduleOptimizer(FullGraphScheduler scheduler) {
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
            Collection<ResourceScheduler<?>> workers = this.scheduler.getWorkers();
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
    public void globalOptimization(long optimizationTS, Collection<ResourceScheduler<?>> workers) {
        int workersCount = workers.size();
        if (workersCount == 0) {
            return;
        }
        OptimizationWorker<?>[] optimizedWorkers = new OptimizationWorker[workersCount];
        LinkedList<OptimizationWorker<?>> receivers = new LinkedList<>();

        int index = 0;
        for (ResourceScheduler<?> rs : workers) {
            optimizedWorkers[index] = new OptimizationWorker<>((FullGraphResourceScheduler<?>) rs);
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

            OptimizationWorker<?> donor = determineDonorAndReceivers(optimizedWorkers, receivers);

            while (!hasDonated) {
                AllocatableAction candidate = donor.pollDonorAction();
                if (candidate == null) {
                    break;
                }
                Iterator<OptimizationWorker<?>> recIt = receivers.iterator();
                while (recIt.hasNext()) {
                    OptimizationWorker<?> receiver = recIt.next();
                    if (move(candidate, donor, receiver)) {
                        hasDonated = true;
                    }
                }
            }

        }
    }

    public OptimizationWorker<?> determineDonorAndReceivers(OptimizationWorker<?>[] workers,
            LinkedList<OptimizationWorker<?>> receivers) {

        receivers.clear();
        PriorityQueue<OptimizationWorker<?>> receiversPQ = new PriorityQueue<>(1, getReceptionComparator());
        long topIndicator = Long.MIN_VALUE;
        OptimizationWorker<?> top = null;

        for (OptimizationWorker<?> ow : workers) {
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
        OptimizationWorker<?> ow;
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
                FullGraphSchedulingInformation action1DSI = (FullGraphSchedulingInformation) action1
                        .getSchedulingInfo();
                FullGraphSchedulingInformation action2DSI = (FullGraphSchedulingInformation) action2
                        .getSchedulingInfo();
                int priority = Long.compare(action2DSI.getExpectedEnd(), action1DSI.getExpectedEnd());
                if (priority == 0) {
                    return Long.compare(action1.getId(), action2.getId());
                } else {
                    return priority;
                }
            }
        };
    }

    public static Comparator<OptimizationWorker<?>> getReceptionComparator() {
        return new Comparator<OptimizationWorker<?>>() {

            @Override
            public int compare(OptimizationWorker<?> worker1, OptimizationWorker<?> worker2) {
                return Long.compare(worker1.getDonationIndicator(), worker2.getDonationIndicator());
            }
        };
    }

    private boolean move(AllocatableAction action, OptimizationWorker<?> donor, OptimizationWorker<?> receiver) {
        List<AllocatableAction> dataPreds = action.getDataPredecessors();
        long dataAvailable = 0;
        try {
            for (AllocatableAction dataPred : dataPreds) {
                FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) dataPred.getSchedulingInfo();
                dataAvailable = Math.max(dataAvailable, dsi.getExpectedEnd());
            }
        } catch (ConcurrentModificationException cme) {
            dataAvailable = 0;
            dataPreds = action.getDataPredecessors();
        }

        Implementation bestImpl = null;
        long bestTime = Long.MAX_VALUE;

        List<Implementation> impls = action.getCompatibleImplementations(receiver.getResource());
        for (Implementation impl : impls) {
            Profile p = receiver.getResource().getProfile(impl);
            long avgTime = p.getAverageExecutionTime();
            if (avgTime < bestTime) {
                bestTime = avgTime;
                bestImpl = impl;
            }
        }

        FullGraphSchedulingInformation dsi = (FullGraphSchedulingInformation) action.getSchedulingInfo();
        long currentEnd = dsi.getExpectedEnd();

        if (bestImpl != null && currentEnd > receiver.getResource().getLastGapExpectedStart() + bestTime) {
            unschedule(action);
            schedule(action, bestImpl, receiver);
            return true;
        }
        return false;
    }

    public void schedule(AllocatableAction action, Implementation impl, OptimizationWorker<?> ow) {
        try {
            action.schedule(ow.getResource(), impl);
            action.tryToLaunch();
        } catch (InvalidSchedulingException ise) {
            try {
                long actionScore = FullGraphScore.getActionScore(action);
                long dataTime = this.dummyScore.getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new FullGraphScore(actionScore, dataTime, 0, 0, 0);
                boolean keepTrying = true;
                for (int i = 0; i < action.getConstrainingPredecessors().size() && keepTrying; ++i) {
                    AllocatableAction pre = action.getConstrainingPredecessors().get(i);
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

    public void unschedule(AllocatableAction action) {
        FullGraphResourceScheduler<?> resource = (FullGraphResourceScheduler<?>) action.getAssignedResource();
        resource.unscheduleAction(action);
    }

}
