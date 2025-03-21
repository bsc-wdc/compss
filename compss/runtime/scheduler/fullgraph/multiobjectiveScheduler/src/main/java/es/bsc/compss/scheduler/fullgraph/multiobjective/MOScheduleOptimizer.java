/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.fullgraph.multiobjective;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.ActionNotFoundException;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.fullgraph.multiobjective.types.MOScore;
import es.bsc.compss.scheduler.fullgraph.multiobjective.types.OptimizationWorker;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.SchedulingOptimizer;

import java.util.Collection;
import java.util.Comparator;
import java.util.ConcurrentModificationException;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class MOScheduleOptimizer extends SchedulingOptimizer<MOScheduler> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final String LOG_PREFIX = "[MOScheduleOptimizer] ";

    // Optimization threshold
    private static long OPTIMIZATION_THRESHOLD = 1_000;

    private boolean stop = false;
    private Semaphore sem = new Semaphore(0);


    /**
     * Creates a new MOSchedulerOptimizer instance.
     * 
     * @param scheduler Associated MOScheduler.
     */
    public MOScheduleOptimizer(MOScheduler scheduler) {
        super(scheduler);
    }

    @Override
    public void run() {
        long lastUpdate = System.currentTimeMillis();
        try {
            Thread.sleep(500);
        } catch (InterruptedException ie) {
            // Do nothing
        }
        while (!this.stop) {
            long optimizationTS = System.currentTimeMillis();
            Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers = this.scheduler.getWorkers();
            globalOptimization(optimizationTS, workers);
            lastUpdate = optimizationTS;
            waitForNextIteration(lastUpdate);
        }
        this.sem.release();
    }

    @Override
    public void shutdown() {
        this.stop = true;
        this.interrupt();
        try {
            this.sem.acquire();
        } catch (InterruptedException ie) {
            // Do nothing
        }
    }

    private void waitForNextIteration(long lastUpdate) {
        long difference = OPTIMIZATION_THRESHOLD - (System.currentTimeMillis() - lastUpdate);
        if (difference > 0) {
            try {
                Thread.sleep(difference);
            } catch (InterruptedException ie) {
                Thread.currentThread().interrupt();
            }
        }
    }

    /*--------------------------------------------------
     ---------------------------------------------------
     --------------- Local  optimization ---------------
     ---------------------------------------------------
     --------------------------------------------------*/

    /**
     * Performs a global optimization.
     * 
     * @param optimizationTS Optimization time stamp.
     * @param workers Available workers.
     */
    @SuppressWarnings("unchecked")
    public void globalOptimization(long optimizationTS,
        Collection<ResourceScheduler<? extends WorkerResourceDescription>> workers) {

        LOGGER.debug(LOG_PREFIX + " --- Start Global Optimization ---");
        int workersCount = workers.size();
        if (workersCount == 0) {
            return;
        }
        OptimizationWorker[] optimizedWorkers = new OptimizationWorker[workersCount];
        LinkedList<OptimizationWorker> receivers = new LinkedList<>();
        int i = 0;
        for (ResourceScheduler<? extends WorkerResourceDescription> worker : workers) {
            optimizedWorkers[i] = new OptimizationWorker((MOResourceScheduler<WorkerResourceDescription>) worker);
            i++;
        }

        boolean hasDonated = true;
        while (hasDonated) {
            optimizationTS = System.currentTimeMillis();
            hasDonated = false;
            LOGGER.debug(LOG_PREFIX + " --- Iteration of global Optimization ---");
            // Perform local optimizations
            for (OptimizationWorker ow : optimizedWorkers) {
                LOGGER.debug(LOG_PREFIX + "Optimizing localy resource " + ow.getName());
                ow.localOptimization(optimizationTS);
                LOGGER.debug(LOG_PREFIX + "Resource " + ow.getName() + " will end at " + ow.getDonationIndicator());
            }

            LinkedList<OptimizationWorker> donors = determineDonorAndReceivers(optimizedWorkers, receivers);

            while (!hasDonated && !donors.isEmpty()) {
                OptimizationWorker donor = donors.remove();
                AllocatableAction candidate;
                while (!hasDonated && (candidate = donor.pollDonorAction()) != null) {
                    /*
                     * if (candidate == null) { break; }
                     */
                    Iterator<OptimizationWorker> recIt = receivers.iterator();
                    while (recIt.hasNext()) {
                        OptimizationWorker receiver = recIt.next();
                        if (move(candidate, donor, receiver)) {
                            hasDonated = true;
                            break;
                        }
                    }
                }
            }
            LOGGER.debug(LOG_PREFIX + "--- Optimization Iteration finished ---");
        }
        LOGGER.debug(LOG_PREFIX + "--- Global Optimization finished ---");
    }

    /**
     * Determines the task donors and receivers.
     * 
     * @param workers List of optimization workers.
     * @param receivers List of receivers.
     * @return List of donors.
     */
    public static LinkedList<OptimizationWorker> determineDonorAndReceivers(OptimizationWorker[] workers,
        LinkedList<OptimizationWorker> receivers) {

        receivers.clear();
        PriorityQueue<OptimizationWorker> receiversPQ =
            new PriorityQueue<OptimizationWorker>(1, getReceptionComparator());
        long topIndicator = Long.MIN_VALUE;
        LinkedList<OptimizationWorker> top = new LinkedList<>();

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

    /**
     * Returns a selection comparator.
     * 
     * @return A selection comparator.
     */
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

    /**
     * Returns a donation comparator.
     * 
     * @return A donation comparator.
     */
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

    /**
     * Returns a reception comparator.
     * 
     * @return A reception comparator.
     */
    public static final Comparator<OptimizationWorker> getReceptionComparator() {
        return new Comparator<OptimizationWorker>() {

            @Override
            public int compare(OptimizationWorker worker1, OptimizationWorker worker2) {
                return Long.compare(worker1.getDonationIndicator(), worker2.getDonationIndicator());
            }
        };
    }

    private boolean move(AllocatableAction action, OptimizationWorker donor, OptimizationWorker receiver) {
        LOGGER
            .debug(LOG_PREFIX + "Trying to move " + action + " from " + donor.getName() + " to " + receiver.getName());
        List<AllocatableAction> dataPreds = action.getDataPredecessors();
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

        List<Implementation> impls = action.getCompatibleImplementations(receiver.getResource());

        Score bestScore = null;
        for (Implementation impl : impls) {
            MOScore actionScore = MOScheduler.getActionScore(action);
            MOScore score = ((MOResourceScheduler<?>) (receiver.getResource())).generateMoveImplementationScore(action,
                null, impl, actionScore, (long) (OPTIMIZATION_THRESHOLD * 2.5));
            if (Score.isBetter(score, bestScore)) {
                bestImpl = impl;
                bestScore = score;
            }
        }
        Implementation currentImpl = action.getAssignedImplementation();
        MOScore actionScore = MOScheduler.getActionScore(action);
        LOGGER.debug(LOG_PREFIX + "Calculating score for current execution");
        MOScore currentScore = ((MOResourceScheduler<?>) (action.getAssignedResource()))
            .generateCurrentImplementationScore(action, currentImpl, actionScore);
        LOGGER.debug(LOG_PREFIX + "Comparing scores: \n\t (New best)" + bestScore + "\n\t (Current" + currentScore);
        if (bestImpl != null && Score.isBetter(bestScore, currentScore)) {
            try {
                LOGGER
                    .debug(LOG_PREFIX + "Moving " + action + " from " + donor.getName() + " to " + receiver.getName());
                unscheduleFromWorker(action);
                scheduleOnWorker(action, bestImpl, receiver);
            } catch (ActionNotFoundException anfe) {
                // Action was already moved from the resource. Recompute Optimizations!!!
            }
            return true;
        } else {
            LOGGER.debug(LOG_PREFIX + "Action " + action + " not moved because new position is not better than actual");
        }
        return false;
    }

    /**
     * Schedules the given action with the given implementation in the given worker.
     * 
     * @param action Action to perform.
     * @param impl Action's implementation.
     * @param ow Selected worker.
     */
    public void scheduleOnWorker(AllocatableAction action, Implementation impl, OptimizationWorker ow) {
        boolean failedSpecificScheduling = false;
        try {
            action.schedule(ow.getResource(), impl);
            try {
                action.tryToLaunch();
            } catch (InvalidSchedulingException ise) {
                failedSpecificScheduling = true;
            }
        } catch (UnassignedActionException be) {
            failedSpecificScheduling = true;
        }

        if (failedSpecificScheduling) {
            try {
                long dataTime = MOScore.getDataPredecessorTime(action.getDataPredecessors());
                Score aScore = new MOScore(action.getPriority(), action.getGroupPriority(), dataTime, 0, 0, 0, 0);
                action.schedule(aScore);
                try {
                    action.tryToLaunch();
                } catch (InvalidSchedulingException ise2) {
                    // Impossible exception if schedule method on action is ok.
                }
            } catch (BlockedActionException | UnassignedActionException be) {
                // Can not happen since there was an original source
            }
        }
    }

    /**
     * Unschedule the given action from its worker.
     * 
     * @param action Action to unschedule.
     * @throws ActionNotFoundException When the action is not registered.
     */
    public void unscheduleFromWorker(AllocatableAction action) throws ActionNotFoundException {
        MOResourceScheduler<?> resource = (MOResourceScheduler<?>) action.getAssignedResource();
        resource.unscheduleAction(action);
    }
}
