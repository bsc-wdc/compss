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
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.FullGraphScore;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;


/**
 * Implementation of a Scheduler that handles the full task graph
 *
 * @param <P>
 * @param <T>
 */
public class FullGraphScheduler extends TaskScheduler {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    private final FullGraphScore dummyScore;
    private final ScheduleOptimizer optimizer;


    /**
     * Constructs a new scheduler. scheduleAction(Action action) Behaves as the basic Task Scheduler, as tasks arrive
     * their executions are scheduled into a worker node
     */
    public FullGraphScheduler() {
        super();

        this.dummyScore = new FullGraphScore(0, 0, 0, 0, 0);
        this.optimizer = new ScheduleOptimizer(this);

        this.optimizer.start();
    }

    @Override
    public void shutdown() {
        try {
            this.optimizer.shutdown();
        } catch (InterruptedException ie) {
            Thread.currentThread().interrupt();
        }
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    @Override
    public <T extends WorkerResourceDescription> SchedulingInformation generateSchedulingInformation(
            ResourceScheduler<T> rs) {
        LOGGER.info("[FGScheduler] Generate empty scheduling information");
        return new FullGraphSchedulingInformation(null);
    }

    @Override
    public <T extends WorkerResourceDescription> ResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
            JSONObject defaultResources, JSONObject defaultImplementations) {

        LOGGER.info("[FGScheduler] Generate scheduler for resource " + w.getName());
        return new FullGraphResourceScheduler<>(w, null, null, getOrchestrator());
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        LOGGER.info("[FGScheduler] Generate Action Score for " + action);
        long actionScore = FullGraphScore.getActionScore(action);
        long dataTime = dummyScore.getDataPredecessorTime(action.getDataPredecessors());
        return new FullGraphScore(actionScore, dataTime, 0, 0, 0);
    }

}
