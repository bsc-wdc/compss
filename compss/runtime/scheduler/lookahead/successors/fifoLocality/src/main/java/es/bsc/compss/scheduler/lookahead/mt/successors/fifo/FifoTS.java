/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.lookahead.mt.successors.fifolocality;

import es.bsc.compss.scheduler.lookahead.mt.SuccessorsTS;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode + data locality.
 */
public class FifoLocalityTS extends SuccessorsTS {

    /**
     * Constructs a new FIFODataScheduler instance.
     */
    public FifoLocalityTS() {
        super();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> FifoLocalityRS<T> generateSchedulerForResource(Worker<T> w,
        JSONObject resJSON, JSONObject implJSON) {
        // LOGGER.debug("[FIFODataScheduler] Generate scheduler for resource " + w.getName());
        return new FifoLocalityRS<>(w, resJSON, implJSON);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        // LOGGER.debug("[FIFODataScheduler] Generate Action Score for " + action);
        return new Score(action.getPriority(), action.getGroupPriority(), 0, -action.getId(), 0);
    }

}
