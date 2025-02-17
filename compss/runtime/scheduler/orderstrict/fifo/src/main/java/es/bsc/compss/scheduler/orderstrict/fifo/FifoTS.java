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
package es.bsc.compss.scheduler.orderstrict.fifo;

import es.bsc.compss.scheduler.orderstrict.OrderStrictTS;
import es.bsc.compss.scheduler.orderstrict.fifo.types.FIFOScore;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode.
 */
public class FifoTS extends OrderStrictTS {

    /**
     * Constructs a new FIFOScheduler instance.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public FifoTS(ActionOrchestrator orchestrator) {
        super(orchestrator);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> FifoRS<T> generateSchedulerForResource(Worker<T> w, JSONObject resJSON,
        JSONObject implJSON) {
        return new FifoRS<>(w, resJSON, implJSON);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        return new FIFOScore(action.getPriority(), action.getGroupPriority(), action.getId(), 0, 0, 0);
    }

}
