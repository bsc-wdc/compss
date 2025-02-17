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
package es.bsc.compss.scheduler.lookahead.locality;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.lookahead.LookaheadTS;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.scheduler.types.schedulinginformation.DataLocality;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import java.util.List;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode.
 */
public class LocalityTS extends LookaheadTS {

    /**
     * Constructs a new FIFOScheduler instance.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public LocalityTS(ActionOrchestrator orchestrator) {
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
    public <T extends WorkerResourceDescription> LocalityRS<T> generateSchedulerForResource(Worker<T> w,
        JSONObject resJSON, JSONObject implJSON) {
        return new LocalityRS<>(w, resJSON, implJSON);
    }

    @Override
    public <T extends WorkerResourceDescription> SchedulingInformation
        generateSchedulingInformation(ResourceScheduler<T> rs, List<? extends Parameter> params, Integer coreId) {
        return new DataLocality(rs, params, coreId);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        return new Score(action.getPriority(), action.getGroupPriority(), 0, 0, 0);
    }

}
