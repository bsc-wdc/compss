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
package es.bsc.compss.scheduler.orderstrict;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import org.json.JSONObject;


public abstract class OrderStrictRS<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    /**
     * New Ready Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public OrderStrictRS(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[ResourceScheduler] Generate resource score for action " + action);

        // Since we are generating the resource score, we copy the previous fields from actionScore
        long priority = actionScore.getPriority();
        long groupId = action.getGroupPriority();

        // Now we compute the rest of the score
        // Computes the resource waiting score
        long waitingScore = -this.blocked.size();
        // Computes the priority of the resource
        long resourceScore = 0;
        if (this.myWorker == Comm.getAppHost()) {
            resourceScore++;
        }

        return new Score(priority, groupId, resourceScore, waitingScore, 0);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {

        // Since we are generating the implementation score, we copy the previous fields from resourceScore
        long priority = resourceScore.getPriority();
        long groupId = action.getGroupPriority();
        long resource = resourceScore.getResourceScore();
        if (!this.myWorker.canRunNow((T) impl.getRequirements())) {
            return null;
        }

        // Now we compute the rest of the score
        long waitingScore = resourceScore.getWaitingScore();
        long implScore = -this.getProfile(impl).getAverageExecutionTime();
        return new Score(priority, groupId, resource, waitingScore, implScore);
    }

}
