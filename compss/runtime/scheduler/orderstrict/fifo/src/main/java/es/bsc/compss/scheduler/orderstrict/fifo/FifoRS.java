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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.scheduler.orderstrict.OrderStrictRS;

import es.bsc.compss.scheduler.orderstrict.fifo.types.FIFOScore;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.json.JSONObject;


/**
 * Implementation for the FifoRS.
 *
 * @param <T> Worker Resource Description.
 */
public class FifoRS<T extends WorkerResourceDescription> extends OrderStrictRS<T> {

    /**
     * FIFO Priority Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public FifoRS(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {

        long priority = action.getPriority();
        long groupId = action.getGroupPriority();
        long actionId = action.getId();
        long resourceScore = 0;
        long waitingScore = 0;
        long implementationScore = 0;

        return new FIFOScore(priority, groupId, actionId, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[FIFOResourceScheduler] Generate resource score for action " + action);

        // Since we are generating the resource score, we copy the previous fields from actionScore
        long priority = actionScore.getPriority();
        long groupId = action.getGroupPriority();
        long actionId = action.getId();

        // Now we compute the rest of the score
        // Computes the resource waiting score
        long waitingScore = -this.blocked.size();
        // Computes the priority of the resource
        long resourceScore = 0;
        if (this.myWorker == Comm.getAppHost()) {
            resourceScore++;
        }

        return new FIFOScore(priority, groupId, actionId, resourceScore, waitingScore, 0);
    }

    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {
        // LOGGER.debug("[ResourceScheduler] Generate implementation score for action " + action);

        // Since we are generating the implementation score, we copy the previous fields from resourceScore
        long priority = resourceScore.getPriority();
        long groupId = action.getGroupPriority();
        long actionId = action.getId();
        long resource = resourceScore.getResourceScore();
        if (!this.myWorker.canRunNow((T) impl.getRequirements())) {
            return null;
        }

        // Now we compute the rest of the score
        long waitingScore = resourceScore.getWaitingScore();
        long implScore = -this.getProfile(impl).getAverageExecutionTime();
        return new FIFOScore(priority, groupId, actionId, resource, waitingScore, implScore);
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "FIFOResourceScheduler@" + getName();
    }

}
