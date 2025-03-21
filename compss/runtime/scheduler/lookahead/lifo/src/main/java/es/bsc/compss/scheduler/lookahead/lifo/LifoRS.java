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
package es.bsc.compss.scheduler.lookahead.lifo;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.scheduler.lookahead.LookaheadRS;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.json.JSONObject;


/**
 * Implementation for the LifoRS.
 *
 * @param <T> Worker Resource Description.
 */
public class LifoRS<T extends WorkerResourceDescription> extends LookaheadRS<T> {

    /**
     * New LIFO Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public LifoRS(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        // LOGGER.debug("[LIFOScheduler] Generate blocked score for action " + action);
        long priority = action.getPriority();
        long groupId = action.getGroupPriority();
        long resourceScore = action.getId();
        long waitingScore = 0;
        long implementationScore = 0;

        return new Score(priority, groupId, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[LIFOScheduler] Generate resource score for action " + action);

        // Since we are generating the resource score, we copy the previous fields from actionScore
        long priority = actionScore.getPriority();
        long groupId = action.getGroupPriority();

        // We compute the rest of the fields
        long resource = actionScore.getResourceScore();
        if (this.myWorker == Comm.getAppHost()) {
            resource++;
        }
        long waitingScore = 0;
        long implementationScore = 0;

        return new Score(priority, groupId, resource, waitingScore, implementationScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score getRunnableImplScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {
        // Since we are generating the implementation score, we copy the previous fields from resourceScore
        long priority = resourceScore.getPriority();
        long groupId = action.getGroupPriority();
        long resourcePriority = resourceScore.getResourceScore();
        long waitingScore = resourceScore.getWaitingScore();

        // We compute the rest of the fields
        long implScore = 0;

        return new Score(priority, groupId, resourcePriority, waitingScore, implScore);
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "LIFOResourceScheduler@" + getName();
    }
}
