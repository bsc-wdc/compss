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
 * Implementation for the LocalityRS.
 *
 * @param <T> Worker Resource Description.
 */
public class LocalityRS<T extends WorkerResourceDescription> extends LookaheadRS<T> {

    /**
     * New LIFO Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public LocalityRS(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        // LOGGER.debug("[DataResourceScheduler] Generate blocked score for action " + action);

        long priority = action.getPriority();
        long groupId = action.getGroupPriority();
        long waitingScore = -this.blocked.size();
        long resourceScore = (long) action.getSchedulingInfo().getPreregisteredScore(myWorker) * 100;
        long implementationScore = 0;

        return new Score(priority, groupId, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[DataResourceScheduler] Generate resource score for action " + action);

        // Since we are generating the resource score, we copy the previous fields from actionScore
        long priority = actionScore.getPriority();
        long groupId = action.getGroupPriority();

        // Compute new score fields
        long resourceScore = (long) action.getSchedulingInfo().getPreregisteredScore(myWorker) * 100;
        if (this.myWorker == Comm.getAppHost()) {
            resourceScore++;
        }

        long waitingScore = -this.blocked.size();

        return new Score(priority, groupId, resourceScore, waitingScore, 0);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score getRunnableImplScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {
        // Since we are generating the implementation score, we copy the previous fields from resourceScore
        long priority = resourceScore.getPriority();
        long groupId = action.getGroupPriority();
        long resource = resourceScore.getResourceScore();
        long waitingScore = resourceScore.getWaitingScore();

        // Compute the rest of the fields
        long implScore = -this.getProfile(impl).getAverageExecutionTime();

        return new Score(priority, groupId, resource, waitingScore, implScore);
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "LocalityResourceScheduler@" + getName();
    }
}
