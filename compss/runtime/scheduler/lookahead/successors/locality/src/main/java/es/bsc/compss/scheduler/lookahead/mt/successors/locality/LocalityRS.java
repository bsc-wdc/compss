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
package es.bsc.compss.scheduler.lookahead.mt.successors.locality;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.scheduler.lookahead.mt.LookaheadRS;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.List;

import org.json.JSONObject;


/**
 * Implementation for the FIFODataResourceScheduler.
 *
 * @param <T> Worker Resource Description.
 */
public class LocalityRS<T extends WorkerResourceDescription> extends LookaheadRS<T> {

    /**
     * New FIFO Data Resource Scheduler instance.
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
        long priority = action.getPriority();
        long groupId = action.getGroupPriority();
        long resourceScore = (long) action.getSchedulingInfo().getPreregisteredScore(myWorker) * 100;
        long waitingScore = -action.getId();
        long implementationScore = 0;

        return new Score(priority, groupId, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {

        // Since we are generating the resource score, we copy the previous fields from actionScore
        long priority = actionScore.getPriority();
        long groupId = action.getGroupPriority();

        // Compute new score fields
        long resourceScore = (long) action.getSchedulingInfo().getPreregisteredScore(myWorker) * 100;
        if (this.myWorker == Comm.getAppHost()) {
            resourceScore++;
        }

        long waitingScore = -action.getId();
        long implementationScore = 0;

        return new Score(priority, groupId, resourceScore, waitingScore, implementationScore);
    }

    private long calculateResourceScore(TaskDescription params, AllocatableAction action) {
        long resourceScore = 0;
        if (params != null) {
            List<Parameter> parameters = params.getParameters();
            if (parameters.size() == 0) {
                return 1;
            }
            resourceScore = (long) action.getSchedulingInfo().getPreregisteredScore(myWorker);
        }
        return resourceScore;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {

        if (this.hasBlockedActions()) {
            // Added for scale-down: In readyScheduler, should disable the node for scheduling more tasks?
            return null;
        }

        if (this.myWorker.canRunNow((T) impl.getRequirements())) {
            // Since we are generating the implementation score, we copy the previous fields from resourceScore
            long priority = resourceScore.getPriority();
            long groupId = action.getGroupPriority();
            long resourcePriority = resourceScore.getResourceScore();
            long waitingScore = resourceScore.getWaitingScore();

            // We compute the rest of the fields
            long implScore = -this.getProfile(impl).getAverageExecutionTime();

            return new Score(priority, groupId, resourcePriority, waitingScore, implScore);
        } else {
            // Implementation cannot be run
            return null;
        }
    }

    /*
     * ***************************************************************************************************************
     * OTHER
     * ***************************************************************************************************************
     */
    @Override
    public String toString() {
        return "lookahead_mt_succ_location_scheduler@" + getName();
    }

}
