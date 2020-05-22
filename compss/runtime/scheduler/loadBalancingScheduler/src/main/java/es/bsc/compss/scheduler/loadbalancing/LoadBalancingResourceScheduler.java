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
package es.bsc.compss.scheduler.loadbalancing;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.scheduler.loadbalancing.types.LoadBalancingScore;
import es.bsc.compss.scheduler.ready.ReadyResourceScheduler;
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
 * Implementation for the LoadBalancingResourceScheduler.
 *
 * @param <T> Worker Resource Description.
 */
public class LoadBalancingResourceScheduler<T extends WorkerResourceDescription> extends ReadyResourceScheduler<T> {

    /**
     * New LoadBalancing Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public LoadBalancingResourceScheduler(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate blocked score for action " + action);
        long priority = action.getPriority();
        long groupId = action.getGroupPriority();
        long resourceScore = 0;
        long waitingScore = this.blocked.size();
        long implementationScore = 0;

        return new LoadBalancingScore(priority, groupId, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate resource score for action " + action);

        // Since we are generating the resource score, we copy the previous fields from actionScore
        long priority = actionScore.getPriority();
        long groupId = action.getGroupPriority();

        // We compute the rest of the fields
        // Computes the resource waiting score
        long waitingScore = -action.getId();
        // Computes the priority of the resource
        long resourceScore = calculateResourceScore(params);
        // Computes the priority of the implementation (should not be computed)
        long implementationScore = -100;

        LoadBalancingScore score =
            new LoadBalancingScore(priority, groupId, resourceScore, waitingScore, implementationScore);
        // LOGGER.debug("[LoadBalancingScheduler] Resource Score " + score + " " + actionPriority + " " + resourceScore
        // + " " + waitingScore
        // + " " + implementationScore);

        return score;
    }

    private long calculateResourceScore(TaskDescription params) {
        long resourceScore = 0;
        if (params != null) {
            List<Parameter> parameters = params.getParameters();
            if (parameters.isEmpty()) {
                return 1;
            }
            resourceScore = 200 * Score.calculateDataLocalityScore(params, myWorker);
        }
        if (this.myWorker == Comm.getAppHost()){
            resourceScore++;
        }
        return resourceScore;
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {
        // LOGGER.debug("[LoadBalancing] Generate implementation score for action " + action);
        if (this.hasBlockedActions()) {
            // Added for scale-down: In readyScheduler, should disable the node for scheduling more tasks?
            return null;
        }
        if (this.myWorker.canRunNow((T) impl.getRequirements())) {
            // Since we are generating the implementation score, we copy the previous fields from resourceScore
            long priority = resourceScore.getPriority();
            long groupId = action.getGroupPriority();
            long resourcePriority = resourceScore.getResourceScore();

            // We compute the rest of the fields
            long waitingScore = -action.getId();
            long implScore = -this.getProfile(impl).getAverageExecutionTime();

            return new LoadBalancingScore(priority, groupId, resourcePriority, waitingScore, implScore);
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
        return "LoadBalancingResourceScheduler@" + getName();
    }
}
