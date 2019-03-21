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
package es.bsc.compss.scheduler.loadBalancingScheduler;

import es.bsc.compss.scheduler.readyScheduler.ReadyResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.schedulerloadBalancingScheduler.types.LoadBalancingScore;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.parameter.Parameter;
import org.json.JSONObject;


public class LoadBalancingResourceScheduler<T extends WorkerResourceDescription> extends ReadyResourceScheduler<T> {

    /**
     * New ready resource scheduler instance
     *
     * @param w
     * @param resJSON
     * @param implJSON
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
        long actionPriority = action.getPriority();
        long resourceScore = 0;
        long waitingScore = this.blocked.size();
        long implementationScore = 0;

        return new LoadBalancingScore(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[LoadBalancingScheduler] Generate resource score for action " + action);

        // Gets the action priority
        long actionPriority = actionScore.getActionScore();

        // Computes the resource waiting score
        long waitingScore = -action.getId();
        // Computes the priority of the resource
        long resourceScore = calculateResourceScore(params);
        // Computes the priority of the implementation (should not be computed)
        long implementationScore = -100;

        LoadBalancingScore score = new LoadBalancingScore(actionPriority, resourceScore, waitingScore,
                implementationScore);
        // LOGGER.debug("[LoadBalancingScheduler] Resource Score " + score + " " + actionPriority + " " + resourceScore
        // + " " + waitingScore
        // + " " + implementationScore);

        return score;
    }

    public long calculateResourceScore(TaskDescription params) {
        long resourceScore = 0;
        if (params != null) {
            Parameter[] parameters = params.getParameters();
            if (parameters.length == 0) {
                return 1;
            }
            resourceScore = 2 * Score.calculateDataLocalityScore(params, myWorker);
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
        if (myWorker.canRunNow((T) impl.getRequirements())) {
            long actionPriority = resourceScore.getActionScore();
            long resourcePriority = resourceScore.getResourceScore();
            long waitingScore = -action.getId();
            long implScore = -this.getProfile(impl).getAverageExecutionTime();

            return new LoadBalancingScore(actionPriority, resourcePriority, waitingScore, implScore);
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
