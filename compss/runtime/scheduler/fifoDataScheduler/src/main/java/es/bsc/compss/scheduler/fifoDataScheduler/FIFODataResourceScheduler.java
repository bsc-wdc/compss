/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.fifoDataScheduler;

import es.bsc.compss.scheduler.readyScheduler.ReadyResourceScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import org.json.JSONObject;


public class FIFODataResourceScheduler<T extends WorkerResourceDescription> extends ReadyResourceScheduler<T> {

    /**
     * New ready resource scheduler instance
     *
     * @param w
     * @param resJSON
     * @param implJSON
     */
    public FIFODataResourceScheduler(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
        super(w, resJSON, implJSON);
    }

    /*
     * ***************************************************************************************************************
     * SCORES
     * ***************************************************************************************************************
     */
    @Override
    public Score generateBlockedScore(AllocatableAction action) {
        // LOGGER.debug("[FIFODataResourceScheduler] Generate blocked score for action " + action);
        long actionPriority = action.getPriority();
        long resourceScore = -action.getId();
        long waitingScore = 0;
        long implementationScore = 0;

        return new Score(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[FIFODataResourceScheduler] Generate resource score for action " + action);

        long actionPriority = actionScore.getActionScore();
        long resourceScore = actionScore.getResourceScore();
        long waitingScore = 0;
        // double resourceScore = Math.min(1.5, 1.0 / (double) myWorker.getUsedTaskCount());
        long implementationScore = 0;

        return new Score(actionPriority, resourceScore, waitingScore, implementationScore);
    }

    @SuppressWarnings("unchecked")
    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl, Score resourceScore) {

        // LOGGER.debug("[FIFODataResourceScheduler] Generate implementation score for action " + action);
        if (this.hasBlockedActions()) {
            // Added for scale-down: In readyScheduler, should disable the node for scheduling more tasks? 
            return null;
        }

        if (myWorker.canRunNow((T) impl.getRequirements())) {
            long actionPriority = resourceScore.getActionScore();
            long resourcePriority = resourceScore.getResourceScore();
            long waitingScore = 0;
            long implScore = 0;

            return new Score(actionPriority, resourcePriority, waitingScore, implScore);
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
        return "FIFODataResourceScheduler@" + getName();
    }

}
