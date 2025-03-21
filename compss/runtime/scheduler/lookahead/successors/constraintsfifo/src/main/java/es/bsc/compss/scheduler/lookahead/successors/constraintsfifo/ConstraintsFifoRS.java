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
package es.bsc.compss.scheduler.lookahead.successors.constraintsfifo;

import es.bsc.compss.scheduler.lookahead.LookaheadRS;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.implementations.TaskType;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.List;

import org.json.JSONObject;


/**
 * Implementation for the FIFODataResourceScheduler.
 *
 * @param <T> Worker Resource Description.
 */
public class ConstraintsFifoRS<T extends WorkerResourceDescription> extends LookaheadRS<T> {

    /**
     * New FIFO Data Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param resJSON Worker JSON description.
     * @param implJSON Implementation JSON description.
     */
    public ConstraintsFifoRS(Worker<T> w, JSONObject resJSON, JSONObject implJSON) {
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
        long priority = action.getPriority();
        long groupId = action.getGroupPriority();
        long resourceScore = 0;
        long waitingScore = -action.getId();
        long implementationScore = 0;

        return new Score(priority, groupId, resourceScore, waitingScore, implementationScore);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        // LOGGER.debug("[FIFODataResourceScheduler] Generate resource score for action " + action);

        // Since we are generating the resource score, we copy the previous fields from actionScore
        long priority = actionScore.getPriority();
        long groupId = action.getGroupPriority();

        // We compute the rest of the fields
        // double resource = Math.min(1.5, 1.0 / (double) myWorker.getUsedTaskCount());
        long resource = calculateConstraintScore(params);
        long waitingScore = -action.getId();
        long implementationScore = 0;

        return new Score(priority, groupId, resource, waitingScore, implementationScore);
    }

    protected long calculateConstraintScore(TaskDescription td) {

        if (td.getType() == TaskType.METHOD) {
            List<Implementation> implementations = td.getCoreElement().getImplementations();
            if (implementations != null && !implementations.isEmpty()) {
                MethodResourceDescription description =
                    (MethodResourceDescription) implementations.get(0).getRequirements();
                return description.getTotalCPUComputingUnits() * td.getNumNodes();
            } else {
                return 0;
            }
        } else {
            return 0;
        }
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
        return "ConstraintsFifo@" + getName();
    }

}
