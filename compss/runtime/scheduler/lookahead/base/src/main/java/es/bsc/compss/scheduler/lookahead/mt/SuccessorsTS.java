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
package es.bsc.compss.scheduler.lookahead.mt;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.List;
import java.util.PriorityQueue;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode + data locality.
 */
public abstract class SuccessorsTS extends LookaheadTS {

    /**
     * Constructs a new FIFODataScheduler instance.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public SuccessorsTS(ActionOrchestrator orchestrator) {
        super(orchestrator);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public final <T extends WorkerResourceDescription> void purgeFreeActions(List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
        ResourceScheduler<T> resource) {

        PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
        for (AllocatableAction action : dataFreeActions) {
            Score actionScore = this.generateActionScore(action);
            Score fullScore = action.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
            executableActions.add(obj);
        }
        dataFreeActions.clear();
        while (!executableActions.isEmpty() && !this.availableWorkers.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();
            try {
                if (freeAction.getCompatibleWorkers().isEmpty()) {
                    blockedCandidates.add(freeAction);
                }
                freeAction.schedule(this.getAvailableWorkers(), generateActionScore(freeAction));
                tryToLaunch(freeAction);
            } catch (UnassignedActionException e) {
                dataFreeActions.add(freeAction);
            }
        }

        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();
            dataFreeActions.add(freeAction);
        }

        super.purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);

    }

}
