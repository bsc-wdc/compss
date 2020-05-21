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
package es.bsc.compss.scheduler.fifodatalocation;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.readynew.ReadyScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.List;
import java.util.PriorityQueue;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode + data locality.
 */
public class FIFODataLocationScheduler extends ReadyScheduler {

    /**
     * Constructs a new FIFODataScheduler instance.
     */
    public FIFODataLocationScheduler() {
        super();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> FIFODataLocationResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
        JSONObject resJSON, JSONObject implJSON) {
        // LOGGER.debug("[FIFODataScheduler] Generate scheduler for resource " + w.getName());
        return new FIFODataLocationResourceScheduler<>(w, resJSON, implJSON);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        // LOGGER.debug("[FIFODataScheduler] Generate Action Score for " + action);
        return new Score(action.getPriority(), action.getGroupPriority(), 0, -action.getId(), 0);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> void purgeFreeActions(List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
        ResourceScheduler<T> resource) {

        LOGGER.debug("[FIFODataLocation Scheduler] Treating dependency free actions");

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
                List<ResourceScheduler<?>> uselessWorkers =
                    freeAction.tryToSchedule(obj.getScore(), this.availableWorkers);
                for (ResourceScheduler<?> worker : uselessWorkers) {
                    this.availableWorkers.remove(worker);
                }
                ResourceScheduler<? extends WorkerResourceDescription> assignedResource =
                    freeAction.getAssignedResource();
                tryToLaunch(freeAction);
                if (!assignedResource.canRunSomething()) {
                    this.availableWorkers.remove(assignedResource);
                }
            } catch (BlockedActionException e) {
                removeFromReady(freeAction);
                addToBlocked(freeAction);
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
