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
package es.bsc.compss.scheduler.fifoScheduler;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.readyScheduler.ReadyScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.List;
import java.util.PriorityQueue;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks and sorts them in FIFO mode
 *
 */
public class FIFOScheduler extends ReadyScheduler {

    /**
     * Constructs a new Ready Scheduler instance
     *
     */
    public FIFOScheduler() {
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
    public <T extends WorkerResourceDescription> FIFOResourceScheduler<T> generateSchedulerForResource(Worker<T> w, JSONObject resJSON,
            JSONObject implJSON) {
        // LOGGER.debug("[FIFOScheduler] Generate scheduler for resource " + w.getName());
        return new FIFOResourceScheduler<>(w, resJSON, implJSON);
    }

    @Override
    public Score generateActionScore(AllocatableAction action) {
        // LOGGER.debug("[FIFOScheduler] Generate Action Score for " + action);
        return new Score(action.getPriority(), -action.getId(), 0, 0);
    }
    
    /************************ START REIMPLEMENTED PART *****************/
    
    @Override
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
        if (!action.hasDataPredecessors()) {
            try {
                action.tryToSchedule(actionScore);
            } catch (UnassignedActionException ex) {
                this.unassignedReadyActions.addAction(action);
            }
        }
    }

    @Override
    protected <T extends WorkerResourceDescription> void scheduleAction(AllocatableAction action, ResourceScheduler<T> targetWorker,
            Score actionScore) throws BlockedActionException, UnassignedActionException {
        if (!action.hasDataPredecessors()) {
            action.schedule(targetWorker, actionScore);
        }
    }

    @Override
    public List<AllocatableAction> getUnassignedActions() {
        return unassignedReadyActions.getAllActions();
    }
    
    @Override
    protected <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
            List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {

        // Try to launch all the data free actions and the resource free actions
        PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
        for (AllocatableAction freeAction : dataFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, fullScore);
            executableActions.add(obj);
        }
        
        for (AllocatableAction freeAction : resourceFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, fullScore);
            if (!executableActions.contains(obj)) {
                executableActions.add(obj);
            }
        }

        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();

            // LOGGER.debug("Trying to launch action " + freeAction);
            try {
                //scheduleAction(freeAction, obj.getScore());
                try {
                    freeAction.tryToSchedule(obj.getScore());
                } catch (UnassignedActionException ex) {
                    this.unassignedReadyActions.addAction(freeAction);
                }
                tryToLaunch(freeAction);
            } catch (BlockedActionException e) {
                removeFromReady(freeAction);
                addToBlocked(freeAction);
            }
        }
    }


    /************************ END REIMPLEMENTED PART *****************/

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> void purgeFreeActions(List<AllocatableAction> dataFreeActions,
            List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {

        List<AllocatableAction> unassignedReadyActions = this.unassignedReadyActions.getAllActions();
        this.unassignedReadyActions.removeAllActions();
        dataFreeActions.addAll(unassignedReadyActions);
    }

}
