/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.scheduler.orderstrict;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.scheduler.types.schedulinginformation.DataLocality;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import java.util.List;
import java.util.PriorityQueue;
import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks.
 */
public abstract class OrderStrictTS extends TaskScheduler {

    protected final PriorityQueue<ObjectValue<AllocatableAction>> readyQueue;


    /**
     * Constructs a new Ready Scheduler instance.
     */
    public OrderStrictTS() {
        super();
        readyQueue = new PriorityQueue<>();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** TASK SCHEDULER STRUCTURES GENERATORS **************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public abstract <T extends WorkerResourceDescription> ResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
        JSONObject defaultResources, JSONObject defaultImplementations);

    @Override
    public <T extends WorkerResourceDescription> SchedulingInformation
        generateSchedulingInformation(ResourceScheduler<T> rs, List<Parameter> params, Integer coreId) {
        return new SchedulingInformation(rs);
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */

    private void addActionToReadyQueue(AllocatableAction action, Score actionScore) {
        ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, actionScore);
        readyQueue.add(obj);
    }

    @Override
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
        if (!action.hasDataPredecessors()) {
            ObjectValue<AllocatableAction> topReady = readyQueue.peek();
            if (topReady == null || actionScore.isBetter(topReady.getScore())) {
                try {
                    action.schedule(actionScore);
                } catch (UnassignedActionException uae) {
                    addActionToReadyQueue(action, actionScore);
                }
            } else {
                if (action.getCompatibleWorkers().isEmpty()) {
                    throw new BlockedActionException();
                }
                addActionToReadyQueue(action, actionScore);
            }
        }
    }

    @Override
    public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {

        PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
        for (AllocatableAction freeAction : dataFreeActions) {
            Score actionScore = generateActionScore(freeAction);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, actionScore);
            executableActions.add(obj);
        }
        // No resourceFreeActions in this kind of scheduler

        boolean canExecute = true;
        boolean readyQueueEmpty = readyQueue.isEmpty();
        boolean executableActionsEmpty = executableActions.isEmpty();
        ObjectValue<AllocatableAction> topPriority = null;
        while (canExecute && (!executableActionsEmpty || !readyQueueEmpty)) {
            ObjectValue<AllocatableAction> topReadyQueue = readyQueue.peek();
            ObjectValue<AllocatableAction> topExecutableActions = executableActions.peek();
            Score topReadyQueueScore = null;
            Score topExecutableActionsScore = null;
            if (!readyQueueEmpty) {
                topReadyQueueScore = topReadyQueue.getScore();
            }
            if (!executableActionsEmpty) {
                topExecutableActionsScore = topExecutableActions.getScore();
            }

            if (Score.isBetter(topReadyQueueScore, topExecutableActionsScore)) {
                topPriority = topReadyQueue;
            } else {
                topPriority = topExecutableActions;
            }
            AllocatableAction aa = topPriority.getObject();
            try {
                aa.schedule(topPriority.getScore());
                tryToLaunch(aa);

                if (topReadyQueue == topPriority) {
                    readyQueue.poll();
                    readyQueueEmpty = readyQueue.isEmpty();
                } else {
                    executableActions.poll();
                    executableActionsEmpty = executableActions.isEmpty();
                }
            } catch (UnassignedActionException uae) {
                canExecute = false;
            } catch (BlockedActionException bae) {
                this.addToBlocked(aa);
            }
        }
        // Merge both queues in one
        if (!executableActions.isEmpty()) {
            readyQueue.addAll(executableActions);
        }
    }

}
