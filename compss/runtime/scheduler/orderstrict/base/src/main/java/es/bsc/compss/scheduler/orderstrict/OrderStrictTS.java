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

package es.bsc.compss.scheduler.orderstrict;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.PriorityQueue;
import java.util.Set;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks.
 */
public abstract class OrderStrictTS extends TaskScheduler {

    protected final PriorityQueue<ObjectValue<AllocatableAction>> readyQueue;
    protected Set<AllocatableAction> upgradedActions;
    protected final Map<AllocatableAction, ObjectValue<AllocatableAction>> addedActions;


    /**
     * Constructs a new Ready Scheduler instance.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public OrderStrictTS(ActionOrchestrator orchestrator) {
        super(orchestrator);
        LOGGER.debug("[OrderStrict] Loading OrderStrict TS");
        this.readyQueue = new PriorityQueue<>();
        this.upgradedActions = new HashSet<>();
        this.addedActions = new HashMap<>();
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
        generateSchedulingInformation(ResourceScheduler<T> rs, List<? extends Parameter> params, Integer coreId) {
        return new SchedulingInformation(rs);
    }

    @Override
    public void upgradeAction(AllocatableAction action) {
        if (DEBUG) {
            LOGGER.debug("[OrderStrict] Upgrading action " + action);
        }
        upgradedActions.add(action);
        ObjectValue<AllocatableAction> obj = addedActions.remove(action);
        readyQueue.remove(obj);

    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    private PriorityQueue<ObjectValue<AllocatableAction>> sortActions(Iterable<AllocatableAction> actions) {

        if (DEBUG) {
            LOGGER.debug("[OrcerStrict] Managing " + upgradedActions.size() + " upgraded actions.");
        }
        PriorityQueue<ObjectValue<AllocatableAction>> sortedActions = new PriorityQueue<>();
        for (AllocatableAction action : actions) {
            Score fullScore = this.generateActionScore(action);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
            sortedActions.add(obj);
        }

        return sortedActions;
    }

    private void manageUpgradedActions(ResourceScheduler<?> resource) {
        if (!upgradedActions.isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[OrderStrict] Managing " + upgradedActions.size() + " upgraded actions.");
            }

            PriorityQueue<ObjectValue<AllocatableAction>> executableActions = sortActions(upgradedActions);

            while (!executableActions.isEmpty()) {
                ObjectValue<AllocatableAction> obj = executableActions.poll();
                AllocatableAction freeAction = obj.getObject();
                if (freeAction.getCompatibleWorkers().contains(resource) && resource.canRunSomething()) {
                    try {
                        freeAction.schedule(resource, obj.getScore());
                        tryToLaunch(freeAction);
                        upgradedActions.remove(freeAction);

                    } catch (UnassignedActionException e) {
                        // Nothing to do it could be scheduled in another resource
                    }
                }
            }
        }
    }

    private void addActionToReadyQueue(AllocatableAction action, Score actionScore) {
        ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, actionScore);
        addedActions.put(action, obj);
        readyQueue.add(obj);
    }

    @Override
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
        if (!action.hasDataPredecessors()) {
            if (upgradedActions.isEmpty()) {
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
    }

    @Override
    public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
        manageUpgradedActions(resource);

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
                    addedActions.remove(aa);
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
