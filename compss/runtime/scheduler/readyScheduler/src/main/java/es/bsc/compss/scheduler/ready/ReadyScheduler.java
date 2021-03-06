/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.ready;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ActionSet;
import java.util.LinkedList;

import java.util.List;
import java.util.PriorityQueue;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Representation of a Scheduler that considers only ready tasks.
 */
public abstract class ReadyScheduler extends TaskScheduler {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    protected final ActionSet unassignedReadyActions;


    /**
     * Constructs a new Ready Scheduler instance.
     */
    public ReadyScheduler() {
        super();
        this.unassignedReadyActions = new ActionSet();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public <T extends WorkerResourceDescription> void workerLoadUpdate(ResourceScheduler<T> resource) {
        LOGGER.debug("[ReadyScheduler] Update load on worker " + resource.getName() + ". Nothing to do.");
    }

    @Override
    public <T extends WorkerResourceDescription> void workerFeaturesUpdate(ResourceScheduler<T> worker, T modification,
        List<AllocatableAction> unblockedActions, List<AllocatableAction> blockedCandidates) {
        List<AllocatableAction> dataFreeActions = new LinkedList<>();
        List<AllocatableAction> resourceFreeActions = unblockedActions;
        purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, worker);
        tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, worker);
    }

    @Override
    public abstract Score generateActionScore(AllocatableAction action);

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ********************************* SCHEDULING OPERATIONS *************************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {
        if (!action.hasDataPredecessors() && !action.hasStreamProducers()) {
            try {
                action.schedule(actionScore);
            } catch (UnassignedActionException ex) {
                this.unassignedReadyActions.addAction(action);
            }
        }
    }

    protected <T extends WorkerResourceDescription> void scheduleAction(AllocatableAction action,
        ResourceScheduler<T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        if (!action.hasDataPredecessors() && !action.hasStreamProducers()) {
            action.schedule(targetWorker, actionScore);
        }
    }

    @Override
    public List<AllocatableAction> getUnassignedActions() {
        return unassignedReadyActions.getAllActions();
    }

    @Override
    public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
        LOGGER.debug("[ReadyScheduler] Handling dependency free actions.");
        purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
        tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
    }

    protected abstract <T extends WorkerResourceDescription> void purgeFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource);

    private <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
        ResourceScheduler<T> resource) {

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
            Score actionScore = obj.getScore();

            LOGGER.debug("[ReadyScheduler] Trying to launch action " + freeAction);
            try {
                scheduleAction(freeAction, actionScore);
                tryToLaunch(freeAction);
            } catch (BlockedActionException e) {
                blockedCandidates.add(freeAction);
            }
        }
    }
}
