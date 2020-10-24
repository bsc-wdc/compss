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
package es.bsc.compss.scheduler.readynew;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.TreeSet;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Representation of a Scheduler that considers only ready tasks.
 */
public abstract class ReadyScheduler extends TaskScheduler {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);

    protected HashMap<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> unassignedReadyActions;
    protected HashMap<AllocatableAction, ObjectValue<AllocatableAction>> addedActions;
    protected final HashSet<ResourceScheduler<?>> availableWorkers;
    protected final HashMap<ResourceScheduler<?>, Future<?>> resourceTokens;
    protected int amountOfWorkers;
    ThreadPoolExecutor schedulerExecutor;


    /**
     * Constructs a new Ready Scheduler instance.
     */
    public ReadyScheduler() {
        super();
        this.unassignedReadyActions = new HashMap<>();
        this.addedActions = new HashMap<>();
        this.resourceTokens = new HashMap<>();
        this.availableWorkers = new HashSet<>();
        this.amountOfWorkers = 0;
        this.schedulerExecutor = new ThreadPoolExecutor(15, 40, 180, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.schedulerExecutor.allowCoreThreadTimeOut(true);
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
        if (resource.canRunSomething()) {
            this.availableWorkers.add(resource);
        }
    }

    @Override
    public void shutdown() {
        schedulerExecutor.shutdown();
        super.shutdown();
    }

    @Override
    protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
        super.workerDetected(resource);
        if (unassignedReadyActions.size() > 0) {
            TreeSet<ObjectValue<AllocatableAction>> orderedActions = new TreeSet<>();
            Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter =
                unassignedReadyActions.entrySet().iterator();
            TreeSet<ObjectValue<AllocatableAction>> actionList = iter.next().getValue();
            for (ObjectValue<AllocatableAction> actionValue : actionList) {
                AllocatableAction action = actionValue.getObject();
                Score actionScore = generateActionScore(action);
                Score fullScore = action.schedulingScore(resource, actionScore);
                ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
                orderedActions.add(obj);
            }
            this.unassignedReadyActions.put(resource, orderedActions);
        } else {
            TreeSet<ObjectValue<AllocatableAction>> orderedActions = new TreeSet<>();
            this.unassignedReadyActions.put(resource, orderedActions);
        }
        this.availableWorkers.add(resource);
        this.resourceTokens.put(resource, null);
        this.amountOfWorkers += 1;
    }

    @Override
    protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
        super.workerRemoved(resource);
        this.availableWorkers.remove(resource);
        this.resourceTokens.remove(resource);
        this.amountOfWorkers -= 1;
        if (this.amountOfWorkers == 0) {
            for (ObjectValue<AllocatableAction> actionValue : this.unassignedReadyActions.get(resource)) {
                AllocatableAction action = actionValue.getObject();
                addToBlocked(action);
            }
        }
        this.unassignedReadyActions.remove(resource);
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
                List<ResourceScheduler<?>> uselessWorkers =
                    action.tryToSchedule(generateActionScore(action), this.availableWorkers);
                for (ResourceScheduler<?> worker : uselessWorkers) {
                    this.availableWorkers.remove(worker);
                }
                ResourceScheduler<? extends WorkerResourceDescription> resource = action.getAssignedResource();
                if (!resource.canRunSomething()) {
                    this.availableWorkers.remove(resource);
                }
                removeActionFromSchedulerStructures(action);
            } catch (UnassignedActionException ex) {
                addActionToSchedulerStructures(action);
            }
        }
    }

    protected <T extends WorkerResourceDescription> void scheduleAction(AllocatableAction action,
        ResourceScheduler<T> targetWorker, Score actionScore) throws BlockedActionException, UnassignedActionException {
        if (!action.hasDataPredecessors() && !action.hasStreamProducers()) {
            action.schedule(targetWorker, actionScore);
            removeActionFromSchedulerStructures(action);
        }
    }

    @Override
    public final void newAllocatableAction(AllocatableAction action) {
        LOGGER.info("[ReadyScheduler] Registering new AllocatableAction " + action);
        if (!action.hasDataPredecessors()) {
            addToReady(action);
            if (!this.availableWorkers.isEmpty()) {
                try {
                    List<ResourceScheduler<?>> uselessWorkers =
                        action.tryToSchedule(generateActionScore(action), this.availableWorkers);
                    for (ResourceScheduler<?> worker : uselessWorkers) {
                        this.availableWorkers.remove(worker);
                    }
                    tryToLaunch(action);
                } catch (BlockedActionException bae) {
                    removeFromReady(action);
                    addToBlocked(action);
                } catch (UnassignedActionException uae) {
                    addActionToSchedulerStructures(action);
                }
            } else {
                addActionToSchedulerStructures(action);
            }
        }
    }

    @Override
    public List<AllocatableAction> getUnassignedActions() {
        LinkedList<AllocatableAction> unassigned = new LinkedList<>();
        Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter =
            unassignedReadyActions.entrySet().iterator();
        TreeSet<ObjectValue<AllocatableAction>> actionList = iter.next().getValue();
        @SuppressWarnings("unchecked")
        ResourceScheduler<?> resource =
            ((Map.Entry<ResourceScheduler<?>, HashSet<ObjectValue<AllocatableAction>>>) iter).getKey();
        Future<?> resourceToken = this.resourceTokens.get(resource);
        if (resourceToken != null) {
            try {
                resourceToken.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.fatal("Unexpected thread interruption", e);
                ErrorManager.fatal("Unexpected thread interruption", e);
            }
        }
        for (ObjectValue<AllocatableAction> actionObject : actionList) {
            unassigned.add(actionObject.getObject());
        }
        return unassigned;
    }

    @Override
    public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
        if (DEBUG) {
            LOGGER.debug("[ReadyScheduler] Treating dependency free actions on resource " + resource.getName());
        }
        if (resource.canRunSomething()) {
            this.availableWorkers.add(resource);
        }
        purgeFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
        tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
    }

    public <T extends WorkerResourceDescription> void purgeFreeActions(List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
        ResourceScheduler<T> resource) {
    }

    private void addActionToSchedulerStructures(AllocatableAction action) {

        if (!this.unassignedReadyActions.isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Add action to scheduler structures " + action);
            }
            if (action.isTargetResourceEnforced()) {
                ResourceScheduler<?> resource = action.getEnforcedTargetResource();
                TreeSet<ObjectValue<AllocatableAction>> actionList = this.unassignedReadyActions.get(resource);
                Score actionScore = generateActionScore(action);
                Score fullScore = action.schedulingScore(resource, actionScore);
                ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
                if (!actionList.add(obj)) {
                    return;
                }
                addedActions.put(action, obj);
            } else {
                Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter =
                    unassignedReadyActions.entrySet().iterator();
                Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry = iter.next();
                TreeSet<ObjectValue<AllocatableAction>> actionList = currentEntry.getValue();

                ResourceScheduler<?> resource = currentEntry.getKey();

                Score actionScore = generateActionScore(action);
                Score fullScore = action.schedulingScore(resource, actionScore);

                ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
                if (!actionList.add(obj)) {
                    return;
                }
                addedActions.put(action, obj);

                while (iter.hasNext()) {
                    currentEntry = iter.next();
                    resource = currentEntry.getKey();
                    Future<?> lastToken = this.resourceTokens.get(resource);
                    this.resourceTokens.put(resource,
                        schedulerExecutor.submit(new AddRunnable(currentEntry, action, lastToken)));
                }
            }

        } else {
            if (DEBUG) {
                LOGGER.debug(
                    "[ReadyScheduler] Cannot add action " + action + " because there are not available resources");
            }
            addToBlocked(action);
        }

    }

    private void removeActionFromSchedulerStructures(AllocatableAction action) {
        if (!this.unassignedReadyActions.isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Remove action from scheduler structures " + action);
            }
            if (action.isTargetResourceEnforced()) {
                ResourceScheduler<?> resource = action.getEnforcedTargetResource();
                TreeSet<ObjectValue<AllocatableAction>> actionList = this.unassignedReadyActions.get(resource);
                ObjectValue<AllocatableAction> obj = addedActions.get(action);
                actionList.remove(obj);
            } else {
                Iterator<Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>>> iter =
                    unassignedReadyActions.entrySet().iterator();
                Map.Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry = iter.next();
                TreeSet<ObjectValue<AllocatableAction>> actionList = currentEntry.getValue();
                ObjectValue<AllocatableAction> obj = addedActions.get(action);

                if (!actionList.remove(obj)) {
                    return;
                }

                while (iter.hasNext()) {
                    currentEntry = iter.next();
                    ResourceScheduler<?> resource = currentEntry.getKey();
                    Future<?> lastToken = this.resourceTokens.get(resource);
                    this.resourceTokens.put(resource,
                        schedulerExecutor.submit(new RemoveRunnable(currentEntry, action, lastToken)));
                }
            }
        }
    }

    protected <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
        ResourceScheduler<T> resource) {
        if (DEBUG) {
            LOGGER.debug("[ReadyScheduler] Try to launch free actions on resource " + resource.getName());
        }

        // Actions that have been freeded by the action that just finished
        for (AllocatableAction freeAction : dataFreeActions) {
            if (DEBUG) {
                LOGGER
                    .debug("[ReadyScheduler] Introducing action " + freeAction + " into the scheduler from data free");
            }
            addActionToSchedulerStructures(freeAction);
        }

        // Resource free actions should always be empty in this scheduler
        for (AllocatableAction freeAction : resourceFreeActions) {
            if (DEBUG) {
                LOGGER.debug(
                    "[ReadyScheduler] Introducing action " + freeAction + " into the scheduler from resource free");
            }
            addActionToSchedulerStructures(freeAction);
        }

        // Only in case there are actions that have entered the scheduler without having
        // available resources -> They were in the blocked list
        for (AllocatableAction freeAction : blockedCandidates) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Introducing action " + freeAction + " into the scheduler from blocked");
            }
            addActionToSchedulerStructures(freeAction);
        }

        Future<?> lastToken = this.resourceTokens.get(resource);
        if (lastToken != null) {
            try {
                lastToken.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.fatal("Unexpected thread interruption", e);
                ErrorManager.fatal("Unexpected thread interruption", e);
            }
        }
        this.resourceTokens.put(resource, null);
        TreeSet<ObjectValue<AllocatableAction>> unassignedActions = this.unassignedReadyActions.get(resource);
        if (unassignedActions != null) {
            Iterator<ObjectValue<AllocatableAction>> executableActionsIterator = unassignedActions.iterator();
            HashSet<ObjectValue<AllocatableAction>> objectValueToErase = new HashSet<>();
            while (executableActionsIterator.hasNext() && !this.availableWorkers.isEmpty()) {
                ObjectValue<AllocatableAction> obj = executableActionsIterator.next();
                AllocatableAction freeAction = obj.getObject();

                try {
                    List<ResourceScheduler<?>> uselessWorkers = null;
                    if (freeAction.isTargetResourceEnforced()) {
                        if (resource.canRunSomething()) {
                            HashSet<ResourceScheduler<?>> resourceSet = new HashSet<>();
                            resourceSet.add(resource);
                            uselessWorkers = freeAction.tryToSchedule(generateActionScore(freeAction), resourceSet);
                        } else {
                            throw new UnassignedActionException();
                        }
                    } else {
                        uselessWorkers =
                            freeAction.tryToSchedule(generateActionScore(freeAction), this.availableWorkers);
                    }

                    this.availableWorkers.removeAll(uselessWorkers);
                    ResourceScheduler<? extends WorkerResourceDescription> assignedResource =
                        freeAction.getAssignedResource();
                    tryToLaunch(freeAction);
                    if (assignedResource != null && !assignedResource.canRunSomething()) {
                        this.availableWorkers.remove(assignedResource);
                    }

                    objectValueToErase.add(obj);
                } catch (BlockedActionException e) {
                    objectValueToErase.add(obj);
                    addToBlocked(freeAction);
                } catch (UnassignedActionException e) {
                    if (DEBUG) {
                        LOGGER.debug("[ReadyScheduler] Action " + freeAction
                            + " could not be assigned to any of the available resources");
                    }
                    // Nothing to be done here since the action was already in the scheduler
                    // structures. If there is an exception, the freeAction will not be added
                    // to the objectValueToErase list.
                    // Hence, this is not an ignored Exception but an expected behavior.
                }
            }
            for (ObjectValue<AllocatableAction> obj : objectValueToErase) {
                AllocatableAction action = obj.getObject();
                removeActionFromSchedulerStructures(action);
            }
        }
    }


    private class RemoveRunnable implements Runnable {

        private Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry;
        private AllocatableAction action;
        private Future<?> token;


        public RemoveRunnable(Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry,
            AllocatableAction action, Future<?> token) {
            this.currentEntry = currentEntry;
            this.action = action;
            this.token = token;
        }

        public void run() {
            if (token != null) {
                try {
                    token.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.fatal("Unexpected thread interruption", e);
                    ErrorManager.fatal("Unexpected thread interruption", e);
                    // Restore interrupted state.
                    Thread.currentThread().interrupt();

                }
            }
            removeActionFromResource(currentEntry, action);
        }

        private void removeActionFromResource(
            Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry,
            AllocatableAction action) {
            currentEntry.getKey();
            TreeSet<ObjectValue<AllocatableAction>> actionList = currentEntry.getValue();
            ObjectValue<AllocatableAction> obj = addedActions.get(action);
            actionList.remove(obj);
        }
    }

    private class AddRunnable implements Runnable {

        private Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry;
        private AllocatableAction action;
        private Future<?> token;


        public AddRunnable(Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry,
            AllocatableAction action, Future<?> token) {
            this.currentEntry = currentEntry;
            this.action = action;
            this.token = token;
        }

        public void run() {
            if (token != null) {
                try {
                    token.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.fatal("Unexpected thread interruption", e);
                    ErrorManager.fatal("Unexpected thread interruption", e);
                    // Restore interrupted state.
                    Thread.currentThread().interrupt();
                }
            }
            addActionToResource(currentEntry, action);
        }

        private void addActionToResource(
            Entry<ResourceScheduler<?>, TreeSet<ObjectValue<AllocatableAction>>> currentEntry,
            AllocatableAction action) {
            TreeSet<ObjectValue<AllocatableAction>> actionList = currentEntry.getValue();
            ObjectValue<AllocatableAction> obj = addedActions.get(action);
            actionList.add(obj);
        }

    }

}
