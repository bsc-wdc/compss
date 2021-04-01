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

import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.Semaphore;
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
    protected final HashSet<ResourceScheduler<?>> availableWorkers;
    protected final HashMap<ResourceScheduler<?>, Future<?>> resourceTokens;
    protected int amountOfWorkers;
    ThreadPoolExecutor schedulerExecutor;
    protected Set<AllocatableAction> upgradedActions;


    /**
     * Constructs a new Ready Scheduler instance.
     */
    public ReadyScheduler() {
        super();
        this.resourceTokens = new HashMap<>();
        this.availableWorkers = new HashSet<>();
        this.amountOfWorkers = 0;
        this.schedulerExecutor = new ThreadPoolExecutor(15, 40, 180, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.schedulerExecutor.allowCoreThreadTimeOut(true);
        this.upgradedActions = new HashSet<>();
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
        if (this.getWorkers().size() > 0) {
            for (AllocatableAction action : getUnassignedActions()) {
                Score actionScore = generateActionScore(action);
                addActionToResource((ReadyResourceScheduler<?>) resource, actionScore, action);
            }
        }
        this.availableWorkers.add(resource);
        this.resourceTokens.put(resource, null);
        this.amountOfWorkers += 1;
    }

    @Override
    protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
        this.availableWorkers.remove(resource);
        this.resourceTokens.remove(resource);
        this.amountOfWorkers -= 1;
        ReadyResourceScheduler<?> rs = (ReadyResourceScheduler<?>) resource;
        if (this.amountOfWorkers == 0) {
            for (ObjectValue<AllocatableAction> actionValue : rs.getUnassignedActions()) {
                AllocatableAction action = actionValue.getObject();
                addToBlocked(action);
            }
        }
        rs.resetUnassignedActions();
        super.workerRemoved(resource);
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
    public Collection<AllocatableAction> getUnassignedActions() {
        Set<AllocatableAction> unassigned = new HashSet<>();
        Iterator<ResourceScheduler<?>> iter = this.getWorkers().iterator();
        while (iter.hasNext()) {
            ReadyResourceScheduler<?> resource = (ReadyResourceScheduler<?>) iter.next();
            Future<?> resourceToken = this.resourceTokens.get(resource);
            if (resourceToken != null) {
                try {
                    resourceToken.get();
                } catch (InterruptedException | ExecutionException e) {
                    LOGGER.fatal("Unexpected thread interruption", e);
                    ErrorManager.fatal("Unexpected thread interruption", e);
                }
            }
            for (ObjectValue<AllocatableAction> actionObject : resource.getUnassignedActions()) {
                unassigned.add(actionObject.getObject());
            }
        }
        return unassigned;
    }

    @Override
    public final <T extends WorkerResourceDescription> void handleDependencyFreeActions(
        List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
        List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {
        if (DEBUG) {
            LOGGER.debug("[ReadyScheduler] Handling dependency free actions on resource " + resource.getName());
        }
        manageUpgradedActions(resource);
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

    private void manageUpgradedActions(ResourceScheduler<?> resource) {

        if (!upgradedActions.isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Managing " + upgradedActions.size() + " upgraded actions.");
            }
            Set<ResourceScheduler<?>> candidates = new HashSet<>();
            candidates.add(resource);
            PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
            for (AllocatableAction action : upgradedActions) {
                Score fullScore = action.schedulingScore(resource, this.generateActionScore(action));
                ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
                executableActions.add(obj);
            }
            while (!executableActions.isEmpty() && resource.canRunSomething()) {
                ObjectValue<AllocatableAction> obj = executableActions.poll();
                AllocatableAction freeAction = obj.getObject();
                try {
                    freeAction.tryToSchedule(obj.getScore(), candidates);
                    tryToLaunch(freeAction);
                    upgradedActions.remove(freeAction);
                } catch (BlockedActionException | UnassignedActionException e) {
                    // Nothing to do It could be scheduled in another resource
                }
            }
        }

    }

    private void updateActionToSchedulerStructures(AllocatableAction action) {

        if (!this.getWorkers().isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Updating action to scheduler structures " + action);
            }
            if (action.isTargetResourceEnforced()) {
                // Add in enforced resource only
                ReadyResourceScheduler<?> resource = (ReadyResourceScheduler<?>) action.getEnforcedTargetResource();
                updateActionInResource(resource, action);
            } else {
                updateActionInAllResources(action);
            }

        } else {
            if (DEBUG) {
                LOGGER.debug(
                    "[ReadyScheduler] Cannot add action " + action + " because there are not available resources");
            }
            addToBlocked(action);
        }

    }

    private void addActionToSchedulerStructures(AllocatableAction action) {

        if (!this.getWorkers().isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Add action to scheduler structures " + action);
            }
            Score actionScore = generateActionScore(action);
            if (action.isTargetResourceEnforced()) {
                // Add in enforced resource only
                ReadyResourceScheduler<?> resource = (ReadyResourceScheduler<?>) action.getEnforcedTargetResource();
                addActionToResource(resource, actionScore, action);
            } else {
                addActionInAllResources(actionScore, action);
            }

        } else {
            if (DEBUG) {
                LOGGER.debug(
                    "[ReadyScheduler] Cannot add action " + action + " because there are not available resources");
            }
            addToBlocked(action);
        }

    }

    private void addActionInAllResources(Score actionScore, AllocatableAction action) {
        // Add action in all workers
        Iterator<ResourceScheduler<?>> iter = getWorkers().iterator();
        if (iter.hasNext()) {
            ReadyResourceScheduler<?> resource = (ReadyResourceScheduler<?>) iter.next();
            if (!addActionToResource(resource, actionScore, action)) {
                if (DEBUG) {
                    LOGGER.debug("[ReadyScheduler] Action already added " + action);
                }
                return;
            }
            while (iter.hasNext()) {
                resource = (ReadyResourceScheduler<?>) iter.next();
                Future<?> lastToken = this.resourceTokens.get(resource);
                this.resourceTokens.put(resource,
                    schedulerExecutor.submit(new AddRunnable(resource, actionScore, action, lastToken)));
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] No resources to addAction");
            }
        }

    }

    private void updateActionInAllResources(AllocatableAction action) {
        // Add action in all workers
        Iterator<ResourceScheduler<?>> iter = getWorkers().iterator();
        while (iter.hasNext()) {
            ReadyResourceScheduler<?> resource = (ReadyResourceScheduler<?>) iter.next();
            Future<?> lastToken = this.resourceTokens.get(resource);
            this.resourceTokens.put(resource,
                schedulerExecutor.submit(new UpdateRunnable(resource, action, lastToken)));
        }

    }

    private void removeActionFromSchedulerStructures(AllocatableAction action) {
        if (!this.getWorkers().isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Remove action from scheduler structures " + action);
            }
            if (action.isTargetResourceEnforced()) {
                ReadyResourceScheduler<?> resource = (ReadyResourceScheduler<?>) action.getEnforcedTargetResource();
                removeActionFromResource(resource, action);
            } else {
                removeActionFromAllResources(action);
            }
        }
    }

    private void removeActionFromAllResources(AllocatableAction action) {

        Iterator<ResourceScheduler<?>> iter = getWorkers().iterator();
        if (iter.hasNext()) {
            ReadyResourceScheduler<?> resource = (ReadyResourceScheduler<?>) iter.next();
            if (!removeActionFromResource(resource, action)) {
                if (DEBUG) {
                    LOGGER.debug("[ReadyScheduler] Action already added " + action);
                }
                return;
            }

            while (iter.hasNext()) {
                resource = (ReadyResourceScheduler<?>) iter.next();
                Future<?> lastToken = this.resourceTokens.get(resource);
                this.resourceTokens.put(resource,
                    schedulerExecutor.submit(new RemoveRunnable(resource, action, lastToken)));
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] No resources to addAction");
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
                LOGGER.debug("[ReadyScheduler] Introducing data free action " + freeAction + " into the scheduler.");
            }
            addActionToSchedulerStructures(freeAction);
        }

        // Resource free actions should always be empty in this scheduler
        for (AllocatableAction freeAction : resourceFreeActions) {
            if (DEBUG) {
                LOGGER
                    .debug("[ReadyScheduler] Introducing resource free action " + freeAction + " into the scheduler.");
            }
            addActionToSchedulerStructures(freeAction);
        }

        // Only in case there are actions that have entered the scheduler without having
        // available resources -> They were in the blocked list
        for (AllocatableAction freeAction : blockedCandidates) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Introducing blocked action " + freeAction + " into the scheduler.");
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
        Semaphore sem = new Semaphore(0);
        this.resourceTokens.put(resource, schedulerExecutor.submit(new WaitSchedulingRunnable(resource, sem)));

        Set<ObjectValue<AllocatableAction>> unassignedActions =
            ((ReadyResourceScheduler<?>) resource).getUnassignedActions();
        if (unassignedActions != null) {
            HashSet<ObjectValue<AllocatableAction>> objectValueToErase = new HashSet<>();
            Iterator<ObjectValue<AllocatableAction>> executableActionsIterator = unassignedActions.iterator();
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler]  ***** Trying to schedule " + unassignedActions.size()
                    + " unassigned actions to " + this.availableWorkers.size() + " workers");
            }
            while (executableActionsIterator.hasNext() && !this.availableWorkers.isEmpty()) {
                ObjectValue<AllocatableAction> obj = executableActionsIterator.next();
                AllocatableAction freeAction = obj.getObject();
                if (DEBUG) {
                    LOGGER.debug("[ReadyScheduler] -- Trying to schedule " + freeAction);
                }
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
                    if (assignedResource != null) {
                        if (DEBUG) {
                            LOGGER.debug("[ReadyScheduler] -- Action " + freeAction
                                + " successfully scheduled and launched in " + assignedResource.getName());
                        }
                        if (!assignedResource.canRunSomething()) {
                            this.availableWorkers.remove(assignedResource);
                        }
                    }
                    objectValueToErase.add(obj);
                } catch (BlockedActionException e) {
                    if (DEBUG) {
                        LOGGER.debug("[ReadyScheduler] -- Action " + freeAction
                            + " added to blocked and removed from unassigned.");
                    }
                    objectValueToErase.add(obj);
                    addToBlocked(freeAction);
                } catch (UnassignedActionException e) {
                    if (DEBUG) {
                        LOGGER.debug("[ReadyScheduler] -- Action " + freeAction
                            + " could not be assigned to any of the available resources");
                    }
                    // Nothing to be done here since the action was already in the scheduler
                    // structures. If there is an exception, the freeAction will not be added
                    // to the objectValueToErase list.
                    // Hence, this is not an ignored Exception but an expected behavior.
                }
            }
            sem.release();
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler]  ***** Unassigned actions scheduling finished. Removing "
                    + objectValueToErase.size() + " actions from scheduler structures.");
            }
            for (ObjectValue<AllocatableAction> obj : objectValueToErase) {
                AllocatableAction action = obj.getObject();
                removeActionFromSchedulerStructures(action);
            }
        }
        if (DEBUG) {
            LOGGER
                .debug("[ReadyScheduler] Try to launch free actions on resource " + resource.getName() + " finished.");
        }
    }

    private boolean removeActionFromResource(ReadyResourceScheduler<?> scheduler, AllocatableAction action) {
        Set<ObjectValue<AllocatableAction>> actionList = scheduler.getUnassignedActions();
        ObjectValue<AllocatableAction> obj = scheduler.getAddedActions().remove(action);
        if (obj != null) {
            return actionList.remove(obj);
        } else {
            return false;
        }

    }

    private boolean addActionToResource(ReadyResourceScheduler<?> scheduler, Score actionScore,
        AllocatableAction action) {
        Set<ObjectValue<AllocatableAction>> actionList = scheduler.getUnassignedActions();
        Score fullScore = action.schedulingScore(scheduler, actionScore);
        ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
        boolean added = actionList.add(obj);
        if (added) {
            scheduler.getAddedActions().put(action, obj);
        }
        return added;

    }

    private void updateActionInResource(ReadyResourceScheduler<?> scheduler, AllocatableAction action) {
        Set<ObjectValue<AllocatableAction>> actionList = scheduler.getUnassignedActions();
        ObjectValue<AllocatableAction> obj = scheduler.getAddedActions().remove(action);
        if (obj != null) {
            if (actionList.remove(obj)) { // NOSONAR
                Score fullScore = action.schedulingScore(scheduler, generateActionScore(action));
                obj = new ObjectValue<>(action, fullScore);
                boolean added = actionList.add(obj);
                if (added) {
                    scheduler.getAddedActions().put(action, obj);
                }
            }
        }
    }

    @Override
    public void upgradeAction(AllocatableAction action) {
        if (DEBUG) {
            LOGGER.debug(" Upgrading action " + action);
        }
        upgradedActions.add(action);
        updateActionToSchedulerStructures(action);
    }


    private class RemoveRunnable implements Runnable {

        private ReadyResourceScheduler<?> scheduler;
        private AllocatableAction action;
        private Future<?> token;


        public RemoveRunnable(ReadyResourceScheduler<?> scheduler, AllocatableAction action, Future<?> token) {
            this.scheduler = scheduler;
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
            removeActionFromResource(scheduler, action);
        }

    }

    private class WaitSchedulingRunnable implements Runnable {

        private ResourceScheduler<?> scheduler;
        private Semaphore sem;


        public WaitSchedulingRunnable(ResourceScheduler<?> resource, Semaphore sem) {
            this.scheduler = resource;
            this.sem = sem;
        }

        public void run() {
            try {
                this.sem.acquire();
            } catch (InterruptedException e) {
                // Nothing to do
            }
        }

    }

    private class AddRunnable implements Runnable {

        private ReadyResourceScheduler<?> scheduler;
        private AllocatableAction action;
        private Future<?> token;
        private Score actionScore;


        public AddRunnable(ReadyResourceScheduler<?> scheduler, Score actionScore, AllocatableAction action,
            Future<?> token) {
            this.scheduler = scheduler;
            this.actionScore = actionScore;
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
            addActionToResource(scheduler, actionScore, action);
        }
    }

    private class UpdateRunnable implements Runnable {

        private ReadyResourceScheduler<?> scheduler;
        private AllocatableAction action;
        private Future<?> token;


        public UpdateRunnable(ReadyResourceScheduler<?> scheduler, AllocatableAction action, Future<?> token) {
            this.scheduler = scheduler;
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
            updateActionInResource(scheduler, action);
        }

    }

}
