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
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ErrorManager;

import java.util.Collection;
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


/**
 * Representation of a Scheduler that considers only ready tasks.
 */
public abstract class LookaheadTS extends TaskScheduler {

    private final ThreadPoolExecutor schedulerExecutor;

    protected final HashSet<ResourceScheduler<? extends WorkerResourceDescription>> availableWorkers;

    protected Set<AllocatableAction> upgradedActions;


    /**
     * Constructs a new Ready Scheduler instance.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public LookaheadTS(ActionOrchestrator orchestrator) {
        super(orchestrator);
        this.availableWorkers = new HashSet<>();
        this.schedulerExecutor = new ThreadPoolExecutor(15, 40, 180, TimeUnit.SECONDS, new LinkedBlockingQueue<>());
        this.schedulerExecutor.allowCoreThreadTimeOut(true);
        this.upgradedActions = new HashSet<>();
    }

    @Override
    public void customSchedulerShutdown() {
        schedulerExecutor.shutdown();
    }

    /*
     * *********************************************************************************************************
     * *********************************************************************************************************
     * ***************************** UPDATE STRUCTURES OPERATIONS **********************************************
     * *********************************************************************************************************
     * *********************************************************************************************************
     */
    @Override
    public void customCoreElementsUpdated() {
        for (ResourceScheduler w : getWorkers()) {
            if (w.canRunSomething()) {
                this.availableWorkers.add(w);
            }
        }
    }

    @Override
    public <T extends WorkerResourceDescription> void workerLoadUpdate(ResourceScheduler<T> resource) {
        LOGGER.debug("[ReadyScheduler] Update load on worker " + resource.getName() + ". Nothing to do.");
        if (resource.canRunSomething()) {
            this.availableWorkers.add(resource);
        }
    }

    @Override
    protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
        super.workerDetected(resource);
        if (this.getWorkers().size() > 0) {
            LookaheadRS<?> rs = (LookaheadRS<?>) resource;
            for (AllocatableAction action : getUnassignedActions()) {
                Score actionScore = generateActionScore(action);
                rs.addAction(action, actionScore);
            }
        }
        this.availableWorkers.add(resource);
    }

    @Override
    protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
        this.availableWorkers.remove(resource);
        LookaheadRS<?> rs = (LookaheadRS<?>) resource;
        if (this.availableWorkers.isEmpty()) {
            for (ObjectValue<AllocatableAction> actionValue : rs.getUnassignedActions()) {
                AllocatableAction action = actionValue.getObject();
                addToBlocked(action);
            }
        }
        rs.clear();
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
                if (action.getCompatibleWorkers().isEmpty()) {
                    throw new BlockedActionException();
                }
                if (!this.availableWorkers.isEmpty()) {
                    action.schedule(this.getAvailableWorkers(), generateActionScore(action));
                    removeActionFromSchedulerStructures(action);
                } else {
                    addActionToSchedulerStructures(action);
                }
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
    public Collection<AllocatableAction> getUnassignedActions() {
        Set<AllocatableAction> unassigned = new HashSet<>();
        Iterator<ResourceScheduler<?>> iter = this.getWorkers().iterator();
        while (iter.hasNext()) {
            LookaheadRS<?> resource = (LookaheadRS<?>) iter.next();
            Future<?> resourceToken = resource.getToken();
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
            LOGGER.debug("[ReadyScheduler] Handling dependency free actions.");
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
            PriorityQueue<ObjectValue<AllocatableAction>> executableActions = new PriorityQueue<>();
            for (AllocatableAction action : upgradedActions) {
                Score fullScore = action.schedulingScore(resource, this.generateActionScore(action));
                ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
                executableActions.add(obj);
            }

            while (!executableActions.isEmpty() && resource.canRunSomething()) {
                ObjectValue<AllocatableAction> obj = executableActions.poll();
                AllocatableAction freeAction = obj.getObject();
                if (freeAction.getCompatibleWorkers().contains(resource)) {
                    try {
                        freeAction.schedule(resource, obj.getScore());
                        tryToLaunch(freeAction);
                        upgradedActions.remove(freeAction);
                        removeActionFromSchedulerStructures(freeAction);
                    } catch (UnassignedActionException e) {
                        // Nothing to do it could be scheduled in another resource
                    }
                }
            }

            if (!resource.canRunSomething()) {
                this.availableWorkers.remove(resource);
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
                LookaheadRS<?> resource = (LookaheadRS<?>) action.getEnforcedTargetResource();
                Score actionScore = generateActionScore(action);
                resource.updateAction(action, actionScore);
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
                LookaheadRS<?> resource = (LookaheadRS<?>) action.getEnforcedTargetResource();
                resource.addAction(action, actionScore);
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
            LookaheadRS<?> resource = (LookaheadRS<?>) iter.next();
            if (!resource.addAction(action, actionScore)) {
                if (DEBUG) {
                    LOGGER.debug("[ReadyScheduler] Action already added " + action);
                }
                return;
            }
            while (iter.hasNext()) {
                resource = (LookaheadRS<?>) iter.next();
                Future<?> lastToken = resource.getToken();
                resource.setToken(schedulerExecutor.submit(new AddRunnable(resource, actionScore, action, lastToken)));
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
            LookaheadRS<?> resource = (LookaheadRS<?>) iter.next();
            Future<?> lastToken = resource.getToken();
            resource.setToken(schedulerExecutor.submit(new UpdateRunnable(resource, action, lastToken)));
        }

    }

    private void removeActionFromSchedulerStructures(AllocatableAction action) {
        if (!this.getWorkers().isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Remove action from scheduler structures " + action);
            }
            if (action.isTargetResourceEnforced()) {
                LookaheadRS<?> resource = (LookaheadRS<?>) action.getEnforcedTargetResource();
                resource.removeAction(action);
            } else {
                removeActionFromAllResources(action);
            }
        }
    }

    private void removeActionFromAllResources(AllocatableAction action) {

        Iterator<ResourceScheduler<?>> iter = getWorkers().iterator();
        if (iter.hasNext()) {
            LookaheadRS<?> resource = (LookaheadRS<?>) iter.next();
            if (!resource.removeAction(action)) {
                if (DEBUG) {
                    LOGGER.debug("[ReadyScheduler] Action already added " + action);
                }
                return;
            }

            while (iter.hasNext()) {
                resource = (LookaheadRS<?>) iter.next();
                Future<?> lastToken = resource.getToken();
                resource.setToken(schedulerExecutor.submit(new RemoveRunnable(resource, action, lastToken)));
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] No resources to addAction");
            }
        }
    }

    protected <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
        ResourceScheduler<T> rs) {
        LookaheadRS<T> resource = (LookaheadRS<T>) rs;
        if (DEBUG) {
            LOGGER.debug("[ReadyScheduler] Try to launch free actions on resource " + resource.getName());
        }

        // Actions that have been freeded by the action that just finished
        for (AllocatableAction freeAction : dataFreeActions) {
            if (freeAction.getCompatibleWorkers().isEmpty()) {
                blockedCandidates.add(freeAction);
                continue;
            }
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Introducing data free action " + freeAction + " into the scheduler.");
            }
            addActionToSchedulerStructures(freeAction);
        }

        // Resource free actions should always be empty in this scheduler

        Future<?> lastToken = resource.getToken();
        if (lastToken != null) {
            try {
                lastToken.get();
            } catch (InterruptedException | ExecutionException e) {
                LOGGER.fatal("Unexpected thread interruption", e);
                ErrorManager.fatal("Unexpected thread interruption", e);
            }
        }
        Semaphore sem = new Semaphore(0);
        resource.setToken(schedulerExecutor.submit(new WaitSchedulingRunnable(resource, sem)));

        Set<ObjectValue<AllocatableAction>> unassignedActions = ((LookaheadRS<?>) resource).getUnassignedActions();
        if (unassignedActions != null) {
            HashSet<ObjectValue<AllocatableAction>> objectValueToErase = new HashSet<>();
            Iterator<ObjectValue<AllocatableAction>> executableActionsIterator = unassignedActions.iterator();
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler]  ***** Trying to schedule " + unassignedActions.size()
                    + " unassigned actions to " + this.availableWorkers.size() + " workers");
            }

            Collection<ResourceScheduler<? extends WorkerResourceDescription>> candidateResources;
            candidateResources = this.getAvailableWorkers();
            while (executableActionsIterator.hasNext() && !candidateResources.isEmpty()) {
                ObjectValue<AllocatableAction> obj = executableActionsIterator.next();
                AllocatableAction freeAction = obj.getObject();
                if (DEBUG) {
                    LOGGER.debug("[ReadyScheduler] -- Trying to schedule " + freeAction);
                }
                try {
                    freeAction.schedule(candidateResources, generateActionScore(freeAction));
                    tryToLaunch(freeAction);

                    ResourceScheduler<? extends WorkerResourceDescription> assignedResource;
                    assignedResource = freeAction.getAssignedResource();
                    if (DEBUG) {
                        LOGGER.debug("[ReadyScheduler] -- Action " + freeAction
                            + " successfully scheduled and launched in " + assignedResource.getName());
                    }

                    objectValueToErase.add(obj);

                    if (!assignedResource.canRunSomething()) {
                        this.availableWorkers.remove(assignedResource);
                    }
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

    @Override
    public void upgradeAction(AllocatableAction action) {
        if (DEBUG) {
            LOGGER.debug(" Upgrading action " + action);
        }
        upgradedActions.add(action);
        updateActionToSchedulerStructures(action);
    }


    private class RemoveRunnable implements Runnable {

        private LookaheadRS<?> resource;
        private AllocatableAction action;
        private Future<?> token;


        public RemoveRunnable(LookaheadRS<?> scheduler, AllocatableAction action, Future<?> token) {
            this.resource = scheduler;
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
            resource.removeAction(action);
        }

    }

    private class WaitSchedulingRunnable implements Runnable {

        private ResourceScheduler<?> resource;
        private Semaphore sem;


        public WaitSchedulingRunnable(ResourceScheduler<?> resource, Semaphore sem) {
            this.resource = resource;
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

        private LookaheadRS<?> resource;
        private AllocatableAction action;
        private Future<?> token;
        private Score actionScore;


        public AddRunnable(LookaheadRS<?> scheduler, Score actionScore, AllocatableAction action, Future<?> token) {
            this.resource = scheduler;
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
            resource.addAction(action, actionScore);
        }
    }

    private class UpdateRunnable implements Runnable {

        private LookaheadRS<?> resource;
        private AllocatableAction action;
        private Future<?> token;


        public UpdateRunnable(LookaheadRS<?> resource, AllocatableAction action, Future<?> token) {
            this.resource = resource;
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
            Score actionScore = generateActionScore(action);
            resource.updateAction(action, actionScore);
        }

    }


    protected Collection<ResourceScheduler<? extends WorkerResourceDescription>> getAvailableWorkers() {
        Iterator<ResourceScheduler<?>> iterator = this.availableWorkers.iterator();
        while (iterator.hasNext()) {
            ResourceScheduler rs = iterator.next();
            if (!rs.canRunSomething()) {
                iterator.remove();
            }
        }
        return this.availableWorkers;
    }
}
