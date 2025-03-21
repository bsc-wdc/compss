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

package es.bsc.compss.scheduler.lookahead;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.ObjectValue;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.util.ActionSet;

import java.util.Collection;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;

import java.util.List;
import java.util.PriorityQueue;
import java.util.Set;

import org.json.JSONObject;


/**
 * Representation of a Scheduler that considers only ready tasks.
 */
public abstract class LookaheadTS extends TaskScheduler {

    protected final ActionSet unassignedReadyActions;
    protected Set<AllocatableAction> upgradedActions;

    private final AvailableWorkersSet availableWorkers;


    /**
     * Constructs a new Ready Scheduler instance.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public LookaheadTS(ActionOrchestrator orchestrator) {
        super(orchestrator);
        this.unassignedReadyActions = new ActionSet();
        this.upgradedActions = new HashSet<>();
        this.availableWorkers = new AvailableWorkersSet();
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
        if (resource.canRunSomething()) {
            this.availableWorkers.addWorker(resource);
        }
    }

    @Override
    protected <T extends WorkerResourceDescription> void workerDetected(ResourceScheduler<T> resource) {
        super.workerDetected(resource);
        if (resource.canRunSomething()) {
            this.availableWorkers.addWorker(resource);
        }
    }

    @Override
    protected <T extends WorkerResourceDescription> void workerRemoved(ResourceScheduler<T> resource) {
        this.availableWorkers.removeWorker(resource);
        super.workerRemoved(resource);
    }

    @Override
    public <T extends WorkerResourceDescription> void workerFeaturesUpdate(ResourceScheduler<T> worker, T modification,
        List<AllocatableAction> unblockedActions, List<AllocatableAction> blockedCandidates) {
        if (worker.canRunSomething()) {
            this.availableWorkers.addWorker(worker);
        }
        List<AllocatableAction> dataFreeActions = new LinkedList<>();
        List<AllocatableAction> resourceFreeActions = unblockedActions;
        tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, worker);
    }

    @Override
    public void customCoreElementsUpdated() {
        for (ResourceScheduler<? extends WorkerResourceDescription> rs : this.getWorkers()) {
            if (rs.canRunSomething()) {
                this.availableWorkers.addWorker(rs);
            }
        }
    }

    @Override
    public abstract Score generateActionScore(AllocatableAction action);

    @Override
    public abstract <T extends WorkerResourceDescription> LookaheadRS<T> generateSchedulerForResource(Worker<T> w,
        JSONObject defaultResources, JSONObject defaultImplementations);

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
            Collection<ResourceScheduler<? extends WorkerResourceDescription>> availableCandidates;
            availableCandidates = this.availableWorkers.getAll();
            if (!availableCandidates.isEmpty()) {
                try {
                    action.schedule(availableCandidates, actionScore);
                } catch (UnassignedActionException ex) {
                    this.unassignedReadyActions.addAction(action);
                }
            } else {
                if (action.getCompatibleWorkers().isEmpty()) {
                    throw new BlockedActionException();
                } else {
                    this.unassignedReadyActions.addAction(action);
                }
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
    public void upgradeAction(AllocatableAction action) {
        if (DEBUG) {
            LOGGER.debug(" Upgrading action " + action);
        }
        upgradedActions.add(action);
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
        tryToLaunchFreeActions(dataFreeActions, resourceFreeActions, blockedCandidates, resource);
    }

    /**
     * Collects all the potential candidates to run after a task end.
     *
     * @param dataFreeActions actions whose data dependencies have been released by the ended action (IN)
     * @param resourceFreeActions actions whose resource dependencies have been released by the ended action (IN)
     * @param unassignedActions dependency-free actions that the scheduler has not assigned yet (INOUT)
     * @param blockedCandidates list of candidates whose submission has failed because there were no compatible
     *            resources
     * @param resource resource that hosted the action execution
     * @return list of actions to try to schedule
     */
    protected <T extends WorkerResourceDescription> PriorityQueue<ObjectValue<AllocatableAction>>
        getCandidateFreeActions(List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions,
            ActionSet unassignedActions, List<AllocatableAction> blockedCandidates, ResourceScheduler<T> resource) {

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

        for (AllocatableAction freeAction : unassignedActions.removeAllActions()) {
            Score actionScore = generateActionScore(freeAction);
            Score fullScore = freeAction.schedulingScore(resource, actionScore);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(freeAction, fullScore);
            if (!executableActions.contains(obj)) {
                executableActions.add(obj);
            }
        }
        return executableActions;
    }

    private PriorityQueue<ObjectValue<AllocatableAction>> sortActions(Iterable<AllocatableAction> actions) {

        if (DEBUG) {
            LOGGER.debug("[ReadyScheduler] Managing " + upgradedActions.size() + " upgraded actions.");
        }
        PriorityQueue<ObjectValue<AllocatableAction>> sortedActions = new PriorityQueue<>();
        for (AllocatableAction action : actions) {
            Score fullScore = this.generateActionScore(action);
            ObjectValue<AllocatableAction> obj = new ObjectValue<>(action, fullScore);
            sortedActions.add(obj);
        }

        return sortedActions;
    }

    private void manageUpgradedActions() {
        if (!upgradedActions.isEmpty()) {
            PriorityQueue<ObjectValue<AllocatableAction>> executableActions = sortActions(upgradedActions);

            while (!executableActions.isEmpty()) {
                ObjectValue<AllocatableAction> obj = executableActions.poll();
                AllocatableAction freeAction = obj.getObject();
                try {
                    tryToScheduleAndLaunch(freeAction, obj.getScore(), freeAction.getCompatibleWorkers());
                    upgradedActions.remove(freeAction);
                    this.unassignedReadyActions.removeAction(freeAction);
                } catch (UnassignedActionException e) {
                    // Nothing to do it could be scheduled in another resource
                } catch (BlockedActionException bae) {
                    this.addToBlocked(freeAction);
                }
            }
        }
    }

    private void manageUpgradedActions(ResourceScheduler<?> resource) {
        if (!upgradedActions.isEmpty()) {
            if (DEBUG) {
                LOGGER.debug("[ReadyScheduler] Managing " + upgradedActions.size() + " upgraded actions.");
            }

            PriorityQueue<ObjectValue<AllocatableAction>> executableActions = sortActions(upgradedActions);

            while (!executableActions.isEmpty()) {
                ObjectValue<AllocatableAction> obj = executableActions.poll();
                AllocatableAction freeAction = obj.getObject();
                if (freeAction.getCompatibleWorkers().contains(resource) && resource.canRunSomething()) {
                    try {
                        tryToScheduleAndLaunch(freeAction, obj.getScore(), resource);
                        upgradedActions.remove(freeAction);
                        this.unassignedReadyActions.removeAction(freeAction);
                    } catch (UnassignedActionException e) {
                        // Nothing to do it could be scheduled in another resource
                    } catch (BlockedActionException bae) {
                        this.addToBlocked(freeAction);
                    }
                }
            }
        }
    }

    private <T extends WorkerResourceDescription> void tryToLaunchFreeActions(List<AllocatableAction> dataFreeActions,
        List<AllocatableAction> resourceFreeActions, List<AllocatableAction> blockedCandidates,
        ResourceScheduler<T> resource) {

        PriorityQueue<ObjectValue<AllocatableAction>> executableActions;

        manageUpgradedActions(resource);

        executableActions = getCandidateFreeActions(dataFreeActions, resourceFreeActions, this.unassignedReadyActions,
            blockedCandidates, resource);

        ActionHandler ah = new AH(blockedCandidates);
        ResourcePool ra = new RA();
        tryToScheduleActions(executableActions, ra, ah);

    }


    protected static interface ResourcePool {

        public boolean checkAvailability();

        public Collection<ResourceScheduler<? extends WorkerResourceDescription>> getAll();

    }

    private class RA implements ResourcePool {

        @Override
        public boolean checkAvailability() {
            return !availableWorkers.isEmpty();
        }

        @Override
        public Collection<ResourceScheduler<? extends WorkerResourceDescription>> getAll() {
            return availableWorkers.getAll();
        }
    }

    protected static interface ActionHandler {

        public void unassigned(AllocatableAction action);

        public void blocked(AllocatableAction action);
    }

    private class AH implements ActionHandler {

        private final List<AllocatableAction> blocked;


        public AH(List<AllocatableAction> blocked) {
            this.blocked = blocked;
        }

        @Override
        public void unassigned(AllocatableAction action) {
            unassignedReadyActions.addAction(action);
        }

        @Override
        public void blocked(AllocatableAction action) {
            this.blocked.add(action);
        }
    }


    protected void tryToScheduleActions(PriorityQueue<ObjectValue<AllocatableAction>> executableActions,
        ResourcePool rp, ActionHandler handler) {

        Set<AllocatableAction> oldUpgradedActions = this.upgradedActions;
        while (!executableActions.isEmpty() && rp.checkAvailability()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();
            Score actionScore = obj.getScore();

            this.upgradedActions = new HashSet<>();
            LOGGER.debug("[ReadyScheduler] Trying to launch action " + freeAction);
            try {
                tryToScheduleAndLaunch(freeAction, actionScore, rp.getAll());
                if (!upgradedActions.isEmpty()) {
                    manageUpgradedActions();
                    oldUpgradedActions.addAll(upgradedActions);
                }
            } catch (UnassignedActionException e) {
                handler.unassigned(freeAction);
            } catch (BlockedActionException e) {
                handler.blocked(freeAction);
            }
        }

        while (!executableActions.isEmpty()) {
            ObjectValue<AllocatableAction> obj = executableActions.poll();
            AllocatableAction freeAction = obj.getObject();
            handler.unassigned(freeAction);
        }
        this.upgradedActions = oldUpgradedActions;

    }

    private void tryToScheduleAndLaunch(AllocatableAction action, Score actionScore, ResourceScheduler<?> resource)
        throws UnassignedActionException, BlockedActionException {
        if (action.getCompatibleWorkers().isEmpty()) {
            throw new BlockedActionException();
        }
        action.schedule(resource, actionScore);
        tryToLaunch(action);

        ResourceScheduler<? extends WorkerResourceDescription> assignedResource;
        assignedResource = action.getAssignedResource();

        if (!assignedResource.canRunSomething()) {
            this.availableWorkers.removeWorker(assignedResource);
        }
    }

    private void tryToScheduleAndLaunch(AllocatableAction action, Score actionScore,
        Collection<ResourceScheduler<?>> resources) throws UnassignedActionException, BlockedActionException {
        if (action.getCompatibleWorkers().isEmpty()) {
            throw new BlockedActionException();
        }
        action.schedule(resources, actionScore);
        tryToLaunch(action);

        ResourceScheduler<? extends WorkerResourceDescription> assignedResource;
        assignedResource = action.getAssignedResource();

        if (!assignedResource.canRunSomething()) {
            this.availableWorkers.removeWorker(assignedResource);
        }
    }


    private static class AvailableWorkersSet {

        // Management of available Workers
        private final HashSet<ResourceScheduler<? extends WorkerResourceDescription>> availableWorkers;


        public AvailableWorkersSet() {
            this.availableWorkers = new HashSet<>();
        }

        public Collection<ResourceScheduler<? extends WorkerResourceDescription>> getAll() {
            Iterator<ResourceScheduler<?>> iterator = this.availableWorkers.iterator();
            while (iterator.hasNext()) {
                ResourceScheduler rs = iterator.next();
                if (!rs.canRunSomething()) {
                    iterator.remove();
                }
            }
            return this.availableWorkers;
        }

        public <T extends WorkerResourceDescription> void addWorker(ResourceScheduler<T> resource) {
            if (resource.canRunSomething()) {
                this.availableWorkers.add(resource);
            }
        }

        private <T extends WorkerResourceDescription> void removeWorker(ResourceScheduler<T> resource) {
            this.availableWorkers.remove(resource);
        }

        private boolean isEmpty() {
            return this.availableWorkers.isEmpty();
        }
    }
}
