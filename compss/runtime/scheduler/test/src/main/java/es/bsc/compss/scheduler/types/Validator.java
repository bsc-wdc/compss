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
package es.bsc.compss.scheduler.types;

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.fake.FakeAllocatableAction;
import es.bsc.compss.scheduler.types.fake.FakeResourceDescription;
import es.bsc.compss.scheduler.types.fake.FakeWorker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import org.junit.Assert;


public abstract class Validator {

    public static enum ActionState {
        REGISTERED, BLOCKED, READY, RUNNING, FINISHED
    }


    protected final TaskScheduler ts;
    private final HashMap<FakeAllocatableAction, ActionState> actions;
    protected final HashMap<String, FakeWorker> resources;
    private final LinkedList<FakeAllocatableAction> tsBlocked;
    private final List<FakeAllocatableAction> ready;
    private final HashMap<FakeAllocatableAction, List<FakeAllocatableAction>> actionSuccessors;
    private final HashMap<FakeAllocatableAction, int[]> dependencyCount;


    /**
     * Constructs a new Validator for a given Scheduler.
     *
     * @param ts Task Scheduler being validated
     */
    public Validator(TaskScheduler ts) {
        this.ts = ts;
        actions = new HashMap<>();
        resources = new HashMap<>();
        actionSuccessors = new HashMap<>();
        dependencyCount = new HashMap<>();
        tsBlocked = new LinkedList<>();
        ready = new LinkedList<>();
    }

    /**
     * A new Resource is added as a worker to the TS.
     *
     * @param name name of the worker
     * @param description capabilities of the worker
     */
    public final void addResource(String name, FakeResourceDescription description) {
        FakeWorker worker = new FakeWorker(name, description, 1000);
        resources.put(name, worker);
        Iterator<FakeAllocatableAction> it = tsBlocked.iterator();
        List<FakeAllocatableAction> unblockedActions = new LinkedList<>();
        while (it.hasNext()) {
            FakeAllocatableAction action = it.next();
            FakeResourceDescription requirements =
                (FakeResourceDescription) action.getImplementations()[0].getDescription().getConstraints();
            if (description.getCoreCount() >= requirements.getCoreCount()) {
                it.remove();
                unblockedActions.add(action);
                ready.add(action);
                actions.put(action, ActionState.READY);
            }
        }
        workerAdded(worker, unblockedActions);
    }

    /**
     * Checks to be performed on a resource addition.
     *
     * @param worker worker added to the system
     * @param unblockedActions list of actions that were unblocked due to adding the resource
     */
    public abstract void workerAdded(FakeWorker worker, List<FakeAllocatableAction> unblockedActions);

    /**
     * Removes a worker from the validation system.
     *
     * @param name Name of the removed worker
     */
    public final void removeResource(String name) {
        FakeWorker worker = resources.remove(name);
        Iterator<FakeAllocatableAction> it = ready.iterator();
        List<FakeAllocatableAction> blockedActions = new LinkedList<>();
        while (it.hasNext()) {
            FakeAllocatableAction action = it.next();
            FakeResourceDescription requirements =
                (FakeResourceDescription) action.getImplementations()[0].getDescription().getConstraints();
            boolean hasCompatibles = false;
            for (Map.Entry<String, FakeWorker> entry : resources.entrySet()) {
                FakeWorker w = entry.getValue();
                FakeResourceDescription description = w.getDescription();
                if (description.getCoreCount() >= requirements.getCoreCount()) {
                    hasCompatibles = true;
                }
            }
            if (!hasCompatibles) {
                it.remove();
                tsBlocked.add(action);
                actions.put(action, ActionState.BLOCKED);
            }
        }
        workerRemoved(worker, blockedActions);
    }

    /**
     * Checks to be performed on a resource removal.
     *
     * @param worker worker removed from the TS
     * @param blockedActions list of actions that were unblocked due to adding the resource
     */
    public abstract void workerRemoved(FakeWorker worker, List<FakeAllocatableAction> blockedActions);

    /**
     * Adds a new action to the Task Scheduler.
     *
     * @param action Action arriving to the TS.
     */
    public final void registerAction(FakeAllocatableAction action) {
        List<AllocatableAction> dataPredecessors = action.getDataPredecessors();
        List<FakeAllocatableAction> actionSuccessors = new LinkedList<>();
        this.actionSuccessors.put(action, actionSuccessors);
        dependencyCount.put(action, new int[] { dataPredecessors.size() });
        ActionState state;
        if (!action.hasDataPredecessors()) {
            FakeResourceDescription requirements =
                (FakeResourceDescription) action.getImplementations()[0].getDescription().getConstraints();
            boolean hasCompatibles = false;
            for (Map.Entry<String, FakeWorker> entry : resources.entrySet()) {
                FakeWorker worker = entry.getValue();
                FakeResourceDescription description = worker.getDescription();
                if (description.getCoreCount() >= requirements.getCoreCount()) {
                    hasCompatibles = true;
                }
            }
            if (hasCompatibles) {
                ready.add(action);
                state = ActionState.READY;
            } else {
                tsBlocked.add(action);
                state = ActionState.BLOCKED;
            }
        } else {
            // Register dependencies for each predecessor
            for (AllocatableAction a : dataPredecessors) {
                List<FakeAllocatableAction> aSuccessors = this.actionSuccessors.get((FakeAllocatableAction) a);
                this.actionSuccessors.put((FakeAllocatableAction) a, aSuccessors);
                aSuccessors.add(action);
            }
            state = ActionState.REGISTERED;
        }

        actions.put(action, state);
        actionRegistered(action, state);
    }

    /**
     * Validates the proper registering of a new Action arriving to the TS.
     *
     * @param action new action arriving to the TS
     * @param state state of the action registered
     */
    public abstract void actionRegistered(FakeAllocatableAction action, ActionState state);

    /**
     * Registers that an action is being submitted to the TS.
     *
     * @param action action being submitted
     */
    public void submittedAction(FakeAllocatableAction action) {

        boolean isBlocked = ts.getBlockedActions().contains(action);
        if (action.hasDataPredecessors()) {
            Assert.assertFalse(action + " is blocked and has pending data dependencies", isBlocked);
        } else {
            FakeResourceDescription requirements;
            requirements = (FakeResourceDescription) action.getImplementations()[0].getDescription().getConstraints();
            boolean hasCompatibles = false;
            for (Map.Entry<String, FakeWorker> entry : resources.entrySet()) {
                FakeWorker worker = entry.getValue();
                FakeResourceDescription description = worker.getDescription();
                if (description.getCoreCount() >= requirements.getCoreCount()) {
                    hasCompatibles = true;
                }
            }
            if (hasCompatibles) {
                Assert.assertFalse(action + " is blocked and has compatible workers", isBlocked);
            } else {
                Assert.assertTrue(action + " is not blocked and has  no compatible workers", isBlocked);
            }
        }
        actionSubmitted(action);
    }

    /**
     * Validates the proper submission of an action to the TS.
     *
     * @param action new action submitted to the TS
     */
    public abstract void actionSubmitted(FakeAllocatableAction action);

    /**
     * Registers that an action started its execution.
     *
     * @param action action starting the execution
     */
    public final void executingAction(FakeAllocatableAction action) {
        ready.remove(action);
        ResourceScheduler<? extends WorkerResourceDescription> rs = action.getAssignedResource();
        String resourceName = rs.getName();
        FakeWorker worker = resources.get(resourceName);
        FakeResourceDescription available = worker.getAvailable();

        // Check if there are enough resources to host the action
        FakeResourceDescription consumption = (FakeResourceDescription) action.getResourceConsumption();
        Assert.assertTrue(resourceName + " has not enough resources to host " + action,
            available.getCoreCount() >= consumption.getCoreCount());
        available.reduceDynamic(consumption);

        actions.put(action, ActionState.RUNNING);

        actionExecuting(action, new LinkedList<>());
    }

    /**
     * Validates the proper start of execution of an action.
     *
     * @param action action starting the execution in a worker
     * @param depFreeActions list of actions that became free of dependencies after starting the action
     */
    public abstract void actionExecuting(FakeAllocatableAction action, List<FakeAllocatableAction> depFreeActions);

    /**
     * Registers the end of an action execution.
     *
     * @param action action being finished
     */
    public final void finishedAction(FakeAllocatableAction action) {
        Assert.assertEquals("Completed action " + action + " without being executed", ActionState.RUNNING,
            actions.get(action));
        actions.put(action, ActionState.FINISHED);

        // Release resources from the worker
        ResourceScheduler<? extends WorkerResourceDescription> rs = action.getAssignedResource();
        String resourceName = rs.getName();
        FakeWorker worker = resources.get(resourceName);
        FakeResourceDescription available = worker.getAvailable();
        FakeResourceDescription consumption = (FakeResourceDescription) action.getResourceConsumption();
        available.increaseDynamic(consumption);

        List<FakeAllocatableAction> depFreeActions = new LinkedList<>();
        // Free data dependencies and put them in ready if no dependencies
        List<FakeAllocatableAction> aSuccessors = this.actionSuccessors.get(action);
        for (FakeAllocatableAction successor : aSuccessors) {
            int[] predecessorsCount = dependencyCount.get(successor);
            predecessorsCount[0]--;
            if (predecessorsCount[0] == 0) {
                ready.add(successor);
                depFreeActions.add(successor);
                actions.put(successor, ActionState.READY);
            }
        }
        actionFinished(action, depFreeActions);
    }

    /**
     * Validates the proper ending of an action.
     *
     * @param action action completed
     * @param depFreeActions list of actions that became free of dependencies after starting the action
     */
    public abstract void actionFinished(FakeAllocatableAction action, List<FakeAllocatableAction> depFreeActions);
}
