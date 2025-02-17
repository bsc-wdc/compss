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
package es.bsc.compss.types.request.td;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.SchedulingInformation;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.CoreElement;
import es.bsc.compss.types.ReduceTask;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.allocatableactions.MultiNodeExecutionAction;
import es.bsc.compss.types.allocatableactions.MultiNodeGroup;
import es.bsc.compss.types.allocatableactions.ReduceExecutionAction;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.ResourceManager;

import java.util.Collection;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * The ExecuteTasksRequest class represents the request to execute a task.
 */
public class ExecuteTasksRequest extends TDRequest {

    private static final Logger TIMER_LOGGER = LogManager.getLogger(Loggers.TIMER);
    private static final boolean IS_TIMER_COMPSS_ENABLED;

    static {
        // Load timer property
        String isTimerCOMPSsEnabledProperty = System.getProperty(COMPSsConstants.TIMER_COMPSS_NAME);
        IS_TIMER_COMPSS_ENABLED = (isTimerCOMPSsEnabledProperty == null || isTimerCOMPSsEnabledProperty.isEmpty()
            || isTimerCOMPSsEnabledProperty.equals("null")) ? false : Boolean.valueOf(isTimerCOMPSsEnabledProperty);
    }

    private final AccessProcessor ap;
    private final Task task;


    /**
     * Constructs a new ScheduleTasks Request.
     *
     * @param ap Access Processor to be notified when the task ends.
     * @param t Task to run.
     */
    public ExecuteTasksRequest(AccessProcessor ap, Task t) {
        this.ap = ap;
        this.task = t;
    }

    /**
     * Returns the task to execute.
     *
     * @return task to execute.
     */
    public AbstractTask getTask() {
        return this.task;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        if (IS_TIMER_COMPSS_ENABLED) {
            long startTime = System.nanoTime();
            processTask(ts);
            long endTime = System.nanoTime();
            float elapsedTime = (endTime - startTime) / (float) 1_000_000;
            TIMER_LOGGER.info("[TIMER] TD Schedule of task " + this.task.getId() + ": " + elapsedTime + " ms");
        } else {
            processTask(ts);
        }
    }

    private void processTask(TaskScheduler ts) throws ShutdownException {
        int coreId = this.task.getTaskDescription().getCoreElement().getCoreId();
        if (DEBUG) {
            LOGGER.debug("Treating Scheduling request for task " + this.task.getId() + "(core " + coreId + ")");
        }

        this.task.setStatus(TaskState.TO_EXECUTE);
        int numNodes = this.task.getTaskDescription().getNumNodes();
        boolean isReplicated = this.task.getTaskDescription().isReplicated();
        boolean isDistributed = this.task.getTaskDescription().isDistributed();

        if (isReplicated) {
            // Method annotation forces to replicate task to all nodes
            if (DEBUG) {
                LOGGER.debug("Replicating task " + this.task.getId());
            }

            Collection<ResourceScheduler<? extends WorkerResourceDescription>> resources = ts.getWorkers();
            this.task.setExecutionCount(resources.size() * numNodes);
            for (ResourceScheduler<? extends WorkerResourceDescription> rs : resources) {
                submitTask(ts, numNodes, rs);
            }
        } else if (isDistributed) {
            // Method annotation forces RoundRobin among nodes
            if (DEBUG) {
                LOGGER.debug("Distributing task " + this.task.getId());
            }

            ResourceScheduler<? extends WorkerResourceDescription> selectedResource =
                ts.getNextResourForDistributed(this.task.getTaskDescription().getCoreElement().getCoreId());
            this.task.setExecutionCount(numNodes);
            submitTask(ts, numNodes, selectedResource);
        } else {
            // Normal task
            if (DEBUG) {
                LOGGER.debug("Submitting task " + this.task.getId());
            }

            this.task.setExecutionCount(numNodes);
            submitTask(ts, numNodes, null);
        }

        if (DEBUG) {
            LOGGER.debug("Treated Scheduling request for task " + this.task.getId() + " (core " + coreId + ")");
        }
    }

    private <T extends WorkerResourceDescription> void submitTask(TaskScheduler ts, int numNodes,
        ResourceScheduler<T> specificResource) {

        // A task can use one or more resources
        if (numNodes == 1) {
            submitSingleTask(ts, specificResource);
        } else {
            submitMultiNodeTask(ts, numNodes, specificResource);
        }
    }

    private <T extends WorkerResourceDescription> void submitSingleTask(TaskScheduler ts,
        ResourceScheduler<T> specificResource) {
        ExecutionAction action;
        ActionOrchestrator orch = ts.getOrchestrator();
        if (this.task.isReduction()) {
            LOGGER.debug("Scheduling request for reduce task " + this.task.getId() + " treated as singleTask");
            SchedulingInformation sInfo = new SchedulingInformation();
            // No need for a specific scheduling information
            action = new ReduceExecutionAction(sInfo, orch, this.ap, (ReduceTask) this.task, ts);
        } else {
            LOGGER.debug("Scheduling request for task " + this.task.getId() + " treated as singleTask");
            int coreId = this.task.getTaskDescription().getCoreElement().getCoreId();
            SchedulingInformation sInfo =
                ts.generateSchedulingInformation(specificResource, this.task.getParameters(), coreId);
            action = new ExecutionAction(sInfo, orch, this.ap, this.task);
        }
        ts.newAllocatableAction(action);
    }

    private <T extends WorkerResourceDescription> void submitMultiNodeTask(TaskScheduler ts, int numNodes,
        ResourceScheduler<T> specificResource) {
        boolean toBlocked = false;
        LOGGER.debug("Scheduling request for task " + this.task.getId() + " treated as multiNodeTask with " + numNodes
            + " nodes");
        if (exceedsMaxResources(numNodes, this.task.getTaskDescription().getCoreElement())) {
            ErrorManager.warn("Task " + this.task.getId() + " can't be executed because exceeds the maximum number "
                + "of available cores. Adding actions to blocked.");
            toBlocked = true;
        }
        // Can use one or more resources depending on the computingNodes
        MultiNodeGroup group = new MultiNodeGroup(numNodes);
        for (int i = 0; i < numNodes; ++i) {
            MultiNodeExecutionAction action = new MultiNodeExecutionAction(
                ts.generateSchedulingInformation(specificResource, this.task.getTaskDescription().getParameters(),
                    this.task.getTaskDescription().getCoreElement().getCoreId()),
                ts.getOrchestrator(), this.ap, this.task, group);
            group.addAction(action);
            if (toBlocked) {
                ts.addToBlocked(action);
            } else {
                ts.newAllocatableAction(action);
            }
        }
    }

    private boolean exceedsMaxResources(int numNodes, CoreElement coreElement) {
        return numNodes > ResourceManager.getTotalSlots()[coreElement.getCoreId()];
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.EXECUTE_TASKS;
    }

}
