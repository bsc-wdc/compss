/*
 *  Copyright 2002-2024 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.predefined;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.BlockedActionException;
import es.bsc.compss.scheduler.exceptions.UnassignedActionException;
import es.bsc.compss.scheduler.predefined.types.PredefinedData;
import es.bsc.compss.scheduler.types.ActionOrchestrator;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.allocatableactions.MultiNodeExecutionAction;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.parameter.Parameter;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;


public class PredefinedTS extends TaskScheduler {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean IS_DEBUG = LOGGER.isDebugEnabled();
    protected static final String LOG_PREFIX = "[PredefinedTS] ";

    private final PredefinedData predefinedData;

    private final Map<String, ResourceScheduler<? extends WorkerResourceDescription>> resourceCache = new HashMap<>();

    // Track which resource index to use for each MultiNode group
    private final Map<Integer, Integer> multiNodeGroupResourceIndex = new HashMap<>();


    /**
     * Constructs a new Ready Scheduler instance.
     *
     * @param orchestrator element ordering the execution of actions
     */
    public PredefinedTS(ActionOrchestrator orchestrator) {
        super(orchestrator);
        String dataPath = System.getProperty(COMPSsConstants.SCHEDULER_CONFIG_FILE);
        try {
            this.predefinedData = new PredefinedData(dataPath);
            LOGGER.debug(LOG_PREFIX + "Loaded predefined data from: " + dataPath);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load predefined CSV: " + dataPath, e);
        }
    }

    /*
     * @Override protected <T extends WorkerResourceDescription> void handleDependencyFreeActions(
     * List<AllocatableAction> dataFreeActions, List<AllocatableAction> resourceFreeActions, List<AllocatableAction>
     * blockedCandidates, ResourceScheduler<T> resource) { if (IS_DEBUG) { LOGGER.debug(LOG_PREFIX +
     * "Skipping base dependency-free scan"); } }
     */

    @Override
    public <T extends WorkerResourceDescription> ResourceScheduler<T> generateSchedulerForResource(Worker<T> w,
        JSONObject defaultResources, JSONObject defaultImplementations) {
        ResourceScheduler<T> rs = new PredefinedRS<>(w, defaultResources, defaultImplementations, predefinedData);

        // Cache al crear ResourceSchedulers
        resourceCache.put(w.getName(), rs);

        return rs;
    }

    @Override
    public <T extends WorkerResourceDescription> PredefinedSchedulingInformation
        generateSchedulingInformation(ResourceScheduler<T> rs, List<? extends Parameter> params, Integer coreId) {
        return new PredefinedSchedulingInformation();
    }

    @Override
    protected void scheduleAction(AllocatableAction action, Score actionScore) throws BlockedActionException {

        // Early return if action is not ExecutionAction
        if (!(action instanceof ExecutionAction)) {
            super.scheduleAction(action, actionScore);
            return;
        }

        ExecutionAction se = (ExecutionAction) action;
        int taskId = se.getTask().getId();

        // Handle MultiNodeExecutionAction
        if (action instanceof MultiNodeExecutionAction) {
            MultiNodeExecutionAction multiNodeAction = (MultiNodeExecutionAction) action;

            // Check if we have predefined resources for this multi-node task
            if (predefinedData.containsAction(taskId) && predefinedData.isMultiNodeAction(taskId)) {
                if (IS_DEBUG) {
                    LOGGER.debug(LOG_PREFIX + "Scheduling MultiNodeExecutionAction task " + taskId
                        + " with predefined resources");
                }
                scheduleMultiNode(multiNodeAction, taskId, actionScore);
                return;
            } else {
                // No predefined resources, delegate to parent
                if (IS_DEBUG) {
                    LOGGER.debug(
                        LOG_PREFIX + "Delegating MultiNodeExecutionAction task " + taskId + " to default scheduler");
                }
                super.scheduleAction(action, actionScore);
                return;
            }
        }

        // Early return if task is not in predefinedData
        if (!predefinedData.containsAction(taskId)) {
            if (IS_DEBUG) {
                LOGGER.debug(LOG_PREFIX + "Task " + taskId + " not found in predefined data, using default scheduling");
            }
            super.scheduleAction(action, actionScore);
            return;
        }

        // Check and set predecessors
        int pendingPredecessors = 0;
        List<Integer> preds = predefinedData.getPredecessors(taskId);
        if (!preds.isEmpty()) {
            for (int pred : preds) {
                if (!predefinedData.isFinished(pred)) {
                    pendingPredecessors++;
                }
            }
        }

        PredefinedSchedulingInformation rInfo = (PredefinedSchedulingInformation) action.getSchedulingInfo();
        rInfo.setPendingPredecessors(pendingPredecessors);

        // set worker and implementation
        String resName = predefinedData.getResourceForAction(taskId);

        ResourceScheduler<? extends WorkerResourceDescription> rs = resourceCache.get(resName);
        if (rs == null) {
            rs = findResourceScheduler(resName);
            if (rs != null) {
                resourceCache.put(resName, rs);
            }
        }

        if (rs == null) {
            LOGGER.warn(LOG_PREFIX + "Resource " + resName + " not found for task " + taskId);
            lostAllocatableAction(action);
            return;
        }

        @SuppressWarnings("unchecked")
        ResourceScheduler<WorkerResourceDescription> target = (ResourceScheduler<WorkerResourceDescription>) rs;

        try {
            Integer implId = predefinedData.getImplementationId(taskId);
            if (implId != null && implId != -1) {
                // Enforce implementation
                Implementation[] impls = se.getImplementations();
                if (implId >= 0 && implId < impls.length) {
                    Implementation impl = impls[implId];
                    action.schedule(target, impl);
                    if (IS_DEBUG) {
                        LOGGER.debug(LOG_PREFIX + "Task " + taskId + " scheduled on " + resName
                            + " with implementation " + implId);
                    }
                } else {
                    LOGGER.warn(LOG_PREFIX + "Invalid implementation ID " + implId + " for task " + taskId
                        + ". Falling back to score-based scheduling.");
                    action.schedule(target, actionScore);
                }
            } else {
                // Default score-based scheduling
                action.schedule(target, actionScore);
            }

        } catch (UnassignedActionException uae) {
            LOGGER.warn(LOG_PREFIX + "UnassignedActionException for task " + taskId + ", marking lost");
            lostAllocatableAction(action);
            return;
        }

        // Afegir a waiting si te predecesores pendents
        if (pendingPredecessors > 0) {
            predefinedData.getWaitingActions().put(taskId, action);
            if (IS_DEBUG) {
                LOGGER.debug(LOG_PREFIX + "Task " + taskId + " waiting for " + pendingPredecessors + " predecessors");
            }
        } else {
            if (IS_DEBUG) {
                LOGGER.debug(LOG_PREFIX + "Task " + taskId + " ready to execute");
            }
        }
    }

    private void scheduleMultiNode(MultiNodeExecutionAction action, int taskId, Score actionScore)
        throws BlockedActionException {

        // 1) Get resources and check if there are enough for the index in the multinode
        // actions group
        List<String> resources = predefinedData.getResourcesForMultiNodeAction(taskId);
        if (resources.isEmpty()) {
            LOGGER.warn(LOG_PREFIX + "No resources defined for multi-node task " + taskId + ". Delegating to default.");
            super.scheduleAction(action, actionScore);
            return;
        }
        int index = multiNodeGroupResourceIndex.getOrDefault(taskId, 0);
        if (index >= resources.size()) {
            LOGGER.warn(LOG_PREFIX + "More actions than resources for task " + taskId + ". Wrapping around.");
            index = index % resources.size();
        }

        // 2) Check and set predecessors
        int pendingPredecessors = 0;
        List<Integer> preds = predefinedData.getPredecessors(taskId);
        if (!preds.isEmpty()) {
            for (int pred : preds) {
                if (!predefinedData.isFinished(pred)) {
                    pendingPredecessors++;
                }
            }
        }

        PredefinedSchedulingInformation rInfo = (PredefinedSchedulingInformation) action.getSchedulingInfo();
        rInfo.setPendingPredecessors(pendingPredecessors);

        // 3) Set resource and implementation and sent to schedule
        String resName = resources.get(index);

        // Update index for the next action of this group
        multiNodeGroupResourceIndex.put(taskId, index + 1);

        ResourceScheduler<? extends WorkerResourceDescription> rs = resourceCache.get(resName);
        if (rs == null) {
            rs = findResourceScheduler(resName);
            if (rs != null) {
                resourceCache.put(resName, rs);
            }
        }

        if (rs == null) {
            LOGGER.warn(LOG_PREFIX + "Resource " + resName + " not found for task " + taskId);
            lostAllocatableAction(action);
            return;
        }

        ResourceScheduler<WorkerResourceDescription> target = (ResourceScheduler<WorkerResourceDescription>) rs;

        try {
            int implId = predefinedData.getImplementationId(taskId);
            if (implId == -1) {
                implId = 0; // Default to 0 if not specified
            }
            Implementation impl = action.getImplementations()[implId];

            action.schedule(target, impl);
            if (IS_DEBUG) {
                LOGGER.debug(LOG_PREFIX + "MultiNode Action (Task " + taskId + ") scheduled on " + resName + " (Index "
                    + index + "/" + resources.size() + ")");
            }
        } catch (UnassignedActionException uae) {
            LOGGER.warn(LOG_PREFIX + "UnassignedActionException for task " + taskId + ", marking lost");
            lostAllocatableAction(action);
            return;
        }

        // 4) Add to waiting if pending predecesors
        if (pendingPredecessors > 0) {
            predefinedData.getWaitingMultiNodeActions().computeIfAbsent(taskId, k -> new ArrayList<>()).add(action);
            if (IS_DEBUG) {
                LOGGER.debug(LOG_PREFIX + "Task " + taskId + " waiting for " + pendingPredecessors + " predecessors");
            }
        } else {
            if (IS_DEBUG) {
                LOGGER.debug(LOG_PREFIX + "Task " + taskId + " ready to execute");
            }
        }

    }

    private ResourceScheduler<? extends WorkerResourceDescription> findResourceScheduler(String name) {
        return getWorkers().stream().filter(rs -> name.equals(rs.getName())).findFirst().orElse(null);
    }

    @Override
    public void upgradeAction(AllocatableAction action) {
        if (IS_DEBUG) {
            LOGGER.debug(LOG_PREFIX + "Upgrading action " + action + " (MultiNode group priority)");
        }
        // For PredefinedScheduler, we don't need special handling for upgraded actions
        // MultiNode actions are delegated to parent scheduler which handles them
        // correctly
        // The parent scheduler will manage the upgrade internally
    }
}
