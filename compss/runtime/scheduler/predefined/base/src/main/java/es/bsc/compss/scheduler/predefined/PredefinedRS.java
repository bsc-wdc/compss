/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.components.impl.ResourceScheduler;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.exceptions.ActionNotFoundException;
import es.bsc.compss.scheduler.exceptions.InvalidSchedulingException;
import es.bsc.compss.scheduler.predefined.types.PredefinedData;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.scheduler.types.Score;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.allocatableactions.ExecutionAction;
import es.bsc.compss.types.implementations.Implementation;
import es.bsc.compss.types.resources.Worker;
import es.bsc.compss.types.resources.WorkerResourceDescription;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONObject;

public class PredefinedRS<T extends WorkerResourceDescription> extends ResourceScheduler<T> {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    protected static final boolean IS_DEBUG = LOGGER.isDebugEnabled();
    protected static final String LOG_PREFIX = "[PredefinedRS] ";

    private final PredefinedData predefinedData;


    /**
     * New Ready Resource Scheduler instance.
     *
     * @param w Associated worker.
     * @param defaultResources Worker JSON description.
     * @param defaultImplementations Implementation JSON description.
     * @param predefinedData Data for the PredefinedScheduling
     */
    public PredefinedRS(Worker<T> w, JSONObject defaultResources, JSONObject defaultImplementations,
        PredefinedData predefinedData) {
        super(w, defaultResources, defaultImplementations);
        this.predefinedData = predefinedData;
    }

    /**
     * Queue initialization.
     * 
     * @return
     */
    @Override
    public Queue<AllocatableAction> constructBlockedQueue() {
        return new LinkedList<AllocatableAction>();
    }

    @Override
    public List<AllocatableAction> unscheduleAction(AllocatableAction action) throws ActionNotFoundException {
        // Early return si no son execution
        if (!(action instanceof ExecutionAction)) {
            return super.unscheduleAction(action);
        }

        ExecutionAction se = (ExecutionAction) action;
        int taskId = se.getTask().getId();

        // Marcar com finished
        predefinedData.setFinished(taskId);

        // Si no hi han successors retorn
        List<Integer> successors = predefinedData.getSuccessors(taskId);
        if (successors.isEmpty()) {
            return super.unscheduleAction(action);
        }

        Map<Integer, AllocatableAction> waitingActions = predefinedData.getWaitingActions();
        List<AllocatableAction> readyToLaunch = new ArrayList<>();

        // Analitzar els succesors
        for (int successorId : successors) {
            AllocatableAction succ = waitingActions.get(successorId);
            if (succ != null) {
                PredefinedSchedulingInformation rInfo = (PredefinedSchedulingInformation) succ.getSchedulingInfo();
                int remainingPreds = rInfo.decrementAndGet();

                if (IS_DEBUG) {
                    LOGGER.debug(
                        LOG_PREFIX + "Task " + successorId + " has " + remainingPreds + " remaining predecessors");
                }

                if (remainingPreds == 0) {
                    readyToLaunch.add(succ);
                    waitingActions.remove(successorId);
                }
            }
            List<AllocatableAction> multiNodeSucc = predefinedData.getWaitingMultiNodeActions().get(successorId);
            if (multiNodeSucc != null) {
                boolean allReady = true;
                for (AllocatableAction mnAction : multiNodeSucc) {
                    PredefinedSchedulingInformation rInfo =
                        (PredefinedSchedulingInformation) mnAction.getSchedulingInfo();
                    int remainingPreds = rInfo.decrementAndGet();

                    if (IS_DEBUG) {
                        LOGGER.debug(LOG_PREFIX + "Task " + successorId + " has " + remainingPreds
                            + " remaining predecessors (multi-node)");
                    }

                    if (remainingPreds > 0) {
                        allReady = false;
                    }
                }
                if (allReady) {
                    readyToLaunch.addAll(multiNodeSucc);
                    predefinedData.getWaitingMultiNodeActions().remove(successorId);
                }
            }

        }

        // try to lauch les tasques succesores que no tenen predecesors
        for (AllocatableAction readyAction : readyToLaunch) {
            try {
                readyAction.tryToLaunch();
                if (IS_DEBUG) {
                    LOGGER.debug(LOG_PREFIX + "Successfully launched ready action");
                }
            } catch (InvalidSchedulingException e) {
                if (IS_DEBUG) {
                    LOGGER.debug(LOG_PREFIX + "Failed to launch ready action: " + e.getMessage());
                }

                if (readyAction instanceof ExecutionAction) {
                    ExecutionAction failedExecution = (ExecutionAction) readyAction;
                    int failedTaskId = failedExecution.getTask().getId();
                    waitingActions.put(failedTaskId, readyAction);

                    PredefinedSchedulingInformation rInfo =
                        (PredefinedSchedulingInformation) readyAction.getSchedulingInfo();
                    rInfo.setPendingPredecessors(1);
                }
            }
        }

        return super.unscheduleAction(action);
    }

    @Override
    public Score generateResourceScore(AllocatableAction action, TaskDescription params, Score actionScore) {
        return actionScore;
    }

    @Override
    public Score generateImplementationScore(AllocatableAction action, TaskDescription params, Implementation impl,
        Score resourceScore) {
        return resourceScore;
    }
}
