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
package es.bsc.compss.types.request.ap;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.TaskMonitor;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.AbstractTask;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.TaskDescription;
import es.bsc.compss.types.TaskState;
import es.bsc.compss.types.tracing.TraceEvent;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TaskAnalysisRequest implements APRequest {

    private static final Logger TIMER_LOGGER = LogManager.getLogger(Loggers.TIMER);
    private static final boolean IS_TIMER_COMPSS_ENABLED;

    static {
        // Load timer property
        String isTimerCOMPSsEnabledProperty = System.getProperty(COMPSsConstants.TIMER_COMPSS_NAME);
        IS_TIMER_COMPSS_ENABLED = (isTimerCOMPSsEnabledProperty == null || isTimerCOMPSsEnabledProperty.isEmpty()
            || isTimerCOMPSsEnabledProperty.equals("null")) ? false : Boolean.valueOf(isTimerCOMPSsEnabledProperty);
    }

    private final Task task;


    /**
     * Creates a new request to analyze a task.
     * 
     * @param task Task to analyze.
     */
    public TaskAnalysisRequest(Task task) {
        this.task = task;
    }

    /**
     * Returns the task to analyze.
     * 
     * @return The task to analyze.
     */
    public AbstractTask getTask() {
        return this.task;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        // Process task
        if (IS_TIMER_COMPSS_ENABLED) {
            long startTime = System.nanoTime();
            processTask();
            long endTime = System.nanoTime();
            float elapsedTime = (endTime - startTime) / (float) 1_000_000;
            TIMER_LOGGER.info("[TIMER] TA Process of task " + this.task.getId() + ": " + elapsedTime + " ms");
        } else {
            processTask();
        }

        // Check if the task has been checkpointed
        if (this.task.getStatus() == TaskState.RECOVERED) {
            if (DEBUG) {
                LOGGER.debug("Task " + this.task.getId() + " was checkpointed in a previous run. Skipping execution.");
            }
            this.task.end(true);
        } else {
            // Send request to schedule task
            td.executeTask(ap, this.task);
        }

        // Notify task monitor
        TaskMonitor monitor = this.task.getTaskMonitor();
        monitor.onAccessesProcessed();
    }

    private void processTask() {
        TaskDescription description = this.task.getTaskDescription();
        LOGGER.info("New " + description.getType().toString().toLowerCase() + " task: Name:" + description.getName()
            + "), ID = " + this.task.getId() + " APP = " + this.task.getApplication().getId());

        task.register();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.ANALYSE_TASK;
    }

}
