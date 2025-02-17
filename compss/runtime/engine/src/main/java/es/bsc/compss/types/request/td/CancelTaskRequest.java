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

import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.scheduler.types.AllocatableAction;
import es.bsc.compss.types.Task;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.request.listener.RequestListener;
import es.bsc.compss.types.tracing.TraceEvent;


/**
 * The CancelTaskRequest class represents the request to cancel a task's executions.
 */
public class CancelTaskRequest extends TDRequest {

    private Task task;
    private final RequestListener listener;


    /**
     * Constructs a new request to cancel the tasks passed in as a parameter whose end won't be notified.
     *
     * @param task task to cancel
     */
    public CancelTaskRequest(Task task) {
        this.task = task;
        this.listener = null;
    }

    /**
     * Constructs a new request to cancel the tasks passed in as a parameter whose completion will be notified to a
     * given listener.
     *
     * @param task task to cancel
     * @param listener listener to notify when the task has been cancelled
     */
    public CancelTaskRequest(Task task, RequestListener listener) {
        this.task = task;
        this.listener = listener;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.CANCEL_TASKS;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        for (AllocatableAction aa : task.getExecutions()) {
            ts.cancelAllocatableAction(aa);
        }
        if (listener != null) {
            listener.performed();
        }
    }

}
