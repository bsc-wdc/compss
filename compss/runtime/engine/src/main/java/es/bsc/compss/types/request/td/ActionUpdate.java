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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.worker.COMPSsException;


/**
 * The ActionUpdate class represents the notification of an update on the state of an allocatable action.
 */
public class ActionUpdate extends TDRequest {

    /**
     * Possible Updates applied to the action.
     */
    public enum Update {
        RUNNING, // The action has begin to run
        ERROR, // There has been an error during the execution
        COMPLETED, // The action execution has succeeded.
        EXCEPTION // The task has produced an exception
    }


    /**
     * The updated allocatable action.
     */
    private final AllocatableAction action;
    /**
     * Update to be notified.
     */
    private final Update update;
    /**
     * COMPSs Exception.
     */
    private COMPSsException exception;


    /**
     * Constructs a new NotifyAllocatableActionEnd for the task.
     *
     * @param action Associated action.
     * @param update Update to be notified.
     */
    public ActionUpdate(AllocatableAction action, Update update) {
        this.action = action;
        this.update = update;
        this.exception = null;
    }

    public void setCOMPSsException(COMPSsException e) {
        this.exception = e;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.ACTION_UPDATE;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        switch (this.update) {
            case RUNNING:
                ts.actionRunning(this.action);
                break;
            case COMPLETED:
                ts.actionCompleted(this.action);
                break;
            case ERROR:
                ts.errorOnAction(this.action);
                break;
            case EXCEPTION:
                ts.exceptionOnAction(this.action, this.exception);
                break;
        }
    }

}
