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

import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;

import org.apache.logging.log4j.Logger;


/**
 * The MonitoringDataRequest class represents a request to obtain the current resources and cores that can be run.
 */
public class TaskSummaryRequest extends TaskDispatcher.SynchTDRequest<Void> {

    /**
     * Logger where to print information.
     */
    private final Logger logger;


    /**
     * Constructs a new TaskStateRequest.
     *
     * @param td TaskDispatcher processing the event
     * @param logger Logger where to dump the task state.
     */
    public TaskSummaryRequest(TaskDispatcher td, Logger logger) {
        td.super();
        this.logger = logger;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        ts.getTaskSummary(this.logger);
        this.onCompletion();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.MONITORING_DATA;
    }

}
