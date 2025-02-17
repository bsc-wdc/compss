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

import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.tracing.TraceEvent;
import java.util.concurrent.Semaphore;


/**
 * The TasksStateRequests class represents a request to obtain the progress of all the applications that are running.
 */
public class TasksStateRequest implements APRequest {

    private final Semaphore sem;
    private String response;


    /**
     * Constructs a new TaskStateRequest.
     *
     * @param sem semaphore where to synchronize until the current state is described.
     */
    public TasksStateRequest(Semaphore sem) {
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the current state is described.
     *
     * @return the semaphore where to synchronize until the current state is described.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the progress description in an xml format string.
     *
     * @return progress description in an xml format string.
     */
    public String getResponse() {
        return this.response;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        this.response = Application.getTaskStateRequest();
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.TASKSTATE;
    }

}
