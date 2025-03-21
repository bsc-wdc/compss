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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.JobDispatcher;
import es.bsc.compss.util.ResourceManager;

import java.util.concurrent.Semaphore;


/**
 * This class represents a notification to end the execution.
 */
public class ShutdownRequest extends TDRequest {

    /**
     * Semaphore where to synchronize until the operation is done.
     */
    private final Semaphore semaphore;


    /**
     * Constructs a new ShutdownRequest.
     *
     * @param sem Waiting semaphore.
     */
    public ShutdownRequest(Semaphore sem) {
        this.semaphore = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the object can be read.
     *
     * @return the semaphore where to synchronize until the object can be read.
     */
    public Semaphore getSemaphore() {
        return this.semaphore;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        LOGGER.debug("Processing ShutdownRequest request...");

        // Shutdown Job Dispatcher
        JobDispatcher.shutdown();

        // Shutdown TaskScheduler
        ts.shutdown();

        // Print core state
        ResourceManager.stopNodes();

        // The semaphore is released after emitting the end event to prevent race conditions
        throw new ShutdownException(semaphore);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.TD_SHUTDOWN;
    }

}
