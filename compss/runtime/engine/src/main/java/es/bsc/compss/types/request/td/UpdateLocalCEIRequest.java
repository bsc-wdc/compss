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
import es.bsc.compss.util.CEIParser;
import es.bsc.compss.util.ResourceManager;

import java.util.List;
import java.util.concurrent.Semaphore;


public class UpdateLocalCEIRequest extends TDRequest {

    private final Class<?> ceiClass;
    private final Semaphore sem;


    /**
     * Creates a new request to update the local CoreElement Interface class.
     * 
     * @param ceiClass CoreElement Interface class to update.
     * @param sem Waiting semaphore.
     */
    public UpdateLocalCEIRequest(Class<?> ceiClass, Semaphore sem) {
        this.ceiClass = ceiClass;
        this.sem = sem;
    }

    /**
     * Returns the CoreElement Interface class.
     *
     * @return The coreElement Interface class.
     */
    public Class<?> getCeiClass() {
        return this.ceiClass;
    }

    /**
     * Returns the semaphore where to synchronize until the operation is done.
     *
     * @return Semaphore where to synchronize until the operation is done.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        LOGGER.debug("Treating request to update core elements");

        // Load new coreElements
        List<Integer> newCores = CEIParser.loadJava(this.ceiClass);
        if (DEBUG) {
            LOGGER.debug("New methods: " + newCores);
        }
        // Update Resources structures
        ResourceManager.coreElementUpdates(newCores);
        // Update Scheduler structures
        ts.coreElementsUpdated();

        // Release
        LOGGER.debug("Data structures resized and CE-resources links updated");
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.UPDATE_CEI_LOCAL;
    }

}
