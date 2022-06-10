/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


public class WaitForDataReadyToDeleteRequest extends APRequest {

    private final Application app;
    private final DataLocation loc;
    private final Semaphore sem;
    private final Semaphore semWait;

    private int nPermits;


    /**
     * Creates a new request to wait for the data to be ready to be deleted.
     * 
     * @param app Application requesting to wait for the value to be ready for deletion
     * @param loc Data Location.
     * @param sem Waiting semaphore.
     * @param semWait Tasks semaphore.
     */
    public WaitForDataReadyToDeleteRequest(Application app, DataLocation loc, Semaphore sem, Semaphore semWait) {
        this.app = app;
        this.loc = loc;
        this.sem = sem;
        this.semWait = semWait;
        this.nPermits = 0;
    }

    /**
     * Returns the associated data location.
     * 
     * @return The associated data location.
     */
    public DataLocation getLocation() {
        return this.loc;
    }

    /**
     * Returns the number of permits of the tasks waiting semaphore.
     * 
     * @return The number of permits of the tasks waiting semaphore.
     */
    public int getNumPermits() {
        return this.nPermits;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        LOGGER.info("[WaitForDataReadyToDelete] Notifying waiting data " + this.loc.getPath() + "to DIP...");
        this.nPermits = dip.waitForDataReadyToDelete(this.app, this.loc, this.semWait);
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.DELETE_FILE;
    }

}
