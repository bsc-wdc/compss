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


public class AlreadyAccessedRequest extends APRequest {

    private final Application app;
    private final DataLocation loc;
    private final Semaphore sem;
    private boolean response;


    /**
     * Creates a new request for already accessed data.
     * 
     * @param app application querying the data access.
     * @param loc Data location
     * @param sem Waiting semaphore.
     */
    public AlreadyAccessedRequest(Application app, DataLocation loc, Semaphore sem) {
        this.app = app;
        this.loc = loc;
        this.sem = sem;
    }

    /**
     * Returns the data location.
     * 
     * @return The data location.
     */
    public DataLocation getLocation() {
        return this.loc;
    }

    /**
     * Returns the associated waiting semaphore.
     * 
     * @return The associated waiting semaphore.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the response message.
     * 
     * @return {@code true} if the location has been accessed, {@code false} otherwise.
     */
    public boolean getResponse() {
        return this.response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        boolean aa = dip.alreadyAccessed(this.app, this.loc);
        this.response = aa;
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.ALREADY_ACCESSED;
    }

}
