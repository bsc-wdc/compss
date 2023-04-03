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
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessparams.DataParams.ObjectData;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


public class IsObjectHereRequest extends APRequest {

    private final Application app;
    private final int code;
    private final Semaphore sem;

    private boolean response;


    /**
     * Constructs a new AP request to check whether the object is in the main memory or not.
     * 
     * @param app Application accessing the object
     * @param code Object being accessed
     * @param sem semaphore where the request will wait
     */
    public IsObjectHereRequest(Application app, int code, Semaphore sem) {
        this.app = app;
        this.code = code;
        this.sem = sem;
    }

    /**
     * Returns the associated object code.
     * 
     * @return The associated object code.
     */
    public int getCode() {
        return this.code;
    }

    /**
     * Returns the waiting semaphore of the request.
     * 
     * @return The waiting semaphore of the request.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the request response.
     * 
     * @return {@code true} if the data is present in the master, {@code false} otherwise.
     */
    public boolean getResponse() {
        return this.response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        DataInstanceId dId = dip.getLastDataAccess(new ObjectData(this.app, this.code));
        this.response = dip.isHere(dId);
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.IS_OBJECT_HERE;
    }

}
