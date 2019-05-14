/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;

import java.util.concurrent.Semaphore;


public class RegisterDataAccessRequest extends APRequest {

    private final AccessParams access;
    private final Semaphore sem;
    private DataAccessId response;


    /**
     * Creates a new request to register a data access.
     * 
     * @param access AccessParams to register.
     * @param sem Waiting semaphore.
     */
    public RegisterDataAccessRequest(AccessParams access, Semaphore sem) {
        this.access = access;
        this.sem = sem;
    }

    /**
     * Returns the associated access parameters.
     * 
     * @return The associated access parameters.
     */
    public AccessParams getAccess() {
        return this.access;
    }

    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the waiting semaphore.
     * 
     * @return The waiting semaphore.
     */
    public DataAccessId getResponse() {
        return this.response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        DataAccessId daId = dip.registerDataAccess(this.access);
        this.response = daId;
        this.sem.release();
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.REGISTER_DATA_ACCESS;
    }

}
