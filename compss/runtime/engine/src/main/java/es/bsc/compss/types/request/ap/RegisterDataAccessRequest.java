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
import es.bsc.compss.types.TaskListener;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;

import java.util.concurrent.Semaphore;


public class RegisterDataAccessRequest extends APRequest implements TaskListener {

    private final AccessParams accessParams;
    private DataAccessId accessId;
    private final AccessMode taskMode;

    private int pendingOperation = 0;
    private final Semaphore sem;


    /**
     * Creates a new request to register a data access.
     *
     * @param access AccessParams to register.
     * @param taskMode Access mode to register the data access.
     */
    public RegisterDataAccessRequest(AccessParams access, AccessMode taskMode) {
        this.accessParams = access;
        this.taskMode = taskMode;
        this.sem = new Semaphore(0);
    }

    /**
     * Returns the associated access parameters.
     *
     * @return The associated access parameters.
     */
    public AccessParams getAccessParams() {
        return this.accessParams;
    }

    /**
     * Returns the associated access mode to the data.
     *
     * @return The associated access mode to the data.
     */
    public AccessParams.AccessMode getTaskAccessMode() {
        return this.taskMode;
    }

    /**
     * Returns the waiting semaphore.
     *
     * @return The waiting semaphore.
     */
    public DataAccessId getAccessId() {
        return this.accessId;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        this.accessId = ta.processMainAccess(this);
        if (pendingOperation == 0) {
            sem.release();
        }
    }

    public void waitForCompletion() {
        sem.acquireUninterruptibly();
    }

    public void addPendingOperation() {
        pendingOperation++;
    }

    @Override
    public void taskFinished() {
        pendingOperation--;
        if (pendingOperation == 0) {
            sem.release();
        }
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.REGISTER_DATA_ACCESS;
    }

}
