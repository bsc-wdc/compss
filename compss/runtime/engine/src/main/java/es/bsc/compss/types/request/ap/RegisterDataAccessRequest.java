/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.TaskListener;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.accessid.RWAccessId;
import es.bsc.compss.types.data.accessid.WAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.accessparams.AccessParams.AccessMode;

import java.util.concurrent.Semaphore;


public class RegisterDataAccessRequest extends APRequest implements TaskListener {

    private final AccessParams accessParams;
    private DataAccessId accessId;
    private final AccessMode accessMode;

    private int pendingOperation = 0;
    private final Semaphore sem;


    /**
     * Creates a new request to register a data access.
     *
     * @param access AccessParams to register.
     * @param accessMode Access mode to register the data access.
     */
    public RegisterDataAccessRequest(AccessParams access, AccessMode accessMode) {
        this.accessParams = access;
        this.accessMode = accessMode;
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
        return this.accessMode;
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
            if (DEBUG) {
                int dataId = this.accessId.getDataId();
                LOGGER.debug("Data " + dataId + " available for main access");
            }
            sem.release();
        } else {
            Application app = this.accessParams.getApp();
            app.stalled();
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
            if (DEBUG) {
                int dataId = this.accessId.getDataId();
                LOGGER.debug("Data " + dataId + " available for main access");
            }
            Application app = this.accessParams.getApp();
            app.readyToContinue(sem);
        }
    }

    @Override
    public APRequestType getRequestType() {
        return APRequestType.REGISTER_DATA_ACCESS;
    }

}
