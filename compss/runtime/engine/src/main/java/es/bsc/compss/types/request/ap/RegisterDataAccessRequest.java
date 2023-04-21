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
import es.bsc.compss.types.TaskListener;
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


public class RegisterDataAccessRequest extends APRequest implements TaskListener {

    private final AccessParams accessParams;
    private DataAccessId accessId;

    private int pendingOperation = 0;
    private boolean released = false;
    private ValueUnawareRuntimeException unawareException = null;
    private final Semaphore sem;


    /**
     * Creates a new request to register a data access.
     *
     * @param access AccessParams to register.
     */
    public RegisterDataAccessRequest(AccessParams access) {
        this.accessParams = access;
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
        return this.accessParams.getMode();
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
        try {
            this.accessId = ta.processMainAccess(this);
        } catch (ValueUnawareRuntimeException e) {
            this.unawareException = e;
        }
        if (pendingOperation == 0) {
            this.notifyReady();
        }
        sem.release();
    }

    /**
     * Waits for the value's producing tasks to complete releasing and recovering the resources if needed.
     * 
     * @throws ValueUnawareRuntimeException the runtime is not aware of the last value of the accessed data
     */
    public void waitForCompletion() throws ValueUnawareRuntimeException {
        // Wait for request processing
        sem.acquireUninterruptibly();

        boolean stalled = false;
        Application app = this.accessParams.getApp();
        synchronized (this) {
            LOGGER.info("App " + app.getId() + " waits for data to be produced");
            if (!released) {
                LOGGER.info("App " + app.getId() + " releases its resources.");
                stalled = true;
                app.stalled();
            }
        }
        LOGGER.info("App " + app.getId() + " awaits for data to be produced.");
        // Wait for producing task completion
        sem.acquireUninterruptibly();
        LOGGER.info("App " + app.getId() + " waiting data is produced.");
        // Wait for app to have resources
        if (stalled) {
            LOGGER.info("App " + app.getId() + " waits for resources.");
            app.readyToContinue(sem);
            sem.acquireUninterruptibly();
            LOGGER.info("App " + app.getId() + " reasources are ready.");
        }
        if (this.unawareException != null) {
            throw this.unawareException;
        }
    }

    public void addPendingOperation() {
        pendingOperation++;
    }

    @Override
    public void taskFinished() {
        synchronized (this) {
            pendingOperation--;
            if (pendingOperation == 0) {
                notifyReady();
            }
        }
    }

    private void notifyReady() {
        if (accessId != null) {
            int dataId = this.accessId.getDataId();
            if (DEBUG) {
                LOGGER.debug("Data " + dataId + " available for main access");
            }
            this.accessId = this.accessId.consolidateValidVersions();
        } else {
            if (DEBUG) {
                LOGGER.debug("Inexisting data");
            }
        }
        released = true;
        sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.REGISTER_DATA_ACCESS;
    }

}
