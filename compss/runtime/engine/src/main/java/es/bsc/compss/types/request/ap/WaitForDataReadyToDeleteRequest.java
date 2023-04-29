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
import es.bsc.compss.types.data.DataParams;
import es.bsc.compss.types.request.exceptions.NonExistingValueException;
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;
import java.util.concurrent.Semaphore;


public class WaitForDataReadyToDeleteRequest extends APRequest {

    private final DataParams data;

    private ValueUnawareRuntimeException vure = null;
    private NonExistingValueException neve = null;
    private final Semaphore sem;


    /**
     * Creates a new request to wait for the data to be ready to be deleted.
     * 
     * @param data data to wait to be ready for its removal
     */
    public WaitForDataReadyToDeleteRequest(DataParams data) {
        this.data = data;
        this.sem = new Semaphore(0);
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        LOGGER.info("[WaitForDataReadyToDelete] Notifying waiting data " + this.data.getDescription() + "to DIP...");
        try {
            dip.waitForDataReadyToDelete(this.data, this.sem);
        } catch (ValueUnawareRuntimeException vure) {
            this.vure = vure;
            this.sem.release();
        } catch (NonExistingValueException neve) {
            this.neve = neve;
            this.sem.release();
        }
    }

    /**
     * Wait until the data is ready to be deleted.
     * 
     * @throws ValueUnawareRuntimeException the runtime is not aware of the data
     * @throws NonExistingValueException the data to delete does not actually exist
     */
    public void waitForDataReadiness() throws ValueUnawareRuntimeException, NonExistingValueException {
        this.sem.acquireUninterruptibly();
        if (vure != null) {
            throw vure;
        }
        if (neve != null) {
            throw neve;
        }

    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.DELETE_DATA;
    }

}
