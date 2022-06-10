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
import es.bsc.compss.types.data.DataAccessId;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


/**
 * The TransferObjectRequest is a request for an object contained in a remote worker.
 */
public class TransferObjectRequest extends APRequest {

    private final DataAccessId daId;
    private final Semaphore sem;

    private Object response;

    private LogicalData target;


    /**
     * Constructs a new TransferObjectRequest.
     *
     * @param daId Object required, data id + version.
     * @param sem Semaphore where to synchronize until the operation is done.
     */
    public TransferObjectRequest(DataAccessId daId, Semaphore sem) {
        this.daId = daId;
        this.sem = sem;
    }

    /**
     * Returns the data id + version of the required object.
     *
     * @return data id + version of the required object.
     */
    public DataAccessId getDaId() {
        return this.daId;
    }

    /**
     * Returns the semaphore where to synchronize until the object can be read.
     *
     * @return the semaphore where to synchronize until the object can be read.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the requested object (Null if it was on a file).
     *
     * @return the requested object (Null if it was on a file).
     */
    public Object getResponse() {
        return this.response;
    }

    /**
     * Returns the requested LogicalData instance.
     *
     * @return the requested LogicalData instance.
     */
    public LogicalData getTargetData() {
        return this.target;
    }

    /**
     * Sets the requested LogicalData instance.
     *
     * @param ld the requested LogicalData instance.
     */
    public void setTargetData(LogicalData ld) {
        this.target = ld;

    }

    /**
     * Sets the requested object.
     *
     * @param response The requested object.
     */
    public void setResponse(Object response) {
        this.response = response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        dip.transferObjectValue(this);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.TRANSFER_OBJECT;
    }

}
