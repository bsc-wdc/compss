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

import es.bsc.compss.comm.Comm;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.DataInfoProvider;
import es.bsc.compss.components.impl.TaskAnalyser;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessid.RAccessId;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.data.operation.FileTransferable;
import es.bsc.compss.types.data.operation.OneOpWithSemListener;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


/**
 * The TransferRawFileRequest class represents a request to transfer a file located in a worker to be transferred to
 * another location without register the transfer.
 */
public class TransferRawFileRequest extends APRequest {

    private final RAccessId faId;
    private final DataLocation location;
    private final Semaphore sem;


    /**
     * Constructs a new TransferOpenFileRequest.
     *
     * @param faId Data Id and version of the requested file.
     * @param location Location where to leave the requested file.
     * @param sem Semaphore where to synchronize until the operation is done.
     */
    public TransferRawFileRequest(RAccessId faId, DataLocation location, Semaphore sem) {
        this.faId = faId;
        this.location = location;
        this.sem = sem;
    }

    /**
     * Returns the semaphore where to synchronize until the operation is done.
     *
     * @return Semaphore where to synchronize until the operation is done.
     */
    public Semaphore getSemaphore() {
        return this.sem;
    }

    /**
     * Returns the data Id and version of the requested file.
     *
     * @return Data Id and version of the requested file.
     */
    public RAccessId getFaId() {
        return this.faId;
    }

    /**
     * Returns the location where to leave the requested file.
     *
     * @return the location where to leave the requested file.
     */
    public DataLocation getLocation() {
        return this.location;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        // Make a copy of the original logical file, we don't want to leave track
        LogicalData sourceData = this.faId.getReadDataInstance().getData();
        Comm.getAppHost().getData(sourceData, this.location, (LogicalData) null, new FileTransferable(),
            new OneOpWithSemListener(this.sem));
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.TRANSFER_RAW_FILE;
    }

}
