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
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.accessparams.DataParams;
import es.bsc.compss.types.data.accessparams.DataParams.FileData;
import es.bsc.compss.types.data.location.DataLocation;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


/**
 * The DataGetLastVersionRequest is a request for the last version of a file contained in a remote worker.
 */
public class DataGetLastVersionRequest extends APRequest {

    private final Semaphore sem;

    private final DataParams data;
    private LogicalData response;


    /**
     * Constructs a new DataGetLastVersionRequest.
     *
     * @param data data whose last version is wanted to be obtained
     */
    public DataGetLastVersionRequest(DataParams data) {
        this.sem = new Semaphore(0);
        this.data = data;
    }

    /**
     * Returns the requested LogicalData instance.
     *
     * @return the requested LogicalData instance.
     */
    public LogicalData getData() {
        sem.acquireUninterruptibly();
        return this.response;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        this.response = dip.getDataLastVersion(data);
        sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.AP_GET_LAST_DATA;
    }

}
