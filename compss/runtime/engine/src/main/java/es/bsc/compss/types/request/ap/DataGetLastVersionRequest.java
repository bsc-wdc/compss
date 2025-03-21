/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.Application;
import es.bsc.compss.types.data.LogicalData;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


/**
 * The DataGetLastVersionRequest is a request for the last version of a file contained in a remote worker.
 */
public class DataGetLastVersionRequest implements APRequest {

    private final Semaphore sem;
    private final Application app;
    private final DataParams data;
    private LogicalData response;


    /**
     * Constructs a new DataGetLastVersionRequest.
     *
     * @param app application obtaining the last version of the data
     * @param data data whose last version is wanted to be obtained
     */
    public DataGetLastVersionRequest(Application app, DataParams data) {
        this.app = app;
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
    public void process(AccessProcessor ap, TaskDispatcher td) {
        this.response = null;
        DataInfo dInfo = data.getRegisteredData(this.app);
        if (dInfo != null) {
            this.response = dInfo.getCurrentDataVersion().getDataInstanceId().getData();
        }
        sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.AP_GET_LAST_DATA;
    }

}
