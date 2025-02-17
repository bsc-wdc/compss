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
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.data.params.DataParams;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.concurrent.Semaphore;


public class AlreadyAccessedRequest implements APRequest {

    private final Application app;
    private final DataParams data;
    private final Semaphore sem;
    private boolean response;


    /**
     * Creates a new request for already accessed data.
     *
     * @param app application accessing the value
     * @param data data whose last version is wanted to be obtained
     */
    public AlreadyAccessedRequest(Application app, DataParams data) {
        this.app = app;
        this.sem = new Semaphore(0);
        this.data = data;
    }

    /**
     * Returns the data.
     *
     * @return The data.
     */
    public DataParams getData() {
        return this.data;
    }

    /**
     * Waits for the completion and returns the response message.
     *
     * @return {@code true} if the data has been accessed, {@code false} otherwise.
     */
    public final boolean getResponse() {
        this.sem.acquireUninterruptibly();
        return this.response;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        LOGGER.debug("Check already accessed: " + data.getDescription());
        DataInfo dInfo = data.getRegisteredData(this.app);
        this.response = dInfo != null;
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.ALREADY_ACCESSED;
    }

}
