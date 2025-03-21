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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;

import java.util.List;
import java.util.concurrent.Semaphore;


public class DeleteAllApplicationDataRequest implements APRequest {

    private final Application app;
    private final Semaphore sem;


    /**
     * Constructs a new Request to remove all the data bound to an application.
     *
     * @param app application whose values are to be removed
     */
    public DeleteAllApplicationDataRequest(Application app) {
        this.app = app;
        this.sem = new Semaphore(0);
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.REMOVE_APP_DATA;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) throws ShutdownException {
        List<DataInfo> data = app.popAllData();
        for (DataInfo di : data) {
            di.delete();
        }
        this.sem.release();
    }

    /**
     * Waits until the operation has been processed.
     */
    public void waitForCompletion() {
        this.sem.acquireUninterruptibly();
    }

}
