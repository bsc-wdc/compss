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
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;
import java.util.concurrent.Semaphore;


public class DeleteDataRequest extends APRequest {

    private final DataParams data;
    private final Semaphore sem;

    private ValueUnawareRuntimeException unawareException = null;
    private final boolean applicationDelete;


    /**
     * Creates a new request to delete a file.
     * 
     * @param data data to delete
     * @param applicationDelete Whether the deletion was requested by the user code of the application {@literal true},
     *            or automatically removed by the runtime {@literal false}.
     */
    public DeleteDataRequest(DataParams data, boolean applicationDelete) {
        this.data = data;
        this.sem = new Semaphore(0);
        this.applicationDelete = applicationDelete;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td) {
        try {
            // File is involved in some task execution
            // File Won't be read by any future task or from the main code.
            // Remove it from the dependency analysis and the files to be transferred back
            LOGGER.info("[DeleteDataRequest] Deleting Data in Task Analyser");
            ta.deleteData(this.data, applicationDelete);
        } catch (ValueUnawareRuntimeException vure) {
            unawareException = vure;
        }
        this.sem.release();
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.DELETE_DATA;
    }

}
