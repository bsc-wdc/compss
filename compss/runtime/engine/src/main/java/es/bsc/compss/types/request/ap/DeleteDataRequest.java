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
import es.bsc.compss.types.request.exceptions.ValueUnawareRuntimeException;
import es.bsc.compss.types.tracing.TraceEvent;
import java.util.concurrent.Semaphore;


public class DeleteDataRequest implements APRequest {

    private final Application app;
    private final DataParams data;
    private final Semaphore sem;

    private ValueUnawareRuntimeException unawareException = null;
    private final boolean applicationDelete;


    /**
     * Creates a new request to delete a file.
     *
     * @param app application requesting the data deletion
     * @param data data to delete
     * @param applicationDelete Whether the deletion was requested by the user code of the application {@literal true},
     *            or automatically removed by the runtime {@literal false}.
     */
    public DeleteDataRequest(Application app, DataParams data, boolean applicationDelete) {
        this.app = app;
        this.data = data;
        this.sem = new Semaphore(0);
        this.applicationDelete = applicationDelete;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) {
        try {
            // File is involved in some task execution
            // File Won't be read by any future task or from the main code.
            // Remove it from the dependency analysis and the files to be transferred back
            LOGGER.info("[DeleteDataRequest] Deleting Data in Task Analyser");
            DataInfo dataInfo = data.delete(this.app);
            int dataId = dataInfo.getDataId();
            LOGGER.info("Deleting data " + dataId);

            // Deleting checkpointed data that is obsolete, INOUT that has a newest version
            if (applicationDelete) {
                app.getCP().deletedData(dataInfo);
            }
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
