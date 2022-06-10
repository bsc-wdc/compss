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
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;


public class RegisterRemoteObjectDataRequest extends APRequest {

    private final Application app;
    private final int code;
    private final String data;


    /**
     * Contructs a new Request to register an external object and bind it to an existing LogicalData.
     *
     * @param app application accessing the value
     * @param code Object Id.
     * @param data Existing LogicalData to bind the value access.
     */
    public RegisterRemoteObjectDataRequest(Application app, int code, String data) {
        this.app = app;
        this.code = code;
        this.data = data;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.REGISTER_REMOTE_OBJECT;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
        throws ShutdownException {
        dip.registerRemoteObjectSources(app, code, data);
    }

}
