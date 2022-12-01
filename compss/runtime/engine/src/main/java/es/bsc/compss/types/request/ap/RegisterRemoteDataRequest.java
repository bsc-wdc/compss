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
import es.bsc.compss.types.data.accessparams.DataParams;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;


public class RegisterRemoteDataRequest extends APRequest {

    private final DataParams accessedValue;
    private final String data;


    /**
     * Contructs a new Request to register an external file and bind it to an existing LogicalData.
     *
     * @param accessedValue the value being accessed by the application
     * @param data Existing LogicalData to bind the value access.
     */
    public RegisterRemoteDataRequest(DataParams accessedValue, String data) {
        this.accessedValue = accessedValue;
        this.data = data;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.REGISTER_REMOTE_DATA;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
        throws ShutdownException {
        dip.registerRemoteDataSources(accessedValue, data);
    }

}
