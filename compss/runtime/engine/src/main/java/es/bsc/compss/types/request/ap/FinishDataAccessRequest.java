/*
 *  Copyright 2002-2023 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.data.DataInstanceId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;


public class FinishDataAccessRequest extends APRequest {

    private final AccessParams access;
    private final DataInstanceId generatedData;


    /**
     * Creates a new finish FileAccess request.
     * 
     * @param ap Associated AccessParams.
     * @param generatedData Associated AccessParams.
     */
    public FinishDataAccessRequest(AccessParams ap, DataInstanceId generatedData) {
        this.access = ap;
        this.generatedData = generatedData;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.FINISH_DATA_ACCESS;
    }

    @Override
    public void process(AccessProcessor ap, TaskAnalyser ta, DataInfoProvider dip, TaskDispatcher td)
        throws ShutdownException {
        dip.finishDataAccess(access, generatedData);
    }

}
