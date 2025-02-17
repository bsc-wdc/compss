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
import es.bsc.compss.types.data.EngineDataInstanceId;
import es.bsc.compss.types.data.access.MainAccess;
import es.bsc.compss.types.data.accessid.EngineDataAccessId;
import es.bsc.compss.types.data.accessparams.AccessParams;
import es.bsc.compss.types.data.info.DataInfo;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;


public class FinishDataAccessRequest implements APRequest {

    private final MainAccess access;
    private final EngineDataInstanceId generatedData;


    /**
     * Creates a new finish FileAccess request.
     * 
     * @param ma Associated AccessParams.
     * @param generatedData Associated AccessParams.
     */
    public FinishDataAccessRequest(MainAccess ma, EngineDataInstanceId generatedData) {
        this.access = ma;
        this.generatedData = generatedData;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.FINISH_DATA_ACCESS;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) throws ShutdownException {
        access.finish(generatedData);
    }

}
