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
package es.bsc.compss.checkpoint.types.request.ap;

import es.bsc.compss.checkpoint.CheckpointRecord;
import es.bsc.compss.components.impl.AccessProcessor;
import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.worker.COMPSsException;


public class CheckpointerSaveLastDataVersionsRequest extends CheckpointerRequest {

    /**
     * Requesting to save all last data versions from checkpointer.
     *
     * @param cp CheckpointerManager handling the request
     */
    public CheckpointerSaveLastDataVersionsRequest(CheckpointRecord cp) {
        super(cp);
    }

    @Override
    public TraceEvent getCheckpointEvent() {
        return TraceEvent.SAVE_LAST_DATA_VERSIONS;
    }

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td, CheckpointRecord cp)
        throws ShutdownException, COMPSsException {
        cp.requestSaveAllLastDataVersion();
    }
}
