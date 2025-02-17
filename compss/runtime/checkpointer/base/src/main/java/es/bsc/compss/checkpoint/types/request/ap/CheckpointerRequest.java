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
import es.bsc.compss.types.request.ap.APRequest;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;


public abstract class CheckpointerRequest implements APRequest {

    private final CheckpointRecord cp;


    /**
     * Request to by the checkpoint manager.
     *
     * @param cp CheckpointManager handling the request.
     */
    public CheckpointerRequest(CheckpointRecord cp) {
        this.cp = cp;
    }

    @Override
    public TraceEvent getEvent() {
        return TraceEvent.AP_CHECKPOINT_REQUEST;
    }

    public abstract TraceEvent getCheckpointEvent();

    @Override
    public void process(AccessProcessor ap, TaskDispatcher td) throws ShutdownException, COMPSsException {
        if (Tracer.isActivated()) {
            Tracer.emitEvent(getCheckpointEvent());
        }
        try {
            process(ap, td, this.cp);
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TraceEventType.CHECKPOINT_EVENTS_TYPE);
            }
        }
    }

    public abstract void process(AccessProcessor ap, TaskDispatcher td, CheckpointRecord cp)
        throws ShutdownException, COMPSsException;
}
