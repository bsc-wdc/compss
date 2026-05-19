/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.types.tracing.binding;

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import java.util.Arrays;
import java.util.List;

public enum InsideWorkerEvent implements Event {

    // Python Events Inside Worker
    WORKER_RUNNING(1, "Worker running"), //
    PROCESS_TASK_PYTHON(2, "Process task"), //
    PROCESS_PING_PYTHON(3, "Process ping"), //
    PROCESS_QUIT_PYTHON(4, "Process quit"), //
    INIT_STORAGE(5, "Init storage"), //
    STOP_STORAGE(6, "Stop storage"), //
    INIT_STORAGE_WORKER(7, "Init storage at worker"), //
    STOP_STORAGE_WORKER(8, "Stop storage at worker"), //
    INIT_STORAGE_WORKER_PROCESS(9, "Init storage at worker process"), //
    STOP_STORAGE_WORKER_PROCESS(10, "Stop storage at worker process"), //
    LOAD_EAR(11, "Load EAR at worker"), //
    FINALIZE_EAR(12, "Finalize EAR at worker"), //
    PRELOAD_IMPORT(13, "Preload imports"), //
    ;


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        List<Event> events = Arrays.asList(InsideWorkerEvent.values());
        type = Tracer.defineNewEventType(9_000_200, "Binding events inside worker", true, events);
    }


    InsideWorkerEvent(int id, String signature) {
        this.id = id;
        this.signature = signature;
    }

    @Override
    public int getId() {
        return this.id;
    }

    @Override
    public String getSignature() {
        return this.signature;
    }

    @Override
    public EventType getType() {
        return type;
    }

}
