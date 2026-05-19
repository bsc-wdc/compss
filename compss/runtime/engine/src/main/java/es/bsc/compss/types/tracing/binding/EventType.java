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
import java.util.ArrayList;

public class EventType {

    // PYTHON RELATED EVENT GROUPS
    private static final es.bsc.wdc.tracing.EventType BINDING_TASKS_FUNC =
        Tracer.defineNewEventType(9_000_000, "Binding tasks", true, new ArrayList<>(0)); // tasks emitted from master
    private static final es.bsc.wdc.tracing.EventType BINDING_INSIDE_TASKS_CPU_AFFINITY =
        Tracer.defineNewEventType(9_000_150, "Binding Tasks CPU affinity", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_INSIDE_TASKS_CPU_COUNT =
        Tracer.defineNewEventType(9_000_151, "Binding Tasks CPU count", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_INSIDE_TASKS_GPU_AFFINITY =
        Tracer.defineNewEventType(9_000_160, "Binding Tasks GPU affinity", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_SERIALIZATION_SIZE =
        Tracer.defineNewEventType(9_000_600, "Binding serialization size events", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_DESERIALIZATION_SIZE =
        Tracer.defineNewEventType(9_000_601, "Binding deserialization size events", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_SERIALIZATION_CACHE_SIZE =
        Tracer.defineNewEventType(9_000_602, "Binding serialization cache size events", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_DESERIALIZATION_CACHE_SIZE =
        Tracer.defineNewEventType(9_000_603, "Binding deserialization cache size events", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_SERIALIZATION_OBJECT_NUM =
        Tracer.defineNewEventType(9_000_700, "Binding serialization object number", true, new ArrayList<>(0));
    private static final es.bsc.wdc.tracing.EventType BINDING_DESERIALIZATION_OBJECT_NUM =
        Tracer.defineNewEventType(9_000_701, "Binding deserialization object number", true, new ArrayList<>(0));


    /**
     * Registers all the Events related to the binding.
     */
    public static void registerAllBindingEvents() {
        InsideTaskEvent t = InsideTaskEvent.BUILD_COMPSS_EXCEPTION_MESSAGE;
        InsideWorkerEvent w = InsideWorkerEvent.FINALIZE_EAR;
        MasterEvent m = MasterEvent.PYTHON_BARRIER;
        WorkerCacheEvent c = WorkerCacheEvent.CACHE_MSG_GET_EVENT;
    }
}
