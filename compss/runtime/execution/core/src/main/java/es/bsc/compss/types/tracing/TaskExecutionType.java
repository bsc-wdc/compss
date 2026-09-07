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
package es.bsc.compss.types.tracing;

import es.bsc.compss.types.implementations.MethodType;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.Tracer;
import java.util.ArrayList;
import java.util.List;

public final class TaskExecutionType {

    public static final EventType TASKS_ID;
    public static final EventType TASKTYPE;
    public static final EventType CPU_COUNTS;
    public static final EventType GPU_COUNTS;
    public static final EventType MEMORY;
    public static final EventType DISK_BW;
    public static final EventType TASKS_CPU_AFFINITY;
    public static final EventType TASKS_GPU_AFFINITY;

    private static final List<Event> TASKTYPE_EVENTS = new ArrayList<>();

    static {
        TASKS_ID = Tracer.defineNewEventType(8_001_133, "Task IDs", true, new ArrayList<>());
        TASKTYPE = Tracer.defineNewEventType(8_001_132, "Type of task", true, TASKTYPE_EVENTS);
        CPU_COUNTS = Tracer.defineNewEventType(8_001_151, "Number of requested CPUs", false, new ArrayList<>());
        GPU_COUNTS = Tracer.defineNewEventType(8_001_153, "Number of requested GPUs", false, new ArrayList<>());
        MEMORY = Tracer.defineNewEventType(8_001_155, "Requested Memory", false, new ArrayList<>());
        DISK_BW = Tracer.defineNewEventType(8_001_156, "Requested disk bandwidth", false, new ArrayList<>());
        TASKS_CPU_AFFINITY = Tracer.defineNewEventType(8_001_152, "Tasks CPU affinity", true, new ArrayList<>());
        TASKS_GPU_AFFINITY = Tracer.defineNewEventType(8_001_154, "Tasks GPU affinity", true, new ArrayList<>());

        registerTaskTypeEvents();
    }


    private TaskExecutionType() {
        // Utility class
    }

    private static void registerTaskTypeEvents() {
        for (MethodType methodType : MethodType.values()) {
            TASKTYPE_EVENTS.add(new SimpleEvent(TASKTYPE, methodType.ordinal(), methodType.name()));
        }
    }


    private static final class SimpleEvent implements Event {

        private final EventType type;
        private final int id;
        private final String signature;


        private SimpleEvent(EventType type, int id, String signature) {
            this.type = type;
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
            return this.type;
        }
    }
}
