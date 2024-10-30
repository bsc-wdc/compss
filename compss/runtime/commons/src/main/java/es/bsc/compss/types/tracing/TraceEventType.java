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
package es.bsc.compss.types.tracing;

import java.util.LinkedList;
import java.util.List;


public enum TraceEventType {

    // Event codes
    // Core Element Id
    TASKS_FUNC(8_000_000, "Task", true),
    // API Invocations
    API(8_001_001, "API", true),
    // Runtime internal events
    RUNTIME(8_001_002, "Runtime", true),
    // Identifies the thread as AP, TD, executor...
    THREAD_IDENTIFICATION(8_001_003, "Thread type identifier", true),
    // Marks the life and end of an executor thread
    EXECUTOR_COUNTS(8_001_004, "Executor threads count", true),
    // Marks the activity of an executor
    EXECUTOR_ACTIVITY(8_001_005, "Executor thread activity", true),
    // Marks the life and end of an executor thread
    EXECUTOR_IDENTIFICATION(8_001_006, "Executor thread identifier", true),
    // Task Ids
    TASKS_ID(8_000_002, "Task IDs", false),

    CHECKPOINT_EVENTS_TYPE(8_001_100, "Checkpoint", true), TASK_TRANSFERS(8_000_003, "Task Transfers Request", true), //
    DATA_TRANSFERS(8_000_004, "Data Transfers", false), //
    STORAGE_TYPE(8_000_005, "Storage API", true), //
    READY_COUNTS(8_000_006, "Ready queue count", true), //
    TASKTYPE(8_000_007, "Type of task", true), //
    CPU_COUNTS(8_000_008, "Number of requested CPUs", false), //
    GPU_COUNTS(8_000_009, "Number of requested GPUs", false), //
    MEMORY(8_000_010, "Requested Memory", false), //
    DISK_BW(8_000_011, "Requested disk bandwidth", false), //
    SYNC(8_000_666, "Trace Synchronization event", true), //
    TASKS_CPU_AFFINITY(8_000_150, "Tasks CPU affinity", true), // Java assignment
    TASKS_GPU_AFFINITY(8_000_160, "Tasks GPU affinity", true), // Java assignment
    AGENT(8_006_000, "Agents events", true),

    // PYTHON RELATED EVENT GROUPS
    BINDING_TASKS_FUNC(9_000_000, "Binding tasks", true), // tasks emitted from master
    BINDING_INSIDE_TASKS(9_000_100, "Binding events inside tasks", true),
    BINDING_INSIDE_TASKS_CPU_AFFINITY(9_000_150, "Binding Tasks CPU affinity", true),
    BINDING_INSIDE_TASKS_CPU_COUNT(9_000_151, "Binding Tasks CPU count", true),
    BINDING_INSIDE_TASKS_GPU_AFFINITY(9_000_160, "Binding Tasks GPU affinity", true),
    BINDING_INSIDE_WORKER(9_000_200, "Binding events inside worker", true),
    BINDING_WORKER_CACHE(9_000_201, "Binding events in worker cache", true),
    BINDING_MASTER(9_000_300, "Binding master events", true),
    BINDING_SERIALIZATION_SIZE(9_000_600, "Binding serialization size events", true),
    BINDING_DESERIALIZATION_SIZE(9_000_601, "Binding deserialization size events", true),
    BINDING_SERIALIZATION_CACHE_SIZE(9_000_602, "Binding serialization cache size events", true),
    BINDING_DESERIALIZATION_CACHE_SIZE(9_000_603, "Binding deserialization cache size events", true),
    BINDING_SERIALIZATION_OBJECT_NUM(9_000_700, "Binding serialization object number", true),
    BINDING_DESERIALIZATION_OBJECT_NUM(9_000_701, "Binding deserialization object number", true);


    public final int code;
    public final String desc;
    public final boolean endable;
    private final List<TraceEvent> events;


    private TraceEventType(int code, String desc, boolean endable) {
        this.code = code;
        this.desc = desc;
        this.endable = endable;
        this.events = new LinkedList<>();
    }

    protected final void addEvent(TraceEvent event) {
        events.add(event);
    }

    public final List<TraceEvent> getEvents() {
        return events;
    }
}
