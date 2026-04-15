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

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import java.util.Arrays;

public enum TaskExecutionEvent implements Event {

    TASK_RUNNING(11, "Task Running"), //

    CREATING_TASK_SANDBOX(54, "Worker: Creating task sandbox"), //
    REMOVING_TASK_SANDBOX(55, "Worker: Removing task sandbox"), //

    BIND_ORIG_NAME(62, "Bind Original File names To Renames.."), //
    UNBIND_ORIG_NAME(63, "Unbind Original File names To Renames.."), //
    CHECK_OUT_PARAM(64, "Check OUT parameters."), //
    INSTRUMENTING_CLASS(90, "Instrumenting nested class."), //

    // Timer events
    TASK_TIMEOUT(58, "Timer: Task timed out"); //


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_002_001, "Task Execution", true, Arrays.asList(TaskExecutionEvent.values()));
    }


    TaskExecutionEvent(int id, String signature) {
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
