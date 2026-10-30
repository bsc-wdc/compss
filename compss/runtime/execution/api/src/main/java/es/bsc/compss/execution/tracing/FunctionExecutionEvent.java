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
package es.bsc.compss.execution.tracing;

import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.ExtensibleEventType;
import es.bsc.wdc.tracing.Tracer;
import java.util.LinkedList;

/**
 * Base type for the extensible tracing events describing the execution of a function. Concrete events refine this class
 * and are emitted by the language instrumentation while a function runs.
 */
public abstract class FunctionExecutionEvent implements Event {

    private static final int TASK_FUNC_ID = 8_001_131;

    /**
     * Tracing event type describing the execution of a function.
     */
    public static final ExtensibleEventType TYPE =
        Tracer.defineNewExtensibleEventType(TASK_FUNC_ID, "Task", true, new LinkedList<>());


    @Override
    public final EventType getType() {
        return TYPE;
    }
}
