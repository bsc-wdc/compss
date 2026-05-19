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
import java.util.ArrayList;
import java.util.List;

public final class ExecutorInfraType {

    private static final List<Event> EXECUTOR_COUNTS_EVENTS = new ArrayList<>();
    private static final List<Event> EXECUTOR_ACTIVITY_EVENTS = new ArrayList<>();

    public static final EventType EXECUTOR_COUNTS =
        Tracer.defineNewEventType(8_001_111, "Executor threads count", true, EXECUTOR_COUNTS_EVENTS);

    public static final EventType EXECUTOR_IDENTIFICATION =
        Tracer.defineNewEventType(8_001_112, "Executor thread identifier", true, new ArrayList<>());

    public static final EventType EXECUTOR_ACTIVITY =
        Tracer.defineNewEventType(8_001_113, "Executor thread activity", true, EXECUTOR_ACTIVITY_EVENTS);


    private ExecutorInfraType() {
        // Utility class
    }

    static void addExecutorCountsEvent(Event event) {
        EXECUTOR_COUNTS_EVENTS.add(event);
    }

    static void addExecutorActivityEvent(Event event) {
        EXECUTOR_ACTIVITY_EVENTS.add(event);
    }
}
