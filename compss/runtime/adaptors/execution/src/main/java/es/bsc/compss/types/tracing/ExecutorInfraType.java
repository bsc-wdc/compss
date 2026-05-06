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

    public static final EventType SYNC =
        Tracer.defineNewEventType(8_000_001, "Trace Synchronization event", true, new ArrayList<>());


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
