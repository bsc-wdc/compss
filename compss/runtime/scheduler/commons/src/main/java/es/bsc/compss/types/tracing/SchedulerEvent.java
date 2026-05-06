package es.bsc.compss.types.tracing;

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import java.util.Arrays;

public enum SchedulerEvent implements Event {

    READY_COUNT(1, "Ready queue count"); // Ready count


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_002_101, "Ready queue count", true, Arrays.asList(SchedulerEvent.values()));
    }


    SchedulerEvent(int id, String signature) {
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
