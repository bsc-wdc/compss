package es.bsc.compss.types.tracing;

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.EventType;
import java.util.ArrayList;

public final class TransferType {

    public static final EventType TASK_TRANSFERS =
        Tracer.defineNewEventType(8_001_311, "Task Transfers Request", true, new ArrayList<>());

    public static final EventType DATA_TRANSFERS =
        Tracer.defineNewEventType(8_001_321, "Data Transfers", false, new ArrayList<>());


    private TransferType() {
        // Utility class
    }
}
