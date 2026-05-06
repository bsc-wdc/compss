package es.bsc.compss.types.tracing;

import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;

public enum ExecutorInfraEvent implements Event {

    EXECUTOR_COUNTS(1, ExecutorInfraType.EXECUTOR_COUNTS, "Executor counts"),
    EXECUTOR_ACTIVE(1, ExecutorInfraType.EXECUTOR_ACTIVITY, "Executor active");


    private final int id;
    private final EventType type;
    private final String signature;


    ExecutorInfraEvent(int id, EventType type, String signature) {
        this.id = id;
        this.type = type;
        this.signature = signature;

        if (type == ExecutorInfraType.EXECUTOR_COUNTS) {
            ExecutorInfraType.addExecutorCountsEvent(this);
        } else if (type == ExecutorInfraType.EXECUTOR_ACTIVITY) {
            ExecutorInfraType.addExecutorActivityEvent(this);
        }
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
