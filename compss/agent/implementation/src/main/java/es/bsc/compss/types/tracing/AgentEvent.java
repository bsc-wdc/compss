package es.bsc.compss.types.tracing;

import es.bsc.compss.util.Tracer;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import java.util.Arrays;

public enum AgentEvent implements Event {

    AGENT_ADD_RESOURCE(6002, "Add resources agent"), //
    AGENT_STOP(6003, "Stop agent"), //
    AGENT_REMOVE_NODE(6004, "Remove node agent"), //
    AGENT_REMOVE_RESOURCES(6005, "Remove resources agent"), //
    AGENT_RUN_TASK(6006, "Run task agent"); //


    public static final EventType type;

    private final int id;
    private final String signature;

    static {
        type = Tracer.defineNewEventType(8_005_001, "Agents events", true, Arrays.asList(AgentEvent.values()));
    }


    AgentEvent(int id, String signature) {
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
