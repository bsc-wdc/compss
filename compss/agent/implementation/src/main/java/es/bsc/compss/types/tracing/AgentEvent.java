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

import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.Tracer;
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
