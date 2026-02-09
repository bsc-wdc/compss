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
package es.bsc.wdc.tracing.monitor.events;

import es.bsc.wdc.tracing.monitor.Constants;

import java.time.Instant;


public class MonitoredEvent {

    private final Instant ts;
    private final String agentId;
    private final String nodeName;
    private final String threadType;
    private final long threadId;
    private final int eventType;
    private final int eventCode;
    private final String eventName;


    /**
     * Constructs a new MonitoredEvent.
     *
     * @param agentId agentId agent where the event occurred
     * @param nodeName node name where the event occurred
     * @param threadType thread Type of the event emitter
     * @param threadId Thread Id of the event emitter
     * @param eventType Type of the event
     * @param eventCode code of the event
     * @param eventName name of the event
     */
    public MonitoredEvent(String agentId, String nodeName, String threadType, long threadId, int eventType,
        int eventCode, String eventName) {
        this.ts = Instant.now();
        this.agentId = agentId;
        this.nodeName = nodeName;
        this.threadType = threadType;
        this.threadId = threadId;
        this.eventType = eventType;
        this.eventCode = eventCode;
        this.eventName = eventName;
    }

    @Override
    public final String toString() {
        StringBuilder sb = new StringBuilder(256);

        // fields
        sb.append('{');
        kv(sb, "ts", ts.toString());
        sb.append(",");
        String finalAgentId = (agentId == null | agentId.trim().isEmpty()) ? Constants.DEFAULT_MASTER_NAME : agentId;
        kv(sb, "agent_id", finalAgentId);
        sb.append(",");
        String finalNodeName = (nodeName != null) ? nodeName : Constants.DEFAULT_NODE_NAME;
        kv(sb, "node_name", finalNodeName);
        sb.append(",");
        kv(sb, "thread_type", threadType);
        sb.append(",");
        kvNum(sb, "thread_id", threadId);
        sb.append(",");
        kvNum(sb, "event_type", eventType);
        sb.append(",");
        kvNum(sb, "event_code", eventCode);
        sb.append(",");
        kv(sb, "event_name", eventName);
        completeWithSpecificDescription(sb);
        sb.append('}');
        return sb.toString();
    }

    protected void completeWithSpecificDescription(StringBuilder sb) {
        // Do nothing
    }

    // helpers JSON (sin libs)
    protected final void kv(StringBuilder sb, String k, String v) {
        sb.append('"').append(esc(k)).append('"').append(':');
        if (v == null) {
            sb.append("null");
        } else {
            sb.append('"').append(esc(v)).append('"');
        }
    }

    protected final void kvNum(StringBuilder sb, String k, long v) {
        sb.append('"').append(esc(k)).append('"').append(':').append(v);
    }

    protected final void kvBool(StringBuilder sb, String k, boolean v) {
        sb.append('"').append(esc(k)).append('"').append(':').append(v ? "true" : "false");
    }

    protected static String esc(String s) {
        StringBuilder out = new StringBuilder(s.length() + 8);
        for (int i = 0; i < s.length(); i++) {
            char c = s.charAt(i);
            switch (c) {
                case '\\':
                    out.append("\\\\");
                    break;
                case '"':
                    out.append("\\\"");
                    break;
                case '\b':
                    out.append("\\b");
                    break;
                case '\f':
                    out.append("\\f");
                    break;
                case '\n':
                    out.append("\\n");
                    break;
                case '\r':
                    out.append("\\r");
                    break;
                case '\t':
                    out.append("\\t");
                    break;
                default:
                    if (c < 0x20) {
                        out.append(String.format("\\u%04x", (int) c));
                    } else {
                        out.append(c);
                    }
            }
        }
        return out.toString();
    }
}
