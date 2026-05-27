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
package es.bsc.compss.components.monitor.impl;

import java.time.Instant;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * Graph event submitted to the monitor graph endpoint.
 */
public class MonitorGraphEvent {

    private static final String DEFAULT_MASTER_NAME = "local-master";

    private final Instant ts = Instant.now();
    private final String runId;
    private final long appId;
    private final String type;
    private final String masterName;
    private final Map<String, Object> payload;


    /**
     * Creates a monitor graph event.
     *
     * @param runId execution run identifier
     * @param appId COMPSs application identifier
     * @param type event type
     * @param masterName master node name
     * @param payload event payload
     */
    public MonitorGraphEvent(String runId, long appId, String type, String masterName, Map<String, Object> payload) {
        this.runId = runId;
        this.appId = appId;
        this.type = type;
        this.masterName = masterName;
        this.payload = (payload == null || payload.isEmpty()) ? null : new LinkedHashMap<>(payload);
    }

    /**
     * Serializes the event as a JSON object.
     *
     * @return JSON representation
     */
    public String toJson() {
        StringBuilder sb = new StringBuilder(256);
        sb.append('{');
        kv(sb, "ts", this.ts.toString());
        sb.append(',');
        kv(sb, "run_id", this.runId);
        sb.append(',');
        kvNum(sb, "app_id", this.appId);
        sb.append(',');
        kv(sb, "type", this.type);
        sb.append(',');
        String finalMasterName =
            (this.masterName == null || this.masterName.trim().isEmpty()) ? DEFAULT_MASTER_NAME : this.masterName;
        kv(sb, "master_name", finalMasterName);
        if (this.payload != null) {
            sb.append(',').append("\"payload\":").append(mapToJson(this.payload));
        }
        sb.append('}');
        return sb.toString();
    }

    private static String mapToJson(Map<String, Object> m) {
        StringBuilder sb = new StringBuilder();
        sb.append('{');
        boolean first = true;
        for (Map.Entry<String, Object> e : m.entrySet()) {
            if (!first) {
                sb.append(',');
            }
            first = false;
            sb.append('"').append(esc(e.getKey())).append('"').append(':').append(asJson(e.getValue()));
        }
        sb.append('}');
        return sb.toString();
    }

    private static String asJson(Object v) {
        if (v == null) {
            return "null";
        }
        if (v instanceof Number || v instanceof Boolean) {
            return String.valueOf(v);
        }
        return '"' + esc(String.valueOf(v)) + '"';
    }

    private static void kv(StringBuilder sb, String k, String v) {
        sb.append('"').append(esc(k)).append('"').append(':');
        if (v == null) {
            sb.append("null");
        } else {
            sb.append('"').append(esc(v)).append('"');
        }
    }

    private static void kvNum(StringBuilder sb, String k, long v) {
        sb.append('"').append(esc(k)).append('"').append(':').append(v);
    }

    private static String esc(String s) {
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
