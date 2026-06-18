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
package es.bsc.wdc.tracing.monitor;

import java.time.Duration;

public class Constants {

    // Properties
    public static final String RUN_ID = "compss.uuid";
    public static final String LOG_DIR = "compss.log.dir";

    public static final String EVENTS_API = "compss.events.api";
    public static final String GRAPH_API = "compss.graph.api";
    public static final String OTEL_ENDPOINT = "compss.otel.endpoint";
    public static final String OTEL_SERVICE = "compss.otel.service";
    public static final String ENV_MONITOR_HOST = "COMPSS_MONITOR_HOST";

    // Default values
    public static final String DEFAULT_MASTER_NAME = "local-master";
    public static final String DEFAULT_NODE_NAME = "master";
    public static final String DEFAULT_MONITOR_HOST = "localhost";
    public static final String DEFAULT_WORKER_NAME = "local-worker";
    public static final String DEFAULT_EVENTS_API = "http://localhost:8088/monitored-events";
    public static final String DEFAULT_GRAPH_API = "http://localhost:8088/graph-events";
    public static final String DEFAULT_OTEL_ENDPOINT = "http://localhost:4317";
    public static final String DEFAULT_SERVICE_NAME = "compss-agent";
    public static final String DEFAULT_RUN = "unknown_run";

    public static final Duration DEFAULT_OTEL_PERIOD = Duration.ofSeconds(1);


    private Constants() {
        // Utility class
    }

    public static String getDefaultEventsApi() {
        return "http://" + getMonitorHost() + ":8088/monitored-events";
    }

    public static String getDefaultGraphApi() {
        return "http://" + getMonitorHost() + ":8088/graph-events";
    }

    public static String getDefaultOtelEndpoint() {
        return "http://" + getMonitorHost() + ":4317";
    }

    private static String getMonitorHost() {
        String host = System.getenv(ENV_MONITOR_HOST);
        if (host == null || host.trim().isEmpty()) {
            return DEFAULT_MONITOR_HOST;
        }
        return host.trim();
    }
}
