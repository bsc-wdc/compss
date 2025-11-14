/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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

    // Environment variables
    public static final String ENV_EVENTS_API = "compss.events.api";
    public static final String ENV_OTEL_ENDPOINT = "compss.otel.endpoint";
    public static final String ENV_OTEL_SERVICE = "compss.otel.service";

    // Default values
    public static final String DEFAULT_MASTER_NAME = "local-master";
    public static final String DEFAULT_NODE_NAME = "master";
    public static final String DEFAULT_EVENTS_API = "http://localhost:8088/events";
    public static final String DEFAULT_OTEL_ENDPOINT = "http://localhost:4317";
    public static final String DEFAULT_SERVICE_NAME = "compss-agent";

    public static final Duration DEFAULT_OTEL_PERIOD = Duration.ofSeconds(1);
}
