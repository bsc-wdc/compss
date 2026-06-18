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

import es.bsc.wdc.tracing.TracingBackend;
import es.bsc.wdc.tracing.TracingBackendFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class MonitorTracerFactory implements TracingBackendFactory {

    // -------------- Constants ----------------
    private static final String TYPE = "monitor";


    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public TracingBackend create(String installDir, int hostId, String nodeName, String configLocation) {
        String masterName = hostId == 0 ? System.getProperty("compss.masterName") : Constants.DEFAULT_WORKER_NAME;
        Properties props = loadProperties(configLocation);

        String runId = getProperty(Constants.RUN_ID, props, null);
        if (runId == null) {
            String logDir = getProperty(Constants.LOG_DIR, props, null);
            if (logDir != null) {
                try {
                    File dir = new File(logDir);
                    runId = dir.getName(); // last directory
                } catch (Exception e) {
                    runId = Constants.DEFAULT_RUN;
                }
            } else {
                runId = Constants.DEFAULT_RUN;
            }
        }

        String serviceName = getProperty(Constants.OTEL_SERVICE, props, Constants.DEFAULT_SERVICE_NAME);
        String metricsEP = getProperty(Constants.OTEL_ENDPOINT, props, Constants.getDefaultOtelEndpoint());
        String eventsEP = getProperty(Constants.EVENTS_API, props, Constants.getDefaultEventsApi());

        return new MonitorTracer(nodeName, masterName, runId, serviceName, metricsEP, eventsEP);
    }

    private static Properties loadProperties(String configLocation) {
        Properties props = new Properties();
        if (configLocation != null) {
            try {
                InputStream stream = Files.newInputStream(Paths.get(configLocation));
                props.load(stream);
            } catch (IOException e) {
                System.err.println(WARN_IT_FILE_NOT_READ); // NOSONAR
                e.printStackTrace(); // NOSONAR
            }
        }
        return props;
    }

    private static String getProperty(String propKey, Properties props, String defaultValue) {
        String propVal = System.getProperty(propKey);
        if (propVal == null || propVal.isEmpty() || "null".compareTo(propVal) == 0) {
            propVal = props.getProperty(propKey, defaultValue);
            if (propVal.isEmpty() || "null".compareTo(propVal) == 0) {
                propVal = defaultValue;
            }
        }
        return propVal;
    }
}
