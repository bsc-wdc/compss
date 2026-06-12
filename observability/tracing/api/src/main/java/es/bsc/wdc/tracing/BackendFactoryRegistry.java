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

package es.bsc.wdc.tracing;

import java.util.HashMap;
import java.util.Map;
import java.util.ServiceLoader;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class BackendFactoryRegistry {

    // Logger
    protected static Logger logger = LogManager.getLogger(Loggers.TRACING);
    protected static boolean debug = logger.isDebugEnabled();

    // Backend factories
    private static final Map<String, TracingBackendFactory> factories = new HashMap<>();

    static {
        discover();
    }


    private static void discover() {
        logger.info("Discovering TracingBackends loaded in classpath");
        for (TracingBackendFactory f : ServiceLoader.load(TracingBackendFactory.class)) {
            logger.info("\tDiscovered TracingBackend for {}", f.getType());
            factories.put(f.getType(), f);
        }
    }

    public static TracingBackendFactory getTracingBackendFactory(String type) {
        return factories.get(type);
    }
}
