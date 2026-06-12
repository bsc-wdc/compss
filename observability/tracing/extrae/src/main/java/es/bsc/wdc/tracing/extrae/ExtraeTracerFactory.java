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
package es.bsc.wdc.tracing.extrae;

import es.bsc.wdc.tracing.TracingBackend;
import es.bsc.wdc.tracing.TracingBackendFactory;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Properties;

public class ExtraeTracerFactory implements TracingBackendFactory {

    // -------------- Constants ----------------
    private static final String TYPE = "extrae";

    // Paths
    private static final String REL_DEPS_DIR = "Dependencies" + File.separator;
    private static final String REL_DEPS_EXTRAE_DIR = REL_DEPS_DIR + "extrae" + File.separator;

    // Properties
    public static final String WORKING_DIR = "compss.extrae.working_dir";
    public static final String WORKER_CONFIG_FILE = "compss.extrae.file";

    // PROPERTIES DEFAULTS
    private static final String CURRENT_WORKING_DIR = ".";
    private static final String CUSTOM_EXTRAE_FILE = "null";


    @Override
    public String getType() {
        return TYPE;
    }

    @Override
    public TracingBackend create(String iDir, int hostId, String nodeName, String configLocation) {

        Properties props = loadProperties(configLocation);

        String extraeLib = iDir + (iDir.endsWith(File.separator) ? "" : File.separator) + REL_DEPS_EXTRAE_DIR;

        String folder = getProperty(WORKING_DIR, props, CURRENT_WORKING_DIR);
        if (!folder.endsWith(File.separator)) {
            folder += File.separator;
        }
        String outputDir = folder;

        String workerExtraeFile = getProperty(WORKER_CONFIG_FILE, props, CUSTOM_EXTRAE_FILE);

        return new ExtraeTracer(hostId, extraeLib, outputDir, workerExtraeFile);

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
