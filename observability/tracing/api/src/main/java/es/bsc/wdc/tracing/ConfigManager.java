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

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConfigManager {

    // --------- Constants --------
    // Error
    private static final String WARN_IT_FILE_NOT_READ = "WARNING: COMPSs Properties file could not be read";
    private static final String WARN_INVALID_HOSTID =
        "No valid hostID provided to the tracing system. Using default {} Provided ID: {}";

    // Properties locations
    public static final String CONFIG_LOCATION = "tracing.config_file";
    private static final String DEFAULT_CONFIG_NAME = "tracing.properties";

    // Properties
    public static final String TRACING_INSTALL_DIR = "tracing.install_dir";
    public static final String TRACING_HOST_ID = "tracing.host.id";
    public static final String TRACING_HOST_NAME = "tracing.host.name";
    public static final String TRACING_MONITOR = "compss.tracing.monitor";
    public static final String TRACING_EXTRAE = "compss.tracing.extrae";
    public static final String TRACING_WORKING_DIR = "compss.extrae.working_dir";
    public static final String TRACING_TASK_DEPENDENCIES = "compss.tracing.task.dependencies";

    // Default values
    private static final String DISABLED = "false";
    private static final String CURRENT_WORKING_DIR = ".";
    private static final String DEFAULT_HOSTNAME = "master";
    private static final String DEFAULT_HOSTID = "0";

    // --------- Configuration --------
    // Logger
    protected static Logger logger = LogManager.getLogger(Loggers.TRACING);
    protected static boolean debug = logger.isDebugEnabled();


    /**
     * Sets up all the necessary environment variables.
     */
    public static final void loadProperties() throws ConfigException {
        InputStream stream = findPropertiesConfigFile();

        Properties props = new Properties();
        if (stream != null) {
            try {
                props.load(stream);
            } catch (IOException ioe) {
                System.err.println(WARN_IT_FILE_NOT_READ); // NOSONAR
                ioe.printStackTrace(); // NOSONAR
            }
        }

        setProperties(props);
        String installDir = System.getProperty(TRACING_INSTALL_DIR);
        if (installDir == null || installDir.isEmpty() || "null".compareTo(installDir) == 0) {
            throw new ConfigException("Tracing installDir is not defined. Please define " + TRACING_INSTALL_DIR);
        }
    }

    private static InputStream findPropertiesConfigFile() {
        String propertiesLoc = System.getProperty(CONFIG_LOCATION);
        if (propertiesLoc != null) {
            try {
                return new FileInputStream(propertiesLoc);
            } catch (FileNotFoundException e) {
                System.err.println(WARN_IT_FILE_NOT_READ); // NOSONAR
                e.printStackTrace(); // NOSONAR
            }
        }

        final Class<?> clazz = ConfigManager.class;
        // Try to get as resource from class
        InputStream stream = clazz.getResourceAsStream(DEFAULT_CONFIG_NAME);
        if (stream == null) {
            stream = clazz.getResourceAsStream(File.separator + DEFAULT_CONFIG_NAME);
        }

        // From the loader that loaded this class
        ClassLoader clazzLoader = null;
        if (stream == null) {
            clazzLoader = clazz.getClassLoader();
            stream = clazzLoader.getResourceAsStream(DEFAULT_CONFIG_NAME);
            if (stream == null) {
                stream = clazzLoader.getResourceAsStream(File.separator + DEFAULT_CONFIG_NAME);
            }
        }

        // IT properties file not found. Looking at parent ClassLoader
        ClassLoader clazzLoaderParent = null;
        if (stream == null) {
            clazzLoaderParent = clazzLoader.getParent();
            stream = clazzLoaderParent.getResourceAsStream(DEFAULT_CONFIG_NAME);
            if (stream == null) {
                stream = clazzLoaderParent.getResourceAsStream(File.separator + DEFAULT_CONFIG_NAME);
            }
        }

        // IT properties file not found in classloader. Looking at system resources
        if (stream == null) {
            stream = ClassLoader.getSystemResourceAsStream(DEFAULT_CONFIG_NAME);
            if (stream == null) {
                stream = ClassLoader.getSystemResourceAsStream(File.separator + DEFAULT_CONFIG_NAME);
            }
        }

        return stream;
    }

    private static void setProperties(Properties props) {
        setProperty(props, TRACING_MONITOR, DISABLED);
        setProperty(props, TRACING_EXTRAE, DISABLED);
        setProperty(props, TRACING_WORKING_DIR, CURRENT_WORKING_DIR);
        setProperty(props, TRACING_TASK_DEPENDENCIES, DISABLED);
        setProperty(props, TRACING_HOST_ID, DEFAULT_HOSTID);
        String hostId = System.getProperty(TRACING_HOST_ID);
        try {
            Integer.parseInt(hostId);
        } catch (NumberFormatException nfe) {
            logger.warn(WARN_INVALID_HOSTID, hostId, DEFAULT_HOSTID);
            System.setProperty(TRACING_HOST_ID, DEFAULT_HOSTID);
        }
        setProperty(props, TRACING_HOST_NAME, DEFAULT_HOSTNAME);
    }

    private static void setProperty(final Properties props, final String propKey, final String defaultValue) {
        if (System.getProperty(propKey) == null) {
            String propValue = props.getProperty(propKey, defaultValue);
            if (propValue.isEmpty() || "null".compareTo(propValue) == 0) {
                propValue = defaultValue;
            }
            System.setProperty(propKey, propValue);
        }
    }
}
