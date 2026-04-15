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

import es.bsc.cepbatools.extrae.Wrapper;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.Loggers;
import es.bsc.wdc.tracing.TracingBackend;
import java.io.File;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExtraeTracer implements TracingBackend {

    // Configuration
    private final String extraeLib;
    private final String extraeFile;
    private final String extraeOuputDir;

    private final Set<EventType> eventTypes;

    // Errors
    private static final String ERROR_TRACE_DIR = "ERROR: Cannot create trace directory";

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Environment Variables
    private static final String ENV_EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE = "EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE";
    private static final String ENV_EXTRAE_LIB = "EXTRAE_LIB";

    // Extrae environment variables to be removed when tracing
    public static final String[] REMOVE_ENVIRONMENT_VARIABLES = new String[] { "EXTRAE_CONFIG_FILE",
        "EXTRAE_USE_POSIX_CLOCK" };
    // Extrae environment variables
    public static final String[] CLEAN_ENVIRONMENT_VARIABLES = new String[] { "LD_PRELOAD" };


    /**
     * Constructs and sets up new tracer leveraging Extrae.
     *
     * @param hostId index of the host
     * @param file path of the extrae configuration file
     * @param folder path of the extrae's working directory
     * @param extraelib path of the extrae's library
     */
    public ExtraeTracer(int hostId, String file, String folder, String extraelib) {
        this.extraeLib = extraelib;
        eventTypes = new HashSet<>();
        boolean customFile = (file != null) && !file.isEmpty() && file.compareTo("null") != 0;
        this.extraeFile = customFile ? file : "null";
        LOGGER.debug("\t Extrae file: " + this.extraeFile);

        boolean customFolder = (folder != null) && !folder.isEmpty() && folder.compareTo("null") != 0;
        folder = customFolder ? folder : ".";
        if (!folder.endsWith(File.separator)) {
            folder += File.separator;
        }
        this.extraeOuputDir = folder;
        LOGGER.debug("\t Tracing Output folder: " + this.extraeOuputDir);

        File traceOutDir = new File(this.extraeOuputDir);
        if (!traceOutDir.exists()) {
            if (!new File(this.extraeOuputDir).mkdir()) {
                LOGGER.error(ExtraeTracer.ERROR_TRACE_DIR);
            }
        }
        setUpWrapper(hostId, hostId + 1);
    }

    @Override
    public final void enablePThreads() {
        synchronized (Wrapper.class) {
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS);
        }
    }

    @Override
    public final void disablePThreads() {
        synchronized (Wrapper.class) {
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
        }
    }

    @Override
    public void defineEventType(EventType type) {
        eventTypes.add(type);
    }

    @Override
    public void emitEvent(int eventType, long value) {
        synchronized (Wrapper.class) {
            Wrapper.Event(eventType, value);
        }
    }

    @Override
    public void emitEventAndCounters(int eventType, long value) {
        synchronized (Wrapper.class) {
            Wrapper.Eventandcounters(eventType, value);
        }
    }

    @Override
    public void emitCommunicationEvent(boolean send, int tag, long size, int partnerID, int commId) {
        synchronized (Wrapper.class) {
            Wrapper.Comm(send, tag, (int) size, partnerID, commId);
        }
    }

    @Override
    public void fini() {
        synchronized (ExtraeTracer.class) {
            for (EventType et : this.eventTypes) {
                defineEventsForType(et);
            }
            stopWrapper();
        }
    }

    @Override
    public void prepareSubProcessEnvironment(Map<String, String> env, Boolean defineExtra) {
        // It removes the content of the variables defined in REMOVE_ENVIRONMENT_VARIABLES and also removes any link to
        // extrae from CLEAN_ENVIRONMENT_VARIABLES.
        // Remove all unnecessary environment variables.
        for (String envVar : ExtraeTracer.REMOVE_ENVIRONMENT_VARIABLES) {
            env.remove(envVar);
        }
        // Remove all unnecessary links to extrae
        for (String envVar : ExtraeTracer.CLEAN_ENVIRONMENT_VARIABLES) {
            String envVarValue = System.getenv(envVar);
            if (envVarValue != null) {
                String[] envVarValuePaths = envVarValue.split(":");
                List<String> finalEnvVarValuePaths = new ArrayList<String>();
                // Look for paths that do not contain extrae
                for (String valuePath : envVarValuePaths) {
                    if (!valuePath.toLowerCase().contains("extrae")) {
                        finalEnvVarValuePaths.add(valuePath);
                    }
                }
                String finalEnvVarValue = String.join(":", finalEnvVarValuePaths);
                // Update the environment variable without extrae paths
                env.put(envVar, finalEnvVarValue);
            }
        }
        if (defineExtra) {
            env.put(ENV_EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE, "1");
            env.put(ENV_EXTRAE_LIB, extraeLib);
        }
    }

    private static void defineEventsForType(EventType type) {
        boolean endable = type.isEndable();
        List<Event> events = type.getEvents();

        long[] values;
        String[] descriptions;
        int size = events.size();
        int offset = 0;
        if (endable) {
            values = new long[size + 1];
            values[0] = 0;
            descriptions = new String[size + 1];
            descriptions[0] = "End";
            offset = 1;
        } else {
            values = new long[size];
            descriptions = new String[size];
        }
        for (Event event : events) {
            values[offset] = event.getId();
            descriptions[offset] = event.getSignature();
            if (DEBUG) {
                LOGGER.debug("Tracing[API]: Type " + type.getCode() + " Event " + offset + "=> value: " + values[offset]
                    + ", Desc: " + descriptions[offset]);
            }
            offset++;
        }
        Wrapper.defineEventType(type.getCode(), type.getDescription(), values, descriptions);
    }

    /**
     * Initialized the Extrae wrapper.
     *
     * @param taskId taskId of the node
     * @param numTasks num of tasks for that node
     */
    private void setUpWrapper(int taskId, int numTasks) {
        synchronized (Wrapper.class) {
            if (DEBUG) {
                LOGGER.debug("[ExtraeTracer] Initializing Wrapper.");
            }
            Wrapper.SetTaskID(taskId);
            Wrapper.SetNumTasks(numTasks);
        }
    }

    /**
     * Stops the extrae wrapper.
     */
    private void stopWrapper() {
        synchronized (Wrapper.class) {
            LOGGER.debug("[ExtraeTracer] Disabling pthreads");
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
            Wrapper.Fini();
            // End wrapper
            if (DEBUG) {
                LOGGER.debug("[ExtraeTracer] Finishing extrae");
            }
            Wrapper.SetOptions(Wrapper.EXTRAE_DISABLE_ALL_OPTIONS);
        }
    }

    /**
     * Shuts down tracing. Disables the instrumentation until the next call to Restart().
     */
    private void shutdownWrapper() {
        synchronized (Wrapper.class) {
            LOGGER.debug("[ExtraeTracer] Shutdown");
            Wrapper.Shutdown();
        }
    }

    /**
     * Restart tracing. Resumes the instrumentation from the previous Shutdown() call.
     */
    private void restartWrapper() {
        synchronized (Wrapper.class) {
            LOGGER.debug("[ExtraeTracer] Restart");
            Wrapper.Restart();
        }
    }

}
