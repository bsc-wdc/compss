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
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ExtraeTracer implements TracingBackend {

    // -------- Constants --------

    // Errors
    private static final String ERROR_TRACE_DIR = "ERROR: Cannot create trace directory";

    // Events
    public static final int SYNCH_EVENT_CODE = 8_000_001;
    private static final EventType SYNCH_EVENT_TYPE = new EventType() {

        @Override
        public int getCode() {
            return SYNCH_EVENT_CODE;
        }

        @Override
        public String getDescription() {
            return "Trace Synchronization event";
        }

        @Override
        public boolean isEndable() {
            return true;
        }

        @Override
        public List<Event> getEvents() {
            return new ArrayList<>(0);
        }
    };

    public static final int THREAD_EVENT_CODE = 8_001_003;
    private static final Map<Integer, HashSet<String>> COMPONENTS = new HashMap<>();
    private static final EventType THREAD_EVENT_TYPE = new EventType() {

        @Override
        public int getCode() {
            return THREAD_EVENT_CODE;
        }

        @Override
        public String getDescription() {
            return "Thread type identifier";
        }

        @Override
        public boolean isEndable() {
            return true;
        }

        @Override
        public List<Event> getEvents() {
            List<Event> events = new ArrayList<>(COMPONENTS.size());
            for (Map.Entry<Integer, HashSet<String>> e : COMPONENTS.entrySet()) {

                StringBuilder signature = new StringBuilder();
                Set<String> labels = e.getValue();
                Iterator<String> labelsItr = labels.iterator();
                while (labelsItr.hasNext()) {
                    signature.append(labelsItr.next());
                    if (labelsItr.hasNext()) {
                        signature.append(",");
                    }
                }
                events.add(new Event() {

                    @Override
                    public int getId() {
                        return e.getKey();
                    }

                    @Override
                    public String getSignature() {
                        return signature.toString();
                    }

                    @Override
                    public EventType getType() {
                        return THREAD_EVENT_TYPE;
                    }
                });
            }
            return events;
        }
    };

    // Errors

    // Logger
    protected static final Logger logger = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean debug = logger.isDebugEnabled();

    // Configuration
    private final String extraeLib;
    private final String workerExtraeFile;

    private final Set<EventType> eventTypes;


    /**
     * Constructs and sets up new tracer leveraging Extrae.
     *
     * @param hostId index of the host
     */
    ExtraeTracer(int hostId, String extraeLib, String outputDir, String workerExtraeFile) {
        this.extraeLib = extraeLib;
        this.workerExtraeFile = workerExtraeFile;

        eventTypes = new HashSet<>();
        eventTypes.add(SYNCH_EVENT_TYPE);
        eventTypes.add(THREAD_EVENT_TYPE);

        logger.debug("\t Extrae file: {}", workerExtraeFile);
        logger.debug("\t Tracing Output folder: {}", outputDir);

        File traceOutDir = new File(outputDir);
        if (!traceOutDir.exists()) {
            if (!new File(outputDir).mkdir()) {
                logger.error(ExtraeTracer.ERROR_TRACE_DIR);
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
    public final void activeComponent(int id, String description) {
        HashSet<String> labels = COMPONENTS.get(id);
        if (labels == null) {
            labels = new HashSet<>();
            COMPONENTS.put(id, labels);
        }
        labels.add(description);
        emitEvent(THREAD_EVENT_CODE, id);
    }

    @Override
    public final void inactiveComponent() {
        emitEvent(THREAD_EVENT_CODE, 0);
    }

    @Override
    public final void startSynch(long value) {
        emitEvent(SYNCH_EVENT_CODE, value);
    }

    @Override
    public final void endSynch() {
        emitEvent(SYNCH_EVENT_CODE, 0);
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
        for (Environment envVar : Environment.REMOVE_ENVIRONMENT_VARIABLES) {
            env.remove(envVar.name());
        }
        // Remove all unnecessary links to extrae
        for (Environment envVar : Environment.CLEAN_ENVIRONMENT_VARIABLES) {
            String envVarValue = System.getenv(envVar.name());
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
                env.put(envVar.name(), finalEnvVarValue);
            }
        }
        if (defineExtra) {
            env.put(Environment.EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE.name(), "1");
            env.put(Environment.EXTRAE_LIB.name(), extraeLib);
        }
    }

    @Override
    public String getWorkerTracingConfiguration() {
        return workerExtraeFile;
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
            if (debug) {
                logger.debug("Tracing[API]: Type " + type.getCode() + " Event " + offset + "=> value: " + values[offset]
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
            if (debug) {
                logger.debug("[ExtraeTracer] Initializing Wrapper.");
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
            logger.debug("[ExtraeTracer] Disabling pthreads");
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
            Wrapper.Fini();
            // End wrapper
            if (debug) {
                logger.debug("[ExtraeTracer] Finishing extrae");
            }
            Wrapper.SetOptions(Wrapper.EXTRAE_DISABLE_ALL_OPTIONS);
        }
    }

    /**
     * Shuts down tracing. Disables the instrumentation until the next call to Restart().
     */
    private void shutdownWrapper() {
        synchronized (Wrapper.class) {
            logger.debug("[ExtraeTracer] Shutdown");
            Wrapper.Shutdown();
        }
    }

    /**
     * Restart tracing. Resumes the instrumentation from the previous Shutdown() call.
     */
    private void restartWrapper() {
        synchronized (Wrapper.class) {
            logger.debug("[ExtraeTracer] Restart");
            Wrapper.Restart();
        }
    }

}
