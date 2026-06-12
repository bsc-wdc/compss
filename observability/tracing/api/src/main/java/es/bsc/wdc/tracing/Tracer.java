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

import es.bsc.wdc.tracing.util.TraceScript;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.atomic.AtomicInteger;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class Tracer {

    // --------- Constants --------
    public static final String PACKAGE_SUFFIX = "_compss_trace.tar.gz";

    private static final int EVENT_END = 0;

    // --------- Configuration --------
    private static boolean tracerAlreadyLoaded = false;

    // Logger
    protected static Logger logger = LogManager.getLogger(Loggers.TRACING);
    protected static boolean debug = logger.isDebugEnabled();

    // Tracing file paths
    private static final String outputDir;
    private static final String installDir;

    // Backends
    private static final boolean enabled;
    private static final boolean monitorEnabled;
    private static final boolean extraeEnabled;

    private static final ArrayList<TracingBackend> backends = new ArrayList<>();

    // Dependencies management
    private static final boolean trackTaskDependencies;
    private static final HashMap<Integer, ArrayList<Integer>> predecessorsMap = new HashMap<>();

    // Host Management
    private static final AtomicInteger nextHostID = new AtomicInteger(1);
    private static final Map<String, TraceHost> hostToSlots = new HashMap<>();

    private static final String nodeName;
    private static final String hostId;

    private static int numPthreadsEnabled = 0;

    static {
        boolean loadedProperties = false;
        try {
            ConfigManager.loadProperties();
            loadedProperties = true;
        } catch (ConfigException ce) {
            // TODO: This should disappear when tracer is installed as a separate package
            String compssHome = System.getenv("COMPSS_HOME");
            if (compssHome != null && !compssHome.isEmpty()) {
                logger.error("Defaulting to ${COMPSS_HOME}");
                System.setProperty(ConfigManager.TRACING_INSTALL_DIR, compssHome);
                try {
                    ConfigManager.loadProperties();
                    loadedProperties = true;
                } catch (ConfigException ce2) {
                    logger.error(ce2.getMessage());
                }
            } else {
                logger.error(ce.getMessage());
            }
        }
        if (loadedProperties) {
            installDir = System.getProperty(ConfigManager.TRACING_INSTALL_DIR);
            nodeName = System.getProperty(ConfigManager.TRACING_HOST_NAME);
            hostId = System.getProperty(ConfigManager.TRACING_HOST_ID);

            // Configuration
            String monitorEnabledStr = System.getProperty(ConfigManager.TRACING_MONITOR);
            monitorEnabled = Boolean.parseBoolean(monitorEnabledStr);

            String extraeEnabledStr = System.getProperty(ConfigManager.TRACING_EXTRAE);
            extraeEnabled = Boolean.parseBoolean(extraeEnabledStr);

            String folder = System.getProperty(ConfigManager.TRACING_WORKING_DIR);
            if (!folder.endsWith(File.separator)) {
                folder += File.separator;
            }
            outputDir = folder;

            String taskDependencies = System.getProperty(ConfigManager.TRACING_TASK_DEPENDENCIES);
            trackTaskDependencies = Boolean.parseBoolean(taskDependencies);

            enabled = Tracer.extraeEnabled || Tracer.monitorEnabled;
        } else {
            outputDir = "";
            installDir = "";
            monitorEnabled = false;
            extraeEnabled = false;
            trackTaskDependencies = false;
            nodeName = "master";
            hostId = "0";
            enabled = false;
        }
    }


    /**
     * Initializes tracer creating the trace folder.
     */
    public static void init() {
        logger.info("Initializing Tracer");
        if (tracerAlreadyLoaded) {
            if (debug) {
                logger.debug("Tracing already initialized.");
            }
            return;
        }
        tracerAlreadyLoaded = true;
        if (debug) {
            logger.debug("Initializing tracing: " + (enabled ? "Enabled" : "Disabled"));
        }

        logger.debug("\t Tracing Node name: " + nodeName);
        logger.debug("\t Tracing Host ID: " + hostId);
        int hostIdInt = Integer.parseInt(hostId);
        logger.debug("\t Task dependencies: " + (trackTaskDependencies ? "Enabled" : "Disabled"));

        if (debug) {
            logger.debug("Initializing extrae tracing: " + (extraeEnabled ? "Enabled" : "Disabled"));
        }
        if (extraeEnabled) {
            TracingBackendFactory bf = BackendFactoryRegistry.getTracingBackendFactory("extrae");
            logger.info("Loading Tracer backend for extrae {}", bf.toString());
            String config = System.getProperty(ConfigManager.CONFIG_LOCATION);
            TracingBackend be = bf.create(installDir, hostIdInt, nodeName, config);
            backends.add(be);
        }

        if (debug) {
            logger.debug("Initializing monitor tracing: " + (monitorEnabled ? "Enabled" : "Disabled"));
        }
        if (monitorEnabled) {
            TracingBackendFactory bf = BackendFactoryRegistry.getTracingBackendFactory("monitor");
            TracingBackend be = bf.create(installDir, hostIdInt, nodeName, null);
            backends.add(be);
        }
    }

    /**
     * Returns if any kind of tracing is activated.
     *
     * @return true if tracing is activated
     */
    public static boolean isActivated() {
        return enabled;
    }

    /**
     * Returns if Extrae tracing is activated.
     *
     * @return true if tracing is activated
     */
    public static boolean isExtraeActivated() {
        return extraeEnabled;
    }

    /**
     * Returns if monitor tracing is activated.
     *
     * @return true if monitor tracing is activated
     */
    public static boolean isMonitorActivated() {
        return monitorEnabled;
    }

    /**
     * Adds a host name and its number of slots to a hashmap required to later merge the traces from each host into a
     * single one.
     *
     * @param name of the host
     * @param slots number of threads the host is expected to have (used in GAT, in NIO is 0, because they will be
     *            computed automatically
     * @return the next ID to be used during the initialization of the tracing in the given host.
     */
    public static int registerHost(String name, int slots) {
        if (debug) {
            logger.debug("Tracing: Registering host " + name + " in the tracing system");
        }
        int id;
        synchronized (hostToSlots) {
            if (hostToSlots.containsKey(name)) {
                if (debug) {
                    logger.debug("Host " + name + " already in tracing system, skipping");
                }
                return -1;
            }
            id = nextHostID.getAndIncrement();
            hostToSlots.put(name, new TraceHost(slots));
        }
        return id;
    }

    /**
     * Returns the next slot ID (thread) that will run a task (GAT only).
     *
     * @param host that is going to execute a task
     * @return the next thread ID available to execute task (don't care about real order)
     */
    public static int getNextSlot(String host) {
        int slot = hostToSlots.get(host).getNextSlot();
        if (debug) {
            logger.debug("Tracing: Getting slot " + slot + " of host " + host);
        }
        return slot;
    }

    /**
     * Signals that a slot ID (thread) of a host is free again.
     *
     * @param host that is going to have a slot freed
     * @param slot to be freed
     */
    public static void freeSlot(String host, int slot) {
        if (debug) {
            logger.debug("Tracing: Freeing slot " + slot + " of host " + host);
        }
        hostToSlots.get(host).freeSlot(slot);
    }

    /**
     * Returns true if task dependencies tracing is activated.
     *
     * @return true or false
     */
    public static boolean isTracingTaskDependencies() {
        return trackTaskDependencies;
    }

    public static ArrayList<Integer> getPredecessors(int taskId) {
        return predecessorsMap.get(taskId);
    }

    public static void removePredecessor(int taskId) {
        predecessorsMap.remove(taskId);
    }

    /**
     * Adds id predecessors to list of predecessors.
     *
     * @param taskId Id of task
     * @param predecessorTaskId Id of predecessor task
     */
    public static void addPredecessors(int taskId, int predecessorTaskId) {
        ArrayList<Integer> predecessors = predecessorsMap.get(taskId);
        if (predecessors == null) {
            predecessors = new ArrayList<>();
        }
        predecessors.add(predecessorTaskId);
        predecessorsMap.put(taskId, predecessors);
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Trace Synchronization
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Registers that a synchronization between multiple processes and traces is going on.
     *
     * @param value value associated to the ongoing synchronization
     */
    public static void startSynchronization(long value) {
        for (TracingBackend tb : backends) {
            tb.startSynch(value);
        }
    }

    /**
     * Registers that an ongoing synchronization reaches its end.
     */
    public static void endSynchronization() {
        for (TracingBackend tb : backends) {
            tb.endSynch();
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Thread Management
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * When using extrae's tracing, this call enables the instrumentation of ALL created threads from here onwards until
     * the same number (n) of disablePThreads is called.
     */
    public static void enablePThreads(int n) {
        synchronized (Tracer.class) {
            numPthreadsEnabled += n;
            if (numPthreadsEnabled > 0) {
                for (TracingBackend tb : backends) {
                    tb.enablePThreads();
                }
            }
        }
    }

    /**
     * When using extrae's tracing, when n reaches the number of enablePThreads, this call disables the instrumentation
     * of any created threads from here onwards. To reactivate it use enablePThreads()
     */
    public static void disablePThreads(int n) {
        synchronized (Tracer.class) {
            numPthreadsEnabled -= n;
            if (numPthreadsEnabled < 1) {
                numPthreadsEnabled = 0;
                for (TracingBackend tb : backends) {
                    tb.disablePThreads();
                }
            }
        }
    }

    /**
     * Registers that the thread starts to run a component.
     *
     * @param id unique identifier associated to the component
     * @param description description of the component
     */
    public static void activeComponent(int id, String description) {
        for (TracingBackend tb : backends) {
            tb.activeComponent(id, description);
        }
    }

    /**
     * Registers that the thread ended running a component.
     */
    public static void inactiveComponent() {
        for (TracingBackend tb : backends) {
            tb.inactiveComponent();
        }
    }

    // ----------------------------------------------------------------------------------------------------------------
    // Event Management
    // ----------------------------------------------------------------------------------------------------------------

    /**
     * Constructs a new Event Type and defines it in the backends.
     *
     * @param code code of the event type
     * @param description description of the event type
     * @param endable {@literal true} if the type is endable
     * @param eventIDs set of ids under the umbrella of the type
     * @param eventLabels set of labels under the umbrella of the type
     */
    public static EventType defineNewEventType(int code, String description, boolean endable, int[] eventIDs,
        String[] eventLabels) {
        ArrayList<Event> events = new ArrayList<>(eventIDs.length);

        EventType type = new EventTypeImpl(code, description, endable, events);

        for (int i = 0; i < eventIDs.length; i++) {
            final int idx = i;
            events.add(new Event() {

                @Override
                public int getId() {
                    return eventIDs[idx];
                }

                @Override
                public String getSignature() {
                    return eventLabels[idx];
                }

                @Override
                public EventType getType() {
                    return type;
                }
            });
        }

        defineEventType(type);
        return type;
    }

    /**
     * Constructs a new Event Type and defines it in the backends.
     *
     * @param code code of the event type
     * @param description description of the event type
     * @param endable {@literal true} if the type is endable
     * @param events set of events under the umbrella of the type
     */
    public static EventType defineNewEventType(int code, String description, boolean endable, List<Event> events) {
        return new EventTypeImpl(code, description, endable, events);
    }

    /**
     * Constructs a new Event Type and defines it in the backends.
     *
     * @param code code of the event type
     * @param description description of the event type
     * @param endable {@literal true} if the type is endable
     * @param events set of events under the umbrella of the type
     */
    public static ExtensibleEventType defineNewExtensibleEventType(int code, String description, boolean endable,
        List<Event> events) {
        return new ExtensibleEventTypeImpl(code, description, endable, events);
    }

    private static void defineEventType(EventType type) {
        for (TracingBackend tb : backends) {
            tb.defineEventType(type);
        }
    }

    /**
     * Emits an event in all the configured tracers.
     *
     * @param event event being emitted
     */
    public static void emitEvent(Event event) {
        emitEvent(event.getType(), event.getId());
    }

    /**
     * Emits an event in all the configured tracers.
     *
     * @param type type of the event.
     * @param value ID of the event
     */
    public static void emitEvent(EventType type, long value) {
        int eventType = type.getCode();
        emitEvent(eventType, value);
    }

    /**
     * Emits an event in all the configured tracers.
     *
     * @param eventType type of the event.
     * @param value ID of the event
     */
    public static void emitEvent(int eventType, long value) {
        if (debug) {
            logger.debug("Emitting synchronized event [type, id] = [" + eventType + " , " + value + "]");
        }

        for (TracingBackend tb : backends) {
            tb.emitEvent(eventType, value);
        }
    }

    /**
     * Emits an event and the current PAPI counters activated using extrae's Wrapper. Requires that Tracer has been
     * initialized with lvl >0.
     *
     * @param type type of the event.
     * @param value ID of the event
     */
    public static void emitEventAndCounters(EventType type, int value) {
        int eventType = type.getCode();
        if (debug) {
            logger.debug(
                "Emitting synchronized event with HW counters [type, taskId] = [" + eventType + " , " + value + "]");
        }

        for (TracingBackend tb : backends) {
            tb.emitEventAndCounters(eventType, value);
        }
    }

    /**
     * Emits the end of an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param event event being emitted
     */
    public static void emitEventEnd(Event event) {
        emitEventEnd(event.getType());
    }

    /**
     * Emits the end of an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param type event being emitted
     */
    public static void emitEventEnd(EventType type) {
        final int typeCode = type.getCode();
        emitEvent(typeCode, EVENT_END);
    }

    /**
     * Emits the end of an event and the current PAPI counters activated using extrae's Wrapper. Requires that Tracer
     * has been initialized with lvl >0
     *
     * @param type event being emitted
     */
    public static void emitEventEndAndCounters(EventType type) {
        emitEventAndCounters(type, EVENT_END);
    }

    /**
     * Emits a new communication event.
     *
     * @param send Whether it is a send ({@literal true}) or a receive ({@literal false}) event.
     * @param partnerID ID the of the other partner involved in the communication.
     * @param id Id of the transfer.
     * @param tag Transfer tag.
     * @param size Transfer size.
     */
    public static void emitCommEvent(boolean send, int partnerID, int id, int tag, long size) {
        if (debug) {
            logger.debug("Emitting communication event [" + (send ? "SEND -> " : "REC <-") + partnerID + "]: " + id
                + ", " + tag + ", " + size);
        }
        for (TracingBackend tb : backends) {
            tb.emitCommunicationEvent(send, tag, size, partnerID, id);
        }
    }

    /**
     * End the extrae tracing system. Finishes master's tracing, generates both master and worker's packages, merges the
     * packages, and clean the intermediate traces.
     */
    public static void fini() {
        if (debug) {
            logger.debug("Tracing: finalizing");
        }
        for (TracingBackend tb : backends) {
            tb.fini();
        }
    }

    /**
     * Checks and updates the environment variables for tracing.
     *
     * @param env Environment to prepare for tracing.
     * @param defineExtra Add extra variables.
     */
    public static void prepareSubProcessEnvironment(Map<String, String> env, Boolean defineExtra) {
        for (TracingBackend tb : backends) {
            tb.prepareSubProcessEnvironment(env, defineExtra);
        }
    }

    /**
     * Returns the configuration of the remote Worker's tracing system. Currently, this configuration is only the config
     * file for extrae (if enabled)
     *
     * @return configuration of the remote Worker's tracing system
     */
    public static String getWorkerTracingConfiguration() {
        String config = null;
        for (TracingBackend tb : backends) {
            String tbConfig = tb.getWorkerTracingConfiguration();
            // Currently, only Extrae backend may return something.
            if (tbConfig != null) {
                config = tbConfig;
            }
        }
        return config;
    }

    /**
     * Collects all the information of the tracing system and generates a trace.
     */
    public static void generateMasterPackage() {
        synchronized (Tracer.class) {
            if (extraeEnabled) {
                String masterPackage = Tracer.outputDir + "master" + PACKAGE_SUFFIX;
                generatePackage(masterPackage);
            }
        }
    }

    /**
     * Constructs a package with all the necessary tracing information related to the node.
     *
     * @param packagePath Path where to store the package with all the tracing information
     */
    public static void generatePackage(String packagePath) {
        if (!extraeEnabled) {
            return;
        }

        if (debug) {
            logger.debug("[Tracer] Generating trace package of " + nodeName);
        }
        try {
            int exitCode = TraceScript.gen_package(installDir, outputDir, packagePath, hostId);
            if (exitCode != 0) {
                logger.warn("Error generating " + nodeName + " package, exit code " + exitCode);
            }
        } catch (IOException e) {
            logger.warn("Error generating " + nodeName + " package", e);

        } catch (InterruptedException e) {
            logger.warn("Error generating " + nodeName + " package (interruptedException)", e);
            Thread.currentThread().interrupt();
        }
    }

    /**
     * Returns the folder where extrae will leave the traces.
     *
     * @return path of extrae's output directory
     */
    public static String getTracerOutputDir() {
        return outputDir;
    }


    private static class TraceHost {

        private boolean[] slots;
        private int numFreeSlots;
        private int nextSlot;


        private TraceHost(int nslots) {
            this.slots = new boolean[nslots];
            this.numFreeSlots = nslots;
            this.nextSlot = 0;
        }

        private int getNextSlot() {
            if (numFreeSlots-- > 0) {
                while (slots[nextSlot]) {
                    nextSlot = (nextSlot + 1) % slots.length;
                }
                slots[nextSlot] = true;
                return nextSlot;
            } else {
                return -1;
            }
        }

        private void freeSlot(int slot) {
            slots[slot] = false;
            nextSlot = slot;
            numFreeSlots++;
        }
    }

    private static class EventTypeImpl implements EventType {

        private final int code;
        private final String description;
        private final boolean endable;
        private final List<Event> events;


        private EventTypeImpl(int code, String description, boolean endable, List<Event> events) {
            this.code = code;
            this.description = description;
            this.endable = endable;
            this.events = events;
            defineEventType(this);
        }

        @Override
        public int getCode() {
            return code;
        }

        @Override
        public String getDescription() {
            return description;
        }

        @Override
        public boolean isEndable() {
            return endable;
        }

        @Override
        public List<Event> getEvents() {
            return events;
        }
    }

    private static class ExtensibleEventTypeImpl extends EventTypeImpl implements ExtensibleEventType {

        private ExtensibleEventTypeImpl(int code, String description, boolean endable, List<Event> events) {
            super(code, description, endable, events);
        }

        @Override
        public void addEvent(Event event) {
            super.getEvents().add(event);
            defineEventType(this);
        }
    }
}
