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

package es.bsc.compss.util;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsPaths;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.tracing.CustomTraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.tracing.TraceScript;
import es.bsc.wdc.tracing.Event;
import es.bsc.wdc.tracing.EventType;
import es.bsc.wdc.tracing.TracingBackend;
import es.bsc.wdc.tracing.extrae.ExtraeTracer;
import es.bsc.wdc.tracing.monitor.MonitorTracer;
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

    // Errors
    private static final String ERROR_TRACE_DIR = "ERROR: Cannot create trace directory";

    // Logger
    protected static Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static boolean DEBUG = LOGGER.isDebugEnabled();

    // Configuration
    public static final boolean MONITOR_ENABLED = System.getProperty(COMPSsConstants.TRACING_MONITOR) != null
        && Boolean.parseBoolean(System.getProperty(COMPSsConstants.TRACING_MONITOR));

    public static final boolean EXTRAE_ENABLED = System.getProperty(COMPSsConstants.TRACING_EXTRAE) != null
        && Boolean.parseBoolean(System.getProperty(COMPSsConstants.TRACING_EXTRAE));

    private static final boolean ENABLED = EXTRAE_ENABLED || MONITOR_ENABLED;

    // Tracing script and file paths
    public static final String TRACE_SUBFOLDER = "trace";

    private static String TRACER_OUT_FILENAME = "tracer.out";
    private static String TRACER_ERR_FILENAME = "tracer.err";

    public static String PACKAGE_SUFFIX = "_compss_trace.tar.gz";

    private static String EXTRAE_FILE;
    private static String EXTRAE_OUTPUT_DIR;

    public static final int EVENT_END = 0;

    // Tracing configuration
    public static boolean tracerAlreadyLoaded = false;

    private static String nodeName;
    private static String installDir;
    private static String hostId;

    protected static boolean tracingTaskDependencies;
    // Hashmap of the predecessors
    private static HashMap<Integer, ArrayList<Integer>> predecessorsMap = new HashMap<>();

    private static AtomicInteger NEXT_HOST_ID = new AtomicInteger(1);
    private static Map<String, TraceHost> hostToSlots = new HashMap<>();

    private static int numPthreadsEnabled = 0;

    private static ArrayList<TracingBackend> BACKENDS = new ArrayList<>();

    static {
        String file = System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE);
        boolean customFile = (file != null) && !file.isEmpty() && file.compareTo("null") != 0;
        EXTRAE_FILE = customFile ? file : "null";

        String folder = System.getProperty(COMPSsConstants.EXTRAE_WORKING_DIR);
        boolean customFolder = (folder != null) && !folder.isEmpty() && folder.compareTo("null") != 0;
        folder = customFolder ? folder : ".";
        if (!folder.endsWith(File.separator)) {
            folder += File.separator;
        }
        EXTRAE_OUTPUT_DIR = folder;
    }


    /**
     * Returns if any kind of tracing is activated.
     *
     * @return true if tracing is activated
     */
    public static boolean isActivated() {
        return ENABLED;
    }

    /**
     * Returns if Extrae tracing is activated.
     *
     * @return true if tracing is activated
     */
    public static boolean isExtraeActivated() {
        return EXTRAE_ENABLED;
    }

    /**
     * Returns the host Id.
     *
     * @return The host Id.
     */
    public static String getHostID() {
        return Tracer.hostId;
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
        if (DEBUG) {
            LOGGER.debug("Tracing: Registering host " + name + " in the tracing system");
        }
        int id;
        synchronized (hostToSlots) {
            if (hostToSlots.containsKey(name)) {
                if (DEBUG) {
                    LOGGER.debug("Host " + name + " already in tracing system, skipping");
                }
                return -1;
            }
            id = NEXT_HOST_ID.getAndIncrement();
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
        if (DEBUG) {
            LOGGER.debug("Tracing: Getting slot " + slot + " of host " + host);
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
        if (DEBUG) {
            LOGGER.debug("Tracing: Freeing slot " + slot + " of host " + host);
        }
        hostToSlots.get(host).freeSlot(slot);
    }

    /**
     * Returns true if task dependencies tracing is activated.
     *
     * @return true or false
     */
    public static boolean isTracingTaskDependencies() {
        return tracingTaskDependencies;
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

    /**
     * Initializes tracer creating the trace folder. If tracing is used then the current node (master) sets its nodeID
     * (taskID) to 0, and its number of tasks to 1 (a single program).
     *
     * @param hostId id of the host
     * @param nodeName name of the node being traced
     * @param installDir Path to the installation directory
     * @param tracingTasks whether the tracing should add dependency-related events or not
     */
    public static void init(int hostId, String nodeName, String installDir, boolean tracingTasks) {
        if (tracerAlreadyLoaded) {
            if (DEBUG) {
                LOGGER.debug("Tracing already initialized.");
            }
            return;
        }
        tracerAlreadyLoaded = true;
        if (DEBUG) {
            LOGGER.debug("Initializing tracing: " + (ENABLED ? "Enabled" : "Disabled"));
        }

        Tracer.hostId = String.valueOf(hostId);
        LOGGER.debug("\t Tracing Host ID: " + Tracer.hostId);
        Tracer.nodeName = nodeName;
        Tracer.installDir = installDir;
        Tracer.tracingTaskDependencies = tracingTasks;
        LOGGER.debug("\t Task dependencies: " + (tracingTasks ? "Enabled" : "Disabled"));

        if (DEBUG) {
            LOGGER.debug("Initializing extrae tracing: " + (EXTRAE_ENABLED ? "Enabled" : "Disabled"));
        }
        if (EXTRAE_ENABLED) {
            String file = System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE);
            String folder = System.getProperty(COMPSsConstants.EXTRAE_WORKING_DIR);
            String extraeLib = installDir + COMPSsPaths.REL_DEPS_EXTRAE_DIR + "lib";
            ExtraeTracer extraeBE = new ExtraeTracer(hostId, file, folder, extraeLib);
            BACKENDS.add(extraeBE);
        }

        if (DEBUG) {
            LOGGER.debug("Initializing monitor tracing: " + (MONITOR_ENABLED ? "Enabled" : "Disabled"));
        }
        if (MONITOR_ENABLED) {
            String masterName = System.getProperty(COMPSsConstants.MASTER_NAME);
            MonitorTracer otelBE = new MonitorTracer(masterName, nodeName);
            BACKENDS.add(otelBE);
        }

        defineEvents();
    }

    /**
     * When using extrae's tracing, this call enables the instrumentation of ALL created threads from here onwards until
     * the same number (n) of disablePThreads is called.
     */
    public static void enablePThreads(int n) {
        synchronized (Tracer.class) {
            numPthreadsEnabled += n;
            if (numPthreadsEnabled > 0) {
                for (TracingBackend tb : BACKENDS) {
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
                for (TracingBackend tb : BACKENDS) {
                    tb.disablePThreads();
                }
            }
        }
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
        EventType type = new EventType() {

            @Override
            public final int getCode() {
                return code;
            }

            @Override
            public final String getDescription() {
                return description;
            }

            @Override
            public final boolean isEndable() {
                return endable;
            }

            @Override
            public final List<Event> getEvents() {
                return events;
            }
        };
        defineEventType(type);
        return type;
    }

    private static void defineEventType(EventType type) {
        for (TracingBackend tb : BACKENDS) {
            tb.defineEventType(type);
        }
    }

    /**
     * Defines a new Event that will be traced.
     *
     * @param eventType type of event
     * @param id id of the event within the type
     * @param signature signature of the event
     */
    public static void defineNewEvent(TraceEventType eventType, int id, String signature) {
        // Instantiates the event and automatically gets registered in the type
        new CustomTraceEvent(eventType, id, signature);
        defineEventType(eventType);
    }

    /**
     * Iterates over all the tracing events and sets them in the Wrapper to generate the config. for the tracefile.
     */
    private static void defineEvents() {
        for (TraceEventType type : TraceEventType.values()) {
            switch (type) {
                case TASKTYPE:
                    defineEventsForTaskType(type, MethodType.values());
                    break;
                default:
                    defineEventType(type);
            }
        }
    }

    private static void defineEventsForTaskType(TraceEventType type, MethodType[] types) {
        // Populate method's id for different task stype
        for (MethodType tp : types) {
            new CustomTraceEvent(type, tp.ordinal(), tp.name());
        }
        defineEventType(type);
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
        if (DEBUG) {
            LOGGER.debug("Emitting synchronized event [type, id] = [" + eventType + " , " + value + "]");
        }

        for (TracingBackend tb : BACKENDS) {
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
    public static void emitEventAndCounters(TraceEventType type, int value) {
        int eventType = type.code;
        if (DEBUG) {
            LOGGER.debug(
                "Emitting synchronized event with HW counters [type, taskId] = [" + eventType + " , " + value + "]");
        }

        for (TracingBackend tb : BACKENDS) {
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
    public static void emitEventEndAndCounters(TraceEventType type) {
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
        if (DEBUG) {
            LOGGER.debug("Emitting communication event [" + (send ? "SEND -> " : "REC <-") + partnerID + "]: " + id
                + ", " + tag + ", " + size);
        }
        for (TracingBackend tb : BACKENDS) {
            tb.emitCommunicationEvent(send, tag, size, partnerID, id);
        }
    }

    /**
     * End the extrae tracing system. Finishes master's tracing, generates both master and worker's packages, merges the
     * packages, and clean the intermediate traces.
     */
    public static void fini() {
        if (DEBUG) {
            LOGGER.debug("Tracing: finalizing");
        }
        for (TracingBackend tb : BACKENDS) {
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
        for (TracingBackend tb : BACKENDS) {
            tb.prepareSubProcessEnvironment(env, defineExtra);
        }
    }

    /**
     * Collects all the information of the tracing system and generates a trace.
     */
    public static void generateMasterPackage() {
        synchronized (Tracer.class) {
            if (EXTRAE_ENABLED) {
                String masterPackage = Tracer.EXTRAE_OUTPUT_DIR + "master" + PACKAGE_SUFFIX;
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
        if (DEBUG) {
            LOGGER.debug("[Tracer] Generating trace package of " + nodeName);
        }
        try {
            int exitCode = TraceScript.package_extrae(installDir, EXTRAE_OUTPUT_DIR, packagePath, hostId);
            if (exitCode != 0) {
                ErrorManager.warn("Error generating " + nodeName + " package, exit code " + exitCode);
            }
        } catch (IOException e) {
            ErrorManager.warn("Error generating " + nodeName + " package", e);

        } catch (InterruptedException e) {
            ErrorManager.warn("Error generating " + nodeName + " package (interruptedException)", e);
            Thread.currentThread().interrupt();
        }
    }

    public static String getTraceOutPath() {
        return EXTRAE_OUTPUT_DIR + TRACER_OUT_FILENAME;
    }

    public static String getTraceErrPath() {
        return EXTRAE_OUTPUT_DIR + TRACER_ERR_FILENAME;
    }

    /**
     * Returns the config file used for extrae.
     *
     * @return path of extrae config file
     */
    public static String getExtraeFile() {
        return EXTRAE_FILE;
    }

    /**
     * Returns the folder where extrae will leave the traces.
     *
     * @return path of extrae's output directory
     */
    public static String getExtraeOutputDir() {
        return EXTRAE_OUTPUT_DIR;
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

}
