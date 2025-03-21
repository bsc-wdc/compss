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

package es.bsc.compss.util;

import es.bsc.cepbatools.extrae.Wrapper;
import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsPaths;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.tracing.TraceScript;
import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Tracer {

    // Logger
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String ERROR_TRACE_DIR = "ERROR: Cannot create trace directory";

    // Tracing script and file paths
    public static final String TRACE_SUBFOLDER = "trace";

    private static final String TRACER_OUT_FILENAME = "tracer.out";
    private static final String TRACER_ERR_FILENAME = "tracer.err";

    public static final String PACKAGE_SUFFIX = "_compss_trace.tar.gz";

    private static final String EXTRAE_FILE;
    private static final String EXTRAE_OUTPUT_DIR;

    private static final String ENV_EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE = "EXTRAE_SKIP_AUTO_LIBRARY_INITIALIZE";
    private static final String ENV_EXTRAE_LIB = "EXTRAE_LIB";

    // Extrae environment variables to be removed when tracing
    public static final String[] REMOVE_ENVIRONMENT_VARIABLES = new String[] { "EXTRAE_CONFIG_FILE",
        "EXTRAE_USE_POSIX_CLOCK" };
    // Extrae environment variables
    public static final String[] CLEAN_ENVIRONMENT_VARIABLES = new String[] { "LD_PRELOAD" };

    public static final int EVENT_END = 0;

    private static final AtomicInteger NEXT_HOST_ID = new AtomicInteger(1);

    // Tracing configuration
    public static boolean tracerAlreadyLoaded = false;
    private static boolean enabled = false;
    private static String nodeName;
    private static String installDir;
    private static String hostId;
    protected static boolean tracingTaskDependencies;

    private static final Map<String, TraceHost> hostToSlots = new HashMap<>();

    private static int numPthreadsEnabled = 0;

    // Hashmap of the predecessors
    private static final HashMap<Integer, ArrayList<Integer>> predecessorsMap = new HashMap<>();

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
     * Checks and updates the environment variables for tracing. In particular, removes the content of the variables
     * defined in REMOVE_ENVIRONMENT_VARIABLES and also removes any link to extrae from CLEAN_ENVIRONMENT_VARIABLES.
     *
     * @param env Environment to prepare for tracing.
     * @param defineExtra Add extra variables.
     */
    public static void prepareEnvironment(Map<String, String> env, Boolean defineExtra) {
        // Remove all unnecessary environment variables.
        for (String envVar : Tracer.REMOVE_ENVIRONMENT_VARIABLES) {
            env.remove(envVar);
        }
        // Remove all unnecessary links to extrae
        for (String envVar : Tracer.CLEAN_ENVIRONMENT_VARIABLES) {
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
            env.put(ENV_EXTRAE_LIB, installDir + COMPSsPaths.REL_DEPS_EXTRAE_DIR + "lib");
        }
    }

    /**
     * Initializes tracer creating the trace folder.If tracing is used then the current node (master) sets its nodeID
     * (taskID) to 0, and its number of tasks to 1 (a single program).
     *
     * @param enabled whether the tracing should be enabled or not
     * @param hostId id of the host
     * @param nodeName name of the node being traced
     * @param installDir Path to the installation directory
     * @param tracingTasks whether the tracing should add dependency-related events or not
     */
    public static void init(boolean enabled, int hostId, String nodeName, String installDir, boolean tracingTasks) {
        if (tracerAlreadyLoaded) {
            if (DEBUG) {
                LOGGER.debug("Tracing already initialized.");
            }
            return;
        }
        tracerAlreadyLoaded = true;
        Tracer.enabled = enabled;
        Tracer.nodeName = nodeName;
        Tracer.installDir = installDir;

        if (DEBUG) {
            LOGGER.debug("Initializing tracing: " + (enabled ? "Enabled" : "Disabled"));
        }

        if (enabled) {
            Tracer.hostId = String.valueOf(hostId);
            LOGGER.debug("\t Tracing Host ID: " + Tracer.hostId);
            tracingTaskDependencies = tracingTasks;
            LOGGER.debug("\t Task dependencies: " + (tracingTasks ? "Enabled" : "Disabled"));

            LOGGER.debug("\t Extrae file: " + Tracer.EXTRAE_FILE);
            LOGGER.debug("\t Tracing Ouput folder: " + Tracer.EXTRAE_OUTPUT_DIR);
            File traceOutDir = new File(Tracer.EXTRAE_OUTPUT_DIR);
            if (!traceOutDir.exists()) {
                if (!new File(Tracer.EXTRAE_OUTPUT_DIR).mkdir()) {
                    ErrorManager.error(ERROR_TRACE_DIR);
                }
            }
            setUpWrapper(hostId, hostId + 1);
        }
    }

    /**
     * Returns the host Id.
     *
     * @return The host Id.
     */
    public static String getHostID() {
        return Tracer.hostId;
    }

    public static String getTraceOutPath() {
        return EXTRAE_OUTPUT_DIR + TRACER_OUT_FILENAME;
    }

    public static String getTraceErrPath() {
        return EXTRAE_OUTPUT_DIR + TRACER_ERR_FILENAME;
    }

    /**
     * Initialized the Extrae wrapper.
     *
     * @param taskId taskId of the node
     * @param numTasks num of tasks for that node
     */
    private static void setUpWrapper(int taskId, int numTasks) {
        synchronized (Tracer.class) {
            if (DEBUG) {
                LOGGER.debug("Initializing extrae Wrapper.");
            }
            Wrapper.SetTaskID(taskId);
            Wrapper.SetNumTasks(numTasks);
        }
    }

    /**
     * Returns if any kind of tracing is activated.
     *
     * @return true if tracing is activated
     */
    public static boolean isActivated() {
        return Tracer.enabled;
    }

    /**
     * Returns true if task dependencies tracing is activated.
     *
     * @return true or false
     */
    public static boolean isTracingTaskDependencies() {

        return tracingTaskDependencies;
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

    /**
     * When using extrae's tracing, this call enables the instrumentation of ALL created threads from here onwards until
     * the same number (n) of disablePThreads is called.
     */
    public static void enablePThreads(int n) {
        synchronized (Tracer.class) {
            numPthreadsEnabled += n;
            if (numPthreadsEnabled > 0) {
                Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS);
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
                Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
            }
        }
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
     * Emits an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param event event being emitted
     */
    public static void emitEvent(TraceEvent event) {
        emitEvent(event.getType(), event.getId());
    }

    /**
     * Emits an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param type type of the event.
     * @param value ID of the event
     */
    public static final void emitEvent(TraceEventType type, long value) {
        int eventType = type.code;
        emitEvent(eventType, value);
    }

    /**
     * Emits an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param eventType type of the event.
     * @param value ID of the event
     */
    public static final void emitEvent(int eventType, long value) {
        if (DEBUG) {
            LOGGER.debug("Emitting synchronized event [type, id] = [" + eventType + " , " + value + "]");
        }
        synchronized (Tracer.class) {
            Wrapper.Event(eventType, value);
        }
    }

    /**
     * Emits an event and the current PAPI counters activated using extrae's Wrapper. Requires that Tracer has been
     * initialized with lvl >0.
     *
     * @param type type of the event.
     * @param value ID of the event
     */
    public static final void emitEventAndCounters(TraceEventType type, int value) {
        int eventType = type.code;
        if (DEBUG) {
            LOGGER.debug(
                "Emitting synchronized event with HW counters [type, taskId] = [" + eventType + " , " + value + "]");
        }
        synchronized (Tracer.class) {
            Wrapper.Eventandcounters(eventType, value);
        }
    }

    /**
     * Emits the end of an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param event event being emitted
     */
    public static final void emitEventEnd(TraceEvent event) {
        emitEventEnd(event.getType());
    }

    /**
     * Emits the end of an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param type event being emitted
     */
    public static final void emitEventEnd(TraceEventType type) {
        final int typeCode = type.code;
        emitEvent(typeCode, EVENT_END);
    }

    /**
     * Emits the end of an event and the current PAPI counters activated using extrae's Wrapper. Requires that Tracer
     * has been initialized with lvl >0
     *
     * @param type event being emitted
     */
    public static final void emitEventEndAndCounters(TraceEventType type) {
        emitEventAndCounters(type, EVENT_END);
    }

    /**
     * Emits a new communication event.
     *
     * @param send Whether it is a send event or not.
     * @param ownID Transfer own Id.
     * @param partnerID Transfer partner Id.
     * @param tag Transfer tag.
     * @param size Transfer size.
     */
    public static void emitCommEvent(boolean send, int ownID, int partnerID, int tag, long size) {
        synchronized (Tracer.class) {
            Wrapper.Comm(send, tag, (int) size, partnerID, ownID);
        }

        if (DEBUG) {
            LOGGER.debug("Emitting communication event [" + (send ? "SEND" : "REC") + "] " + tag + ", " + size + ", "
                + partnerID + ", " + ownID + "]");
        }
    }

    /**
     * End the extrae tracing system. Finishes master's tracing, generates both master and worker's packages, merges the
     * packages, and clean the intermediate traces.
     *
     * @param runtimeEvents label-Id pairs for the runtimeEvents
     */
    public static void fini(Map<String, Integer> runtimeEvents) {
        if (DEBUG) {
            LOGGER.debug("Tracing: finalizing");
        }

        synchronized (Tracer.class) {
            if (enabled) {
                defineEvents(runtimeEvents);
                Tracer.stopWrapper();
            }
        }
    }

    /**
     * Stops the extrae wrapper.
     */
    private static void stopWrapper() {
        synchronized (Tracer.class) {
            LOGGER.debug("[Tracer] Disabling pthreads");
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
            Wrapper.Fini();
            // End wrapper
            if (DEBUG) {
                LOGGER.debug("[Tracer] Finishing extrae");
            }
            Wrapper.SetOptions(Wrapper.EXTRAE_DISABLE_ALL_OPTIONS);
        }
    }

    /**
     * Collects all the information of the tracing system and generates a trace.
     */
    public static void generateMasterPackage() {
        synchronized (Tracer.class) {
            if (enabled) {
                String masterPackage = Tracer.EXTRAE_OUTPUT_DIR + "master" + PACKAGE_SUFFIX;
                generatePackage(masterPackage);
            }
        }
    }

    /**
     * Iterates over all the tracing events and sets them in the Wrapper to generate the config. for the tracefile.
     *
     * @param runtimeEvents label-Id pairs for the runtimeEvents
     */
    private static void defineEvents(Map<String, Integer> runtimeEvents) {
        if (DEBUG) {
            LOGGER.debug("SignatureToId size: " + runtimeEvents.size());
        }

        for (TraceEventType type : TraceEventType.values()) {
            switch (type) {
                case TASKS_FUNC:
                    defineEventsForFunctions(type, runtimeEvents);
                    break;
                case BINDING_TASKS_FUNC:
                    // defineEventsForFunctions(type, runtimeEvents);
                    break;
                case TASKTYPE:
                    defineEventsForTaskType(type, MethodType.values());
                    break;
                default:
                    defineEventsForType(type);
            }
        }
    }

    private static void defineEventsForTaskType(TraceEventType type, MethodType[] types) {
        int size = types.length + 1;
        long[] values = new long[size];
        String[] descriptionValues = new String[size];
        values[0] = 0;
        descriptionValues[0] = "End";
        int i = 1;
        for (MethodType tp : types) {
            values[i] = tp.ordinal() + 1L;
            descriptionValues[i] = tp.name();
            ++i;
        }
        Wrapper.defineEventType(type.code, type.desc, values, descriptionValues);

    }

    private static void defineEventsForFunctions(TraceEventType type, Map<String, Integer> runtimeEvents) {
        int size = runtimeEvents.entrySet().size() + 1;
        long[] values = new long[size];
        String[] descriptionValues = new String[size];
        values[0] = 0;
        descriptionValues[0] = "End";
        int i = 1;
        for (Entry<String, Integer> entry : runtimeEvents.entrySet()) {
            String signature = entry.getKey();
            Integer methodId = entry.getValue();
            values[i] = methodId + 1L;
            LOGGER.debug("Tracing debug: " + signature);
            String methodName = signature.substring(signature.indexOf('.') + 1, signature.length());
            String mN = methodName.replace("(", "([").replace(")", "])");
            if (mN.contains(".")) {
                int start = mN.lastIndexOf(".");
                mN = "[" + mN.substring(0, start) + ".]" + mN.substring(start + 1);
            }
            descriptionValues[i] = mN;
            if (DEBUG) {
                LOGGER.debug("Tracing Funtion Event [i,methodId]: [" + i + "," + methodId + "] => value: " + values[i]
                    + ", Desc: " + descriptionValues[i]);
            }
            i++;
        }

        Wrapper.defineEventType(type.code, type.desc, values, descriptionValues);
    }

    private static void defineEventsForType(TraceEventType type) {
        boolean endable = type.endable;
        List<TraceEvent> events = type.getEvents();

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
        for (TraceEvent event : events) {
            values[offset] = event.getId();
            descriptions[offset] = event.getSignature();
            if (DEBUG) {
                LOGGER.debug("Tracing[API]: Type " + type.code + " Event " + offset + "=> value: " + values[offset]
                    + ", Desc: " + descriptions[offset]);
            }
            offset++;
        }
        Wrapper.defineEventType(type.code, type.desc, values, descriptions);
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
