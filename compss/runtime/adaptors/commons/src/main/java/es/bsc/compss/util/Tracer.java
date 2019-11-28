/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.data.location.ProtocolType;
import es.bsc.compss.types.uri.SimpleURI;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;
import java.util.HashMap;
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
    private static final String ERROR_MASTER_PACKAGE_FILEPATH =
        "Cannot locate master tracing package " + "on working directory";

    // Tracing script and file paths
    protected static final String TRACE_SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts"
        + File.separator + "system" + File.separator + "trace" + File.separator + "trace.sh";
    protected static final String TRACE_OUT_RELATIVE_PATH = File.separator + "trace" + File.separator + "tracer.out";
    protected static final String TRACE_ERR_RELATIVE_PATH = File.separator + "trace" + File.separator + "tracer.err";

    // Extrae loaded properties
    private static final boolean IS_CUSTOM_EXTRAE_FILE =
        (System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) != null)
            && !System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE).isEmpty()
            && !System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE).equals("null");
    private static final String EXTRAE_FILE =
        IS_CUSTOM_EXTRAE_FILE ? System.getProperty(COMPSsConstants.EXTRAE_CONFIG_FILE) : "null";

    // Extrae environment flags
    public static final String LD_PRELOAD = "LD_PRELOAD";
    public static final String EXTRAE_CONFIG_FILE = "EXTRAE_CONFIG_FILE";

    // Description tags for Paraver
    private static final String TASK_DESC = "Task";
    private static final String API_DESC = "Runtime";
    private static final String TASKID_DESC = "Task IDs";
    private static final String DATA_TRANSFERS_DESC = "Data Transfers";
    private static final String TASK_TRANSFERS_DESC = "Task Transfers Request";
    private static final String STORAGE_DESC = "Storage API";
    private static final String INSIDE_TASK_DESC = "Events inside tasks";

    // Event codes
    protected static final int TASKS_FUNC_TYPE = 8_000_000;
    protected static final int RUNTIME_EVENTS = 8_000_001;
    protected static final int TASKS_ID_TYPE = 8_000_002;
    protected static final int TASK_TRANSFERS = 8_000_003;
    protected static final int DATA_TRANSFERS = 8_000_004;
    protected static final int STORAGE_TYPE = 8_000_005;
    protected static final int READY_COUNTS = 8_000_006;
    protected static final int SYNC_TYPE = 8_000_666;
    protected static final int INSIDE_TASKS_TYPE = 60_000_100;

    public static final int EVENT_END = 0;

    // Tracing modes
    public static final int BASIC_MODE = 1;
    public static final int SCOREP_MODE = -1;
    public static final int MAP_MODE = -2;

    protected static int tracingLevel = 0;
    private static String traceDirPath;
    private static Map<String, TraceHost> hostToSlots;
    private static AtomicInteger hostId;


    /**
     * Initializes tracer creating the trace folder. If extrae's tracing is used (level > 0) then the current node
     * (master) sets its nodeID (taskID in extrae) to 0, and its number of tasks to 1 (a single program).
     *
     * @param logDirPath Path to the log directory
     * @param level type of tracing: -3: arm-ddt, -2: arm-map, -1: scorep, 0: off, 1: extrae-basic, 2: extrae-advanced
     */
    public static void init(String logDirPath, int level) {
        if (DEBUG) {
            LOGGER.debug("Initializing tracing with level " + level);
        }

        hostId = new AtomicInteger(1);
        hostToSlots = new HashMap<>();

        if (!logDirPath.endsWith(File.separator)) {
            logDirPath += logDirPath;
        }
        traceDirPath = logDirPath + "trace" + File.separator;
        if (!new File(traceDirPath).mkdir()) {
            ErrorManager.error(ERROR_TRACE_DIR);
        }

        tracingLevel = level;

        if (Tracer.extraeEnabled()) {
            setUpWrapper(0, 1);
        } else {
            if (Tracer.scorepEnabled()) {
                if (DEBUG) {
                    LOGGER.debug("Initializing scorep.");
                }
            } else {
                if (Tracer.mapEnabled()) {
                    if (DEBUG) {
                        LOGGER.debug("Initializing arm-map.");
                    }
                }
            }
        }
    }

    /**
     * Initialized the Extrae wrapper.
     *
     * @param taskId taskId of the node
     * @param numTasks num of tasks for that node
     */
    protected static void setUpWrapper(int taskId, int numTasks) {
        synchronized (Tracer.class) {
            if (DEBUG) {
                LOGGER.debug("Initializing extrae Wrapper.");
            }
            Wrapper.SetTaskID(taskId);
            Wrapper.SetNumTasks(numTasks);
        }
    }

    /**
     * Returns if the current execution is being instrumented by extrae.
     *
     * @return true if currently instrumented by extrae
     */
    public static boolean extraeEnabled() {
        return tracingLevel > 0;
    }

    /**
     * Returns if the current execution is being instrumented by scorep.
     *
     * @return true if currently instrumented by scorep
     */
    public static boolean scorepEnabled() {
        return tracingLevel == Tracer.SCOREP_MODE;
    }

    /**
     * Returns if the current execution is being instrumented by arm-map.
     *
     * @return true if currently instrumented by arm-map
     */
    public static boolean mapEnabled() {
        return tracingLevel == Tracer.MAP_MODE;
    }

    /**
     * Returns if any kind of tracing is activated including ddt, map, scorep, or extrae).
     *
     * @return true if any kind of tracing is activated
     */
    public static boolean isActivated() {
        return tracingLevel != 0;
    }

    /**
     * Returns whether extrae is working and is activated in basic mode.
     *
     * @return true if extrae is enabled in basic mode
     */
    public static boolean basicModeEnabled() {
        return tracingLevel == Tracer.BASIC_MODE;
    }

    /**
     * Returns with which tracing level the Tracer has been initialized (0 if it's not active).
     *
     * @return int with tracing level (in [-3, -2, -1, 0, 1, 2])
     */
    public static int getLevel() {
        return tracingLevel;
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
     * When using extrae's tracing, this call enables the instrumentation of ALL created threads from here onwards. To
     * deactivate it use disablePThreads().
     */
    public static void enablePThreads() {
        synchronized (Tracer.class) {
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS);
        }
    }

    /**
     * When using extrae's tracing, this call disables the instrumentation of any created threads from here onwards. To
     * reactivate it use enablePThreads()
     */
    public static void disablePThreads() {
        synchronized (Tracer.class) {
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
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
            id = hostId.getAndIncrement();
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

    public static int getRuntimeEventsType() {
        return RUNTIME_EVENTS;
    }

    public static int getSyncType() {
        return SYNC_TYPE;
    }

    public static int getTaskTransfersType() {
        return TASK_TRANSFERS;
    }

    public static int getDataTransfersType() {
        return DATA_TRANSFERS;
    }

    public static int getTaskEventsType() {
        return TASKS_FUNC_TYPE;
    }

    public static int getTaskSchedulingType() {
        return TASKS_ID_TYPE;
    }

    public static int getInsideTasksEventsType() {
        return INSIDE_TASKS_TYPE;
    }

    public static TraceEvent getAcessProcessorRequestEvent(String eventType) {
        return TraceEvent.valueOf(eventType);
    }

    /**
     * Returns the corresponding event ID for a TD request event type.
     *
     * @param eventType of the TD
     * @return the tracing event ID associated with eventType
     */
    public static TraceEvent getTaskDispatcherRequestEvent(String eventType) {
        TraceEvent event = null;
        try {
            event = TraceEvent.valueOf(eventType);
        } catch (Exception e) {
            LOGGER.error("Task Dispatcher event " + eventType + " is not present in Tracer's list ");
        }
        return event;
    }

    /**
     * Emits an event using extrae's Wrapper. Requires that Tracer has been initialized with lvl >0
     *
     * @param eventID ID of the event
     * @param eventType type of the event.
     */
    public static void emitEvent(long eventID, int eventType) {
        synchronized (Tracer.class) {
            Wrapper.Event(eventType, eventID);
        }

        if (DEBUG) {
            LOGGER.debug("Emitting synchronized event [type, id] = [" + eventType + " , " + eventID + "]");
        }
    }

    /**
     * Emits an event and the current PAPI counters activated using extrae's Wrapper. Requires that Tracer has been
     * initialized with lvl >0.
     *
     * @param taskId ID of the event
     * @param eventType type of the event.
     */
    public static void emitEventAndCounters(int taskId, int eventType) {
        synchronized (Tracer.class) {
            Wrapper.Eventandcounters(eventType, taskId);
        }

        if (DEBUG) {
            LOGGER.debug(
                "Emitting synchronized event with HW counters [type, taskId] = [" + eventType + " , " + taskId + "]");
        }
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
            if (extraeEnabled()) {
                defineEvents(runtimeEvents);

                Tracer.stopWrapper();

                generateMasterPackage("package");
                transferMasterPackage();
                generateTrace("gentrace");
                cleanMasterPackage();
            } else if (scorepEnabled()) {
                // No master ScoreP trace - only Python Workers
                // generateMasterPackage("package-scorep");
                // transferMasterPackage();
                generateTrace("gentrace-scorep");
                // cleanMasterPackage();
            }
        }
    }

    /**
     * Stops the extrae wrapper.
     */
    protected static void stopWrapper() {
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
     * Returns how many events of a given type exist.
     *
     * @param type of the events
     * @return how many events does that type of event contains.
     */
    private static int getSizeByEventType(int type) {
        int size = 0;
        for (TraceEvent task : TraceEvent.values()) {
            if (task.getType() == type) {
                ++size;
            }
        }
        return size;
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

        int size = getSizeByEventType(RUNTIME_EVENTS) + 1;
        long[] values = new long[size];
        // int offset = Event.values().length; // We offset the values of the
        // defined API events (plus the 0 which is the end task always).

        String[] descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        int i = 1;
        for (TraceEvent task : TraceEvent.values()) {
            if (task.getType() == RUNTIME_EVENTS) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (DEBUG) {
                    LOGGER.debug(
                        "Tracing[API]: Api Event " + i + "=> value: " + values[i] + ", Desc: " + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(RUNTIME_EVENTS, API_DESC, values, descriptionValues);

        size = runtimeEvents.entrySet().size() + 1;

        values = new long[size];
        descriptionValues = new String[size];
        values[0] = 0;
        descriptionValues[0] = "End";

        i = 1;
        for (Entry<String, Integer> entry : runtimeEvents.entrySet()) {
            String signature = entry.getKey();
            Integer methodId = entry.getValue();
            values[i] = methodId + 1;
            LOGGER.debug("Tracing debug: " + signature);
            String methodName = signature.substring(signature.indexOf('.') + 1, signature.length());
            descriptionValues[i] = methodName;
            if (DEBUG) {
                LOGGER.debug("Tracing[TASKS_FUNC_TYPE] Event [i,methodId]: [" + i + "," + methodId + "] => value: "
                    + values[i] + ", Desc: " + descriptionValues[i]);
            }
            i++;
        }

        Wrapper.defineEventType(TASKS_FUNC_TYPE, TASK_DESC, values, descriptionValues);

        // Definition of TRANSFER_TYPE events
        size = getSizeByEventType(TASK_TRANSFERS) + 1;
        values = new long[size];
        descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        i = 1;
        for (TraceEvent task : TraceEvent.values()) {
            if (task.getType() == TASK_TRANSFERS) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (DEBUG) {
                    LOGGER.debug("Tracing[TASK_TRANSFERS]: Event " + i + "=> value: " + values[i] + ", Desc: "
                        + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(TASK_TRANSFERS, TASK_TRANSFERS_DESC, values, descriptionValues);

        // Definition of STORAGE_TYPE events
        size = getSizeByEventType(STORAGE_TYPE) + 1;
        values = new long[size];
        descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        i = 1;
        for (TraceEvent task : TraceEvent.values()) {
            if (task.getType() == STORAGE_TYPE) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (DEBUG) {
                    LOGGER.debug("Tracing[STORAGE_TYPE]: Event " + i + "=> value: " + values[i] + ", Desc: "
                        + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(STORAGE_TYPE, STORAGE_DESC, values, descriptionValues);

        // Definition of Events inside task
        size = getSizeByEventType(INSIDE_TASKS_TYPE) + 1;
        values = new long[size];
        descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        i = 1;
        for (TraceEvent task : TraceEvent.values()) {
            if (task.getType() == INSIDE_TASKS_TYPE) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (DEBUG) {
                    LOGGER.debug("Tracing[INSIDE_TASKS_EVENTS]: Event " + i + "=> value: " + values[i] + ", Desc: "
                        + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(INSIDE_TASKS_TYPE, INSIDE_TASK_DESC, values, descriptionValues);

        // Definition of Scheduling and Transfer time events
        size = 0;
        values = new long[size];

        descriptionValues = new String[size];

        Wrapper.defineEventType(TASKS_ID_TYPE, TASKID_DESC, values, descriptionValues);

        // Definition of Data transfers
        size = 0;
        values = new long[size];

        descriptionValues = new String[size];

        Wrapper.defineEventType(DATA_TRANSFERS, DATA_TRANSFERS_DESC, values, descriptionValues);
    }

    /**
     * Generate the tracing package for the master. The mode parameter enables to use different packaging methods. The
     * currently supported modes are: "package" --------> for Extrae "package-scorep" -> for ScoreP "package-map" ---->
     * for Map
     *
     * @param mode of the packaging (see trace.sh)
     */
    private static void generateMasterPackage(String mode) {
        if (DEBUG) {
            LOGGER.debug("Tracing: generating master package: " + mode);
        }

        String script = System.getenv(COMPSsConstants.COMPSS_HOME) + TRACE_SCRIPT_PATH;
        ProcessBuilder pb = new ProcessBuilder(script, mode, ".", "master");
        pb.environment().remove(LD_PRELOAD);
        Process p;
        try {
            p = pb.start();
        } catch (IOException e) {
            ErrorManager.warn("Error generating master package", e);
            return;
        }

        if (DEBUG) {
            StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), System.out, LOGGER);
            StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), System.err, LOGGER);
            outputGobbler.start();
            errorGobbler.start();
        }

        try {
            int exitCode = p.waitFor();
            if (exitCode != 0) {
                ErrorManager.warn("Error generating master package, exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            ErrorManager.warn("Error generating master package (interruptedException) : " + e.getMessage());
        }
    }

    /**
     * Copy the tracing master package from the working directory. Node packages are transferred on NIOTracer of
     * GATTracer.
     */
    private static void transferMasterPackage() {
        if (DEBUG) {
            LOGGER.debug("Tracing: Transferring master package");
        }

        String filename = ProtocolType.FILE_URI.getSchema() + "master_compss_trace.tar.gz";
        String filePath = "";
        try {
            SimpleURI uri = new SimpleURI(filename);
            filePath = new File(uri.getPath()).getCanonicalPath();
        } catch (Exception e) {
            ErrorManager.error(ERROR_MASTER_PACKAGE_FILEPATH, e);
            return;
        }

        try {
            Path source = Paths.get(filePath);
            Path target = Paths.get(traceDirPath + "master_compss_trace.tar.gz");
            Files.copy(source, target, StandardCopyOption.REPLACE_EXISTING);
        } catch (IOException ioe) {
            ErrorManager.error("Could not copy the master trace package into " + traceDirPath, ioe);
        }
    }

    /**
     * Generate the final extrae tracefile with all transferred packages.
     *
     * @param mode of the trace generation (see trace.sh)
     */
    private static void generateTrace(String mode) {
        if (DEBUG) {
            LOGGER.debug("Tracing: Generating trace with mode " + mode);
        }
        String script = System.getenv(COMPSsConstants.COMPSS_HOME) + TRACE_SCRIPT_PATH;
        String appName = System.getProperty(COMPSsConstants.APP_NAME);

        ProcessBuilder pb = new ProcessBuilder(script, mode, System.getProperty(COMPSsConstants.APP_LOG_DIR), appName,
            String.valueOf(hostToSlots.size() + 1));
        Process p;
        pb.environment().remove(LD_PRELOAD);
        try {
            p = pb.start();
        } catch (IOException e) {
            ErrorManager.warn("Error generating trace", e);
            return;
        }

        StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), System.out, LOGGER);
        StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), System.err, LOGGER);
        outputGobbler.start();
        errorGobbler.start();

        int exitCode = 0;
        try {
            exitCode = p.waitFor();
            if (exitCode != 0) {
                ErrorManager.warn("Error generating trace, exit code " + exitCode);
            }
        } catch (InterruptedException e) {
            ErrorManager.warn("Error generating trace (interruptedException) : " + e.getMessage());
        }

        String lang = System.getProperty(COMPSsConstants.LANG);
        if (exitCode == 0 && lang.equalsIgnoreCase(COMPSsConstants.Lang.PYTHON.name()) && extraeEnabled()) {
            try {
                new TraceMerger(System.getProperty(COMPSsConstants.APP_LOG_DIR), appName).merge();
            } catch (Exception e) {
                ErrorManager.warn("Error while trying to merge files: " + e.toString());
            }
        }
    }

    /**
     * Removing the tracing temporal packages.
     */
    private static void cleanMasterPackage() {

        String filename = ProtocolType.FILE_URI.getSchema() + "master_compss_trace.tar.gz";
        String filePath = "";
        try {
            SimpleURI uri = new SimpleURI(filename);
            filePath = new File(uri.getPath()).getCanonicalPath();
        } catch (Exception e) {
            ErrorManager.error(ERROR_MASTER_PACKAGE_FILEPATH, e);
            return;
        }

        if (DEBUG) {
            LOGGER.debug("Tracing: Removing tracing master package: " + filePath);
        }

        File f;
        try {
            f = new File(filePath);
            boolean deleted = f.delete();
            if (!deleted) {
                ErrorManager.warn("Unable to remove tracing temporary files of master node.");
            } else {
                if (DEBUG) {
                    LOGGER.debug("Deleted master tracing package.");
                }
            }
        } catch (Exception e) {
            ErrorManager.warn("Exception while trying to remove tracing temporary " + "files of master node.", e);
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
