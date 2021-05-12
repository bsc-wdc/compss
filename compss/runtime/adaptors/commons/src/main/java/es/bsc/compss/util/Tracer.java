/*
 *  Copyright 2002-2021 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.uri.SimpleURI;
import es.bsc.compss.util.types.PrvHeader;
import es.bsc.compss.util.types.PrvLine;
import es.bsc.compss.util.types.RowFile;
import es.bsc.compss.util.types.ThreadTranslator;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.StandardCopyOption;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Pattern;

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
    private static final String MASTER_TRACE_FILE = "master_compss_trace.tar.gz";
    private static final String RUNTIME = "Runtime";
    protected static final String TRACE_PATH = File.separator + "trace" + File.separator;
    protected static final String TRACE_SCRIPT_PATH =
        File.separator + RUNTIME + File.separator + "scripts" + File.separator + "system" + TRACE_PATH + "trace.sh";
    protected static final String TRACE_OUT_RELATIVE_PATH = TRACE_PATH + "tracer.out";
    protected static final String TRACE_ERR_RELATIVE_PATH = TRACE_PATH + "tracer.err";
    public static final String TRACE_SUBDIR = "trace";
    public static final String TO_MERGE_SUBDIR = "to_merge";

    // Naming
    public static final String MASTER_TRACE_SUFFIX = "_compss";
    public static final String TRACE_ROW_FILE_EXTENTION = ".row";
    public static final String TRACE_PRV_FILE_EXTENTION = ".prv";
    public static final String TRACE_PCF_FILE_EXTENTION = ".pcf";

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
    public static final String EXTRAE_USE_POSIX_CLOCK = "EXTRAE_USE_POSIX_CLOCK";

    // Description tags for Paraver
    private static final String TASK_DESC = "Task";
    private static final String API_DESC = "API";
    private static final String RUNTIME_DESC = RUNTIME;
    private static final String TASKID_DESC = "Task IDs";
    private static final String DATA_TRANSFERS_DESC = "Data Transfers";
    private static final String TASK_TRANSFERS_DESC = "Task Transfers Request";
    private static final String STORAGE_DESC = "Storage API";
    private static final String INSIDE_TASK_DESC = "Events inside tasks";
    private static final String INSIDE_TASK_CPU_AFFINITY_DESC = "Tasks CPU affinity";
    private static final String INSIDE_TASK_GPU_AFFINITY_DESC = "Tasks GPU affinity";
    private static final String AGENT_EVENTS_TYPE_DESC = "Agents events";
    private static final String INSIDE_WORKER_DESC = "Events inside worker";
    private static final String BINDING_MASTER_DESC = "Binding events";
    private static final String BINDING_SERIALIZATION_SIZE_DESC = "Binding serialization size events";
    private static final String BINDING_DESERIALIZATION_SIZE_DESC = "Binding deserialization size events";
    private static final String BINDING_SERIALIZATION_OBJECT_NUM = "Binding serialization object number";
    private static final String BINDING_DESERIALIZATION_OBJECT_NUM = "Binding deserialization object number";
    private static final String TASKTYPE_DESC = "Type of task";
    private static final String READY_COUNT_DESC = "Ready queue count";
    private static final String CPU_COUNT_DESC = "Number of requested CPUs";
    private static final String GPU_COUNT_DESC = "Number of requested GPUs";
    private static final String MEMORY_DESC = "Requested Memory";
    private static final String DISK_BW_DESC = "Requested disk bandwidth";
    private static final String RUNTIME_THREAD_EVENTS_DESC = "Thread type identifier";
    private static final String EXECUTOR_COUNTS_DESC = "Executor threads count";

    // Event codes
    protected static final int TASKS_FUNC_TYPE = 8_000_000;
    protected static final int API_EVENTS = 8_001_001;
    protected static final int RUNTIME_EVENTS = 8_001_002;
    protected static final int THREAD_IDENTIFICATION_EVENTS = 8_001_003; // Identifies the thread as AP, TD, executor...
    protected static final int EXECUTOR_COUNTS = 8_001_004; // Marks the life and end of an executor thread
    protected static final int TASKS_ID_TYPE = 8_000_002;
    protected static final int TASK_TRANSFERS = 8_000_003;
    protected static final int DATA_TRANSFERS = 8_000_004;
    protected static final int STORAGE_TYPE = 8_000_005;
    protected static final int READY_COUNTS = 8_000_006;
    protected static final int TASKTYPE_EVENTS = 8_000_007;
    protected static final int CPU_COUNTS = 8_000_008;
    protected static final int GPU_COUNTS = 8_000_009;
    protected static final int MEMORY = 8_000_010;
    protected static final int DISK_BW = 8_000_011;
    protected static final int SYNC_TYPE = 8_000_666;
    protected static final int TASKS_CPU_AFFINITY_TYPE = 8_000_150; // Java assignment
    protected static final int TASKS_GPU_AFFINITY_TYPE = 8_000_160; // Java assignment
    protected static final int AGENT_EVENTS_TYPE = 8_006_000;
    protected static final int INSIDE_TASKS_TYPE = 60_000_100;
    protected static final int INSIDE_TASKS_CPU_AFFINITY_TYPE = 60_000_150; // Python view
    protected static final int INSIDE_TASKS_GPU_AFFINITY_TYPE = 60_000_160; // Python view
    protected static final int INSIDE_WORKER_TYPE = 60_000_200;
    protected static final int BINDING_MASTER_TYPE = 60_000_300;
    protected static final int BINDING_SERIALIZATION_SIZE_TYPE = 60_000_600;
    protected static final int BINDING_DESERIALIZATION_SIZE_TYPE = 60_000_601;
    protected static final int BINDING_SERIALIZATION_OBJECT_NUM_TYPE = 60_000_700;
    protected static final int BINDING_DESERIALIZATION_OBJECT_NUM_TYPE = 60_000_701;
<<<<<<< HEAD

=======
>>>>>>> instrumenting serialization object id
    public static final int EVENT_END = 0;

    // Tracing modes
    public static final int BASIC_MODE = 1;
    public static final int SCOREP_MODE = -1;
    public static final int MAP_MODE = -2;

    protected static int tracingLevel = 0;
    private static String traceDirPath;
    private static Map<String, TraceHost> hostToSlots;
    private static AtomicInteger hostId;

    // Globally defined thread identification numbers with their labels, needed for
    // Paraver label updating
    // Pairs where key is the id and value the label
    public static final int AP_ID = 2;
    public static final int TD_ID = 3;
    public static final int FS_ID = 4;
    public static final int TIMER_ID = 5;
    public static final int EXECUTOR_ID = 6; // executor must be bigger than any runtime id
    public static final String appThread = "1:1:1";
    public static final String APThread = "1:1:2";
    public static final String TDThread = "1:1:3";
    public static final String workerMainEnding = "1:1";
    public static final String LastNumberFSThread = "2";
    public static final String LastNumberTimerId = "4";

    public static final String RUNTIME_ID = "1";
    public static final String NON_RUNTIME_ID = "2";

    public static final Pattern INSIDE_PARENTHESIS_PATTERN = Pattern.compile("\\(.+\\)");

    public static boolean tracerAlreadyLoaded = false;

    private static int numPthreadsEnabled = 0;
    // Hashmap of the predecessors
    private static HashMap<Integer, ArrayList<Integer>> predecessorsMap;

    /**
     * Initializes tracer creating the trace folder. If extrae's tracing is used (level > 0) then the current node
     * (master) sets its nodeID (taskID in extrae) to 0, and its number of tasks to 1 (a single program).
     *
     * @param logDirPath Path to the log directory
     * @param level type of tracing: -3: arm-ddt, -2: arm-map, -1: scorep, 0: off, 1: extrae-basic, 2: extrae-advanced
     */
    public static void init(String logDirPath, int level) {
        if (tracerAlreadyLoaded) {
            if (DEBUG) {
                LOGGER.debug("Tracing already initialized " + level + "no need for a second initialization");
            }
            return;
        }
        tracerAlreadyLoaded = true;
        if (DEBUG) {
            LOGGER.debug("Initializing tracing with level " + level);
        }

        hostId = new AtomicInteger(1);
        hostToSlots = new HashMap<>();
        predecessorsMap = new HashMap<>();

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
        } else if (DEBUG) {
            if (Tracer.scorepEnabled()) {
                LOGGER.debug("Initializing scorep.");
            } else {
                if (Tracer.mapEnabled()) {
                    LOGGER.debug("Initializing arm-map.");
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

    public static int getTasksCPUAffinityEventsType() {
        return TASKS_CPU_AFFINITY_TYPE;
    }

    public static int getTasksGPUAffinityEventsType() {
        return TASKS_GPU_AFFINITY_TYPE;
    }

    public static int getInsideTasksCPUAffinityEventsType() {
        return INSIDE_TASKS_CPU_AFFINITY_TYPE;
    }

    public static int getInsideTasksGPUAffinityEventsType() {
        return INSIDE_TASKS_GPU_AFFINITY_TYPE;
    }

    public static int getInsideWorkerEventsType() {
        return INSIDE_WORKER_TYPE;
    }

    public static int getBindingMasterEventsType() {
        return BINDING_MASTER_TYPE;
    }

    public static int getTaskTypeEventsType() {
        return TASKTYPE_EVENTS;
    }

    public static int getCPUCountEventsType() {
        return CPU_COUNTS;
    }

    public static int getGPUCountEventsType() {
        return GPU_COUNTS;
    }

    public static int getReadyCountEventsType() {
        return READY_COUNTS;
    }

    public static int getMemoryEventsType() {
        return MEMORY;
    }

    public static int getDiskBWEventsType() {
        return DISK_BW;
    }

    public static TraceEvent getAcessProcessorRequestEvent(String eventType) {
        return TraceEvent.valueOf(eventType);
    }

    public static boolean taskHasPredecessors(Integer taskId) {
        return predecessorsMap.containsKey(taskId);
    }

    public static ArrayList<Integer> getPredecessors(int taskId) {
        return predecessorsMap.get(taskId);
    }

    public static void removePredecessor(int taskId) {
        predecessorsMap.remove(taskId);
    }

    public static void setPredecessors(int taskId, ArrayList<Integer> predecessors) {
        predecessorsMap.put(taskId, predecessors);
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
                if (basicModeEnabled()) {
                    updateThreads();
                }
                cleanMasterPackage();
            } else if (scorepEnabled()) {
                // No master ScoreP trace - only Python Workers
                generateTrace("gentrace-scorep");
            }
        }
    }

    /**
     * Updates the threads in .prv and .row classifying them in runtime or non runtime and assigning the corresponding
     * labels
     */
    private static void updateThreads() {
        String disable = System.getProperty(COMPSsConstants.DISABLE_CUSTOM_THREADS_TRACING);
        if (disable != null) {
            LOGGER.debug("Custom thread translation disabled");
            return;
        }
        LOGGER.debug("Tracing: Updating thread labels");
        File[] rowFileArray;
        File[] prvFileArray;
        try {
            String appLogDir = System.getProperty(COMPSsConstants.APP_LOG_DIR);
            File dir = new File(appLogDir + TRACE_SUBDIR);
            final String traceNamePrefix = Tracer.getTraceNamePrefix();
            rowFileArray = dir.listFiles((File d, String name) -> name.endsWith(TRACE_ROW_FILE_EXTENTION));
            prvFileArray = dir.listFiles((File d, String name) -> name.endsWith(TRACE_PRV_FILE_EXTENTION));
        } catch (Exception e) {
            ErrorManager.error(ERROR_MASTER_PACKAGE_FILEPATH, e);
            return;
        }
        try {
            if (rowFileArray != null && rowFileArray.length > 0) {
                File rowFile = rowFileArray[0];
                File prvFile = prvFileArray[0];
                ThreadTranslator thTranslator = createThreadTranslations(prvFile);
                writeTranslatedPrvThreads(prvFile, thTranslator);
                updateRowLabels(rowFile, thTranslator.getRowLabels());
            }
        } catch (Exception e) {
            LOGGER.debug(e);
            LOGGER.debug(e.toString());
            ErrorManager.error("Could not update thread labels " + traceDirPath, e);
            e.printStackTrace();
        }
    }

    /**
     * Reads the .prv and creates a map from the old thread identifier to a new one based on
     * THREAD_IDENTIFICATION_EVENTS
     */
    public static ThreadTranslator createThreadTranslations(File prvFile) throws Exception {
        final BufferedReader br = new BufferedReader(new FileReader(prvFile));
        final String threadIdEvent = Integer.toString(THREAD_IDENTIFICATION_EVENTS);
        final ThreadTranslator thTranslator = new ThreadTranslator();
        br.readLine(); // we don't need the header right now
        String line;
        // the isEmpty check should not be necessary if the .prv files are well constructed
        while ((line = br.readLine()) != null && !line.isEmpty()) {
            PrvLine prvLine = new PrvLine(line);
            String oldThreadId = prvLine.getStateLineThreadIdentifier();
            Map<String, String> events = prvLine.getEvents();
            String identifierEventValue = events.get(threadIdEvent);
            thTranslator.addThread(oldThreadId, identifierEventValue);
        }
        br.close();
        return thTranslator;
    }

    /**
     * Updates the threads in .prv with the information from translations.
     * 
     * @throws Exception Exception reading or parsing the files
     */
    public static void writeTranslatedPrvThreads(File prvFile, ThreadTranslator thThranslator) throws Exception {
        Map<String, String> translations = thThranslator.createThreadTranslationMap();
        LOGGER.debug("Tracing: Updating thread identifiers in .prv file");
        final String oldFilePath = prvFile.getAbsolutePath();
        final String newFilePath = oldFilePath + "_tmp_updatedThreadsId";
        final File updatedPrvFile = new File(newFilePath);
        if (!updatedPrvFile.exists()) {
            updatedPrvFile.createNewFile();
        }
        final BufferedReader br = new BufferedReader(new FileReader(prvFile));
        final PrintWriter prvWriter = new PrintWriter(new FileWriter(updatedPrvFile.getAbsolutePath(), true));
        PrvHeader header = new PrvHeader(br.readLine());
        // Needed in the case of the runcompss, won't do anything in agents
        header.transformNodesToAplications();
        header.splitRuntimeExecutors(thThranslator.createRuntimeThreadNumberPerApp());
        prvWriter.println(header.toString());
        String line;
        // the isEmpty check should not be necessary if the .prv files are well constructed
        while ((line = br.readLine()) != null && !line.isEmpty()) {
            PrvLine prvLine = new PrvLine(line);
            prvLine.translateLineThreads(translations);
            prvWriter.println(prvLine.toString());
        }

        br.close();
        prvWriter.close();
        updatedPrvFile.renameTo(new File(oldFilePath));
    }

    /**
     * Updates in the .row the threads changed in the .prv and apply the corresponding labels from LABEL_TRANSLATIONS.
     * 
     * @throws Exception Exception reading or parsing the files
     */
    public static void updateRowLabels(File rf, List<String> labels) throws IOException {
        RowFile rowFile = new RowFile(rf);
        rowFile.updateRowLabels(labels);
        rowFile.printInfo(rf);
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

    private static List<TraceEvent> getEventsByType(int eventsType) {
        LinkedList<TraceEvent> eventsList = new LinkedList<>();
        for (TraceEvent traceEvent : TraceEvent.values()) {
            if (traceEvent.getType() == eventsType) {
                eventsList.add(traceEvent);
            }
        }
        return eventsList;
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
        defineEventsForType(API_EVENTS, API_DESC);
        defineEventsForType(RUNTIME_EVENTS, RUNTIME_DESC);
        defineEventsForFunctions(TASKS_FUNC_TYPE, TASK_DESC, runtimeEvents);
        defineEventsForType(TASK_TRANSFERS, TASK_TRANSFERS_DESC);
        defineEventsForType(STORAGE_TYPE, STORAGE_DESC);
        defineEventsForType(INSIDE_TASKS_TYPE, INSIDE_TASK_DESC);
        defineEventsForType(AGENT_EVENTS_TYPE, AGENT_EVENTS_TYPE_DESC);
        defineEventsForType(INSIDE_TASKS_CPU_AFFINITY_TYPE, INSIDE_TASK_CPU_AFFINITY_DESC);
        defineEventsForType(INSIDE_TASKS_GPU_AFFINITY_TYPE, INSIDE_TASK_GPU_AFFINITY_DESC);
        defineEventsForType(INSIDE_WORKER_TYPE, INSIDE_WORKER_DESC);
        defineEventsForType(BINDING_MASTER_TYPE, BINDING_MASTER_DESC);
        defineEventsForType(THREAD_IDENTIFICATION_EVENTS, RUNTIME_THREAD_EVENTS_DESC);
        defineEventsForType(EXECUTOR_COUNTS, EXECUTOR_COUNTS_DESC);
        defineEventsForType(BINDING_SERIALIZATION_SIZE_TYPE, BINDING_SERIALIZATION_SIZE_DESC);
        defineEventsForType(BINDING_DESERIALIZATION_SIZE_TYPE, BINDING_DESERIALIZATION_SIZE_DESC);
        defineEventsForType(BINDING_SERIALIZATION_OBJECT_NUM_TYPE, BINDING_SERIALIZATION_OBJECT_NUM);
        defineEventsForType(BINDING_DESERIALIZATION_OBJECT_NUM_TYPE, BINDING_DESERIALIZATION_OBJECT_NUM);

        defineEventsForTaskType(TASKTYPE_EVENTS, TASKTYPE_DESC, MethodType.values());
        // Definition of Scheduling and Transfer time events
        Wrapper.defineEventType(TASKS_ID_TYPE, TASKID_DESC, new long[0], new String[0]);
        // Definition of Data transfers
        Wrapper.defineEventType(DATA_TRANSFERS, DATA_TRANSFERS_DESC, new long[0], new String[0]);
        // Definition of Ready Counts
        defineEventsForType(READY_COUNTS, READY_COUNT_DESC);
        // Definition of CPU Counts
        Wrapper.defineEventType(CPU_COUNTS, CPU_COUNT_DESC, new long[0], new String[0]);
        // Definition of GPU Counts
        Wrapper.defineEventType(GPU_COUNTS, GPU_COUNT_DESC, new long[0], new String[0]);
        // Definition of Memory
        Wrapper.defineEventType(MEMORY, MEMORY_DESC, new long[0], new String[0]);
        // Definition of Disk BW
        Wrapper.defineEventType(DISK_BW, DISK_BW_DESC, new long[0], new String[0]);
    }

    private static void defineEventsForTaskType(int tasktypeEvents, String tasktypeDesc, MethodType[] types) {
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
        Wrapper.defineEventType(tasktypeEvents, tasktypeDesc, values, descriptionValues);

    }

    private static void defineEventsForFunctions(int tasksFuncType, String taskDesc,
        Map<String, Integer> runtimeEvents) {
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

        Wrapper.defineEventType(tasksFuncType, taskDesc, values, descriptionValues);
    }

    private static void defineEventsForType(int eventsType, String eventsDesc) {
        List<TraceEvent> events = getEventsByType(eventsType);
        // defined API events (plus the 0 which is the end task always).
        int size = events.size() + 1;
        long[] values = new long[size];
        String[] descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        int i = 1;
        for (TraceEvent event : events) {
            values[i] = event.getId();
            descriptionValues[i] = event.getSignature();
            if (DEBUG) {
                LOGGER.debug("Tracing[API]: Type " + eventsType + " Event " + i + "=> value: " + values[i] + ", Desc: "
                    + descriptionValues[i]);
            }
            ++i;
        }
        Wrapper.defineEventType(eventsType, eventsDesc, values, descriptionValues);

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
            ErrorManager.warn("Error generating master package (interruptedException)", e);
            Thread.currentThread().interrupt();
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

        String filename = ProtocolType.FILE_URI.getSchema() + MASTER_TRACE_FILE;
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
            Path target = Paths.get(traceDirPath + MASTER_TRACE_FILE);
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
        String traceName = "";
        String appName = System.getProperty(COMPSsConstants.APP_NAME);
        String label = System.getProperty(COMPSsConstants.TRACE_LABEL);

        if (appName != null && !appName.isEmpty() && !appName.equals("None")) {
            if (label != null && !label.isEmpty() && !label.equals("None")) {
                traceName = appName.concat("_" + label);
            } else {
                traceName = appName;
            }
        } else {
            if (label != null && !label.isEmpty() && !label.equals("None")) {
                traceName = label;
            }
        }

        ProcessBuilder pb = new ProcessBuilder(script, mode, System.getProperty(COMPSsConstants.APP_LOG_DIR), traceName,
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
            ErrorManager.warn("Error generating trace (interruptedException)", e);
            Thread.currentThread().interrupt();
        }

        String lang = System.getProperty(COMPSsConstants.LANG);
        if (exitCode == 0 && lang.equalsIgnoreCase(COMPSsConstants.Lang.PYTHON.name()) && extraeEnabled()) {
            try {
                String appLogDir = System.getProperty(COMPSsConstants.APP_LOG_DIR);
                PythonTraceMerger t = new PythonTraceMerger(appLogDir);
                t.merge();
            } catch (Exception e) {
                ErrorManager.warn("Error while trying to merge files", e);
            }
        }
    }

    /**
     * Returns the beginning of the name of the trace files.
     */
    public static String getTraceNamePrefix() {
        String traceName = System.getProperty(COMPSsConstants.APP_NAME);
        String label = System.getProperty(COMPSsConstants.TRACE_LABEL);
        if (label != null && !label.isEmpty() && !label.equals("None")) {
            traceName = traceName.concat("_" + label);
        }
        return traceName + Tracer.MASTER_TRACE_SUFFIX;
    }

    /**
     * Removing the tracing temporal packages.
     */
    private static void cleanMasterPackage() {

        String filename = ProtocolType.FILE_URI.getSchema() + MASTER_TRACE_FILE;
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
            ErrorManager.warn("Exception while trying to remove tracing temporary files of master node.", e);
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
