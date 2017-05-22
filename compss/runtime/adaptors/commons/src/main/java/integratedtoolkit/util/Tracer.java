package integratedtoolkit.util;

import java.io.File;
import java.io.IOException;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.HashMap;
import java.util.concurrent.Semaphore;

import es.bsc.cepbatools.extrae.Wrapper;
import integratedtoolkit.comm.Comm;
import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.data.LogicalData;
import integratedtoolkit.types.data.listener.TracingCopyListener;
import integratedtoolkit.types.data.location.DataLocation;
import integratedtoolkit.types.data.transferable.TracingCopyTransferable;
import integratedtoolkit.types.uri.SimpleURI;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class Tracer {

    private static final String taskDesc = "Task";
    private static final String apiDesc = "Runtime";
    private static final String taskIdDesc = "Task IDs";
    private static final String dataTransfersDesc = "Data Transfers";
    private static final String tasksTransfersDesc = "Task Transfers Request";
    private static final String storageDesc = "Storage API";
    private static final String insideTaskDesc = "Events inside tasks";

    protected static final String TRACE_SCRIPT_PATH = File.separator + "Runtime" + File.separator + "scripts" + File.separator + "system"
            + File.separator + "trace.sh";
    protected static final String traceOutRelativePath = File.separator + "trace" + File.separator + "tracer.out";
    protected static final String traceErrRelativePath = File.separator + "trace" + File.separator + "tracer.err";

    protected static final Logger logger = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean debug = logger.isDebugEnabled();
    protected static final String ERROR_TRACE_DIR = "ERROR: Cannot create trace directory";

    private static final int TASKS_FUNC_TYPE    = 8_000_000;
    private static final int RUNTIME_EVENTS     = 8_000_001;
    private static final int TASKS_ID_TYPE      = 8_000_002;
    private static final int TASK_TRANSFERS     = 8_000_003;
    private static final int DATA_TRANSFERS     = 8_000_004;
    private static final int STORAGE_TYPE       = 8_000_005;
    private static final int READY_COUNTS       = 8_000_006;
    private static final int SYNC_TYPE          = 8_000_666;
    private static final int INSIDE_TASKS_TYPE  = 60_000_100;

    public static final int EVENT_END = 0;

    public static final int BASIC_MODE = 1;

    public static final String LD_PRELOAD = "LD_PRELOAD";
    public static final String EXTRAE_CONFIG_FILE = "EXTRAE_CONFIG_FILE";

    protected static int tracing_level = 0;
    
    private static final boolean isCustomExtraeFile = (System.getProperty(ITConstants.IT_EXTRAE_CONFIG_FILE) != null)
            && !System.getProperty(ITConstants.IT_EXTRAE_CONFIG_FILE).isEmpty()
            && !System.getProperty(ITConstants.IT_EXTRAE_CONFIG_FILE).equals("null");
    private static final String extraeFile = isCustomExtraeFile ? System.getProperty(ITConstants.IT_EXTRAE_CONFIG_FILE) : "null";


    public enum Event {
        STATIC_IT(1, RUNTIME_EVENTS, "Loading Runtime"), 
        START(2, RUNTIME_EVENTS, "Start"), 
        STOP(3, RUNTIME_EVENTS, "Stop"), 
        TASK(4, RUNTIME_EVENTS, "Execute Task"), 
        NO_MORE_TASKS(5, RUNTIME_EVENTS, "Waiting for tasks end"), 
        WAIT_FOR_ALL_TASKS(6, RUNTIME_EVENTS, "Barrier"), 
        OPEN_FILE(7, RUNTIME_EVENTS, "Waiting for open file"), 
        GET_FILE(8, RUNTIME_EVENTS, "Waiting for get file"), 
        GET_OBJECT(9, RUNTIME_EVENTS, "Waiting for get object"), 
        TASK_RUNNING(11, RUNTIME_EVENTS, "Task Running"),
        DELETE(12, RUNTIME_EVENTS, "Delete File"),
        WORKER_RECEIVED_NEW_TASK(13, RUNTIME_EVENTS, "Received new task"),


        // Access Processor Events
        DEBUG(17, RUNTIME_EVENTS, "Access Processor: Debug"),
        ANALYSE_TASK(18, RUNTIME_EVENTS, "Access Processor: Analyse task"),
        UPDATE_GRAPH(19, RUNTIME_EVENTS, "Access Processor: Update graph"),
        WAIT_FOR_TASK(20, RUNTIME_EVENTS, "Access Processor: Wait for task"),
        END_OF_APP(21, RUNTIME_EVENTS, "Access Processor: End of app"),
        ALREADY_ACCESSED(22, RUNTIME_EVENTS, "Access Processor: Already accessed"),
        REGISTER_DATA_ACCESS(23, RUNTIME_EVENTS, "Access Processor: Register data access"),
        TRANSFER_OPEN_FILE(24, RUNTIME_EVENTS, "Access Processor: Transfer open file"),
        TRANSFER_RAW_FILE(25, RUNTIME_EVENTS, "Access Processor: Transfer raw file"),
        TRANSFER_OBJECT(26, RUNTIME_EVENTS, "Access Processor: Transfer object"),
        NEW_VERSION_SAME_VALUE(27, RUNTIME_EVENTS,"Access Processor: New version same value"),
        IS_OBJECT_HERE(28, RUNTIME_EVENTS, "Access Processor: Is object here"),
        SET_OBJECT_VERSION_VALUE(29, RUNTIME_EVENTS, "Access Processor: Set object version value"),
        GET_LAST_RENAMING(30, RUNTIME_EVENTS, "Access Processor: Get last renaming"),
        BLOCK_AND_GET_RESULT_FILES(31, RUNTIME_EVENTS,"Access Processor: Block and get result files"),
        UNBLOCK_RESULT_FILES(32, RUNTIME_EVENTS,"Access Processor: Unblock result files"),
        SHUTDOWN(33, RUNTIME_EVENTS, "Access Processor: Shutdown"),
        GRAPHSTATE(34, RUNTIME_EVENTS, "Access Processor: Graphstate"),
        TASKSTATE(35, RUNTIME_EVENTS, "Access Processor: Taskstate"),
        DELETE_FILE(36, RUNTIME_EVENTS, "Access Processor: Delete file"),
        // Storage Events
        STORAGE_GETBYID(37, STORAGE_TYPE, "getByID"), 
        STORAGE_NEWREPLICA(38, STORAGE_TYPE, "newReplica"), 
        STORAGE_NEWVERSION(39,STORAGE_TYPE, "newVersion"), 
        STORAGE_INVOKE(40, STORAGE_TYPE, "invoke"), 
        STORAGE_EXECUTETASK(41, STORAGE_TYPE,"executeTask"), 
        STORAGE_GETLOCATIONS(42, STORAGE_TYPE, "getLocations"), 
        STORAGE_CONSOLIDATE(43, STORAGE_TYPE, "consolidateVersion"), 
        // Task Dispatcher Events
        ACTION_UPDATE(45, RUNTIME_EVENTS, "Task Dispatcher: Action update"), 
        CE_REGISTRATION(46, RUNTIME_EVENTS, "Task Dispatcher: CE registration"), 
        EXECUTE_TASKS(47, RUNTIME_EVENTS, "Task Dispatcher: Execute tasks"), 
        GET_CURRENT_SCHEDULE(48, RUNTIME_EVENTS, "Task Dispatcher: Get current schedule"), 
        PRINT_CURRENT_GRAPH(49, RUNTIME_EVENTS, "Task Dispatcher: Print current graph"),
        MONITORING_DATA(50, RUNTIME_EVENTS, "Task Dispatcher: Monitoring data"), 
        TD_SHUTDOWN(51, RUNTIME_EVENTS, "Task Dispatcher: Shutdown"), 
        UPDATE_CEI_LOCAL(52, RUNTIME_EVENTS, "Task Dispatcher: Update CEI local"), 
        WORKER_UPDATE_REQUEST(53, RUNTIME_EVENTS, "Task Dispatcher: Worker update request"),
        // Task Events
        TASK_EXECUTION_PYTHON(1, INSIDE_TASKS_TYPE, "Task execution"),
        USER_CODE_PYTHON(2, INSIDE_TASKS_TYPE, "User code execution"),
        IMPORTING_MODULES_PYTHON(3, INSIDE_TASKS_TYPE, "Importing modules"),
        THREAD_BINDING_PYTHON(4, INSIDE_TASKS_TYPE, "Thread binding"),
        DESERIALIZE_OBJECT_PYTHON1(5, INSIDE_TASKS_TYPE, "Deserializing object"),
        DESERIALIZE_OBJECT_PYTHON2(6, INSIDE_TASKS_TYPE, "Deserializing object"),
        SERIALIZE_OBJECT_PYTHON(7, INSIDE_TASKS_TYPE, "Serializing object"),
        WORKER_RUNNING(102, INSIDE_TASKS_TYPE, "Worker running"),

        READY_COUNT(1, READY_COUNTS, "Ready queue count");

        private final int id;
        private final int type;
        private final String signature;


        Event(int id, int type, String signature) {
            this.id = id;
            this.type = type;
            this.signature = signature;
        }

        public int getId() {
            return this.id;
        }

        public int getType() {
            return this.type;
        }

        public String getSignature() {
            return this.signature;
        }
    }


    private static String traceDirPath;
    private static Map<String, TraceHost> hostToSlots;
    private static AtomicInteger hostId;


    public static void init(int level) {
        if (debug) {
            logger.debug("Initializing tracing with level " + level);
        }

        hostId = new AtomicInteger(1);
        hostToSlots = new HashMap<>();

        traceDirPath = Comm.getAppHost().getAppLogDirPath() + "trace" + File.separator;
        if (!new File(traceDirPath).mkdir()) {
            ErrorManager.error(ERROR_TRACE_DIR);
        }

        Wrapper.SetTaskID(0);
        Wrapper.SetNumTasks(1);

        tracing_level = level;
    }

    public static boolean isActivated() {
        return tracing_level > 0;
    }

    public static boolean basicModeEnabled() {
        return tracing_level == Tracer.BASIC_MODE;
    }

    public static int getLevel() {
        return tracing_level;
    }
    
    public static String getExtraeFile() {
        return extraeFile;
    }

    public static void enablePThreads() {
        synchronized (Tracer.class) {
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS);
        }
    }

    public static void disablePThreads() {
        synchronized (Tracer.class) {
            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
        }
    }

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
            id = hostId.getAndIncrement();
            hostToSlots.put(name, new TraceHost(slots));
        }
        return id;
    }

    public static int getNextSlot(String host) {
        int slot = hostToSlots.get(host).getNextSlot();
        if (debug) {
            logger.debug("Tracing: Getting slot " + slot + " of host " + host);
        }
        return slot;
    }

    public static void freeSlot(String host, int slot) {
        if (debug) {
            logger.debug("Tracing: Freeing slot " + slot + " of host " + host);
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

    public static Event getAPRequestEvent(String eventType) {
        return Event.valueOf(eventType);
    }

    public static Event getTDRequestEvent(String eventType) {
        Event event = null;
        try {
            event = Event.valueOf(eventType);
        } catch (Exception e) {
            logger.error("Task Dispatcher event " + eventType + " is not present in Tracer's list ");
        }
        return event;
    }

    public static void emitEvent(long eventID, int eventType) {
        synchronized (Tracer.class) {
            Wrapper.Event(eventType, eventID);
        }

        if (debug) {
            logger.debug("Emitting synchronized event [type, id] = [" + eventType + " , " + eventID + "]");
        }
    }

    public static void emitEventAndCounters(int taskId, int eventType) {
        synchronized (Tracer.class) {
            Wrapper.Eventandcounters(eventType, taskId);
        }

        if (debug) {
            logger.debug("Emitting synchronized event with HW counters [type, taskId] = [" + eventType + " , " + taskId + "]");
        }

    }

    public static void fini() {
        if (debug) {
            logger.debug("Tracing: finalizing");
        }

        synchronized (Tracer.class) {
            defineEvents();

            Wrapper.SetOptions(Wrapper.EXTRAE_ENABLE_ALL_OPTIONS & ~Wrapper.EXTRAE_PTHREAD_OPTION);
            Wrapper.Fini();
            Wrapper.SetOptions(Wrapper.EXTRAE_DISABLE_ALL_OPTIONS);

            generateMasterPackage();
            transferMasterPackage();
            generateTrace();
            cleanMasterPackage();
        }

    }

    private static int getSizeByEventType(int type) {
        int size = 0;
        for (Event task : Event.values()) {
            if (task.getType() == type) {
                ++size;
            }
        }
        return size;
    }

    private static void defineEvents() {
        Map<String, Integer> signatureToId = CoreManager.getSignaturesToId();
        if (debug) {
            logger.debug("SignatureToId size: " + signatureToId.size());
        }

        int size = getSizeByEventType(RUNTIME_EVENTS) + 1;
        long[] values = new long[size];
        // int offset = Event.values().length; // We offset the values of the
        // defined API events (plus the 0 which is the end task always).

        String[] descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        int i = 1;
        for (Event task : Event.values()) {
            if (task.getType() == RUNTIME_EVENTS) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (debug) {
                    logger.debug("Tracing[API]: Api Event " + i + "=> value: " + values[i] + ", Desc: " + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(RUNTIME_EVENTS, apiDesc, values, descriptionValues);

        size = signatureToId.entrySet().size() + 1;

        values = new long[size];
        descriptionValues = new String[size];
        values[0] = 0;
        descriptionValues[0] = "End";

        i = 1;
        for (Entry<String, Integer> entry : signatureToId.entrySet()) {
            String signature = entry.getKey();
            Integer methodId = entry.getValue();
            values[i] = methodId + 1;
            String methodName = signature.substring(0, signature.indexOf('('));
            descriptionValues[i] = methodName;
            if (debug) {
                logger.debug("Tracing[TASKS_FUNC_TYPE] Event [i,methodId]: [" + i + "," + methodId + "] => value: " + values[i] + ", Desc: "
                        + descriptionValues[i]);
            }
            i++;
        }

        Wrapper.defineEventType(TASKS_FUNC_TYPE, taskDesc, values, descriptionValues);

        // Definition of TRANSFER_TYPE events
        size = getSizeByEventType(TASK_TRANSFERS) + 1;
        values = new long[size];
        descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        i = 1;
        for (Event task : Event.values()) {
            if (task.getType() == TASK_TRANSFERS) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (debug) {
                    logger.debug("Tracing[TASK_TRANSFERS]: Event " + i + "=> value: " + values[i] + ", Desc: " + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(TASK_TRANSFERS, tasksTransfersDesc, values, descriptionValues);

        // Definition of STORAGE_TYPE events
        size = getSizeByEventType(STORAGE_TYPE) + 1;
        values = new long[size];
        descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        i = 1;
        for (Event task : Event.values()) {
            if (task.getType() == STORAGE_TYPE) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (debug) {
                    logger.debug("Tracing[STORAGE_TYPE]: Event " + i + "=> value: " + values[i] + ", Desc: " + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(STORAGE_TYPE, storageDesc, values, descriptionValues);

        // Definition of Events inside task
        size = getSizeByEventType(INSIDE_TASKS_TYPE) + 1;
        values = new long[size];
        descriptionValues = new String[size];

        values[0] = 0;
        descriptionValues[0] = "End";
        i = 1;
        for (Event task : Event.values()) {
            if (task.getType() == INSIDE_TASKS_TYPE) {
                values[i] = task.getId();
                descriptionValues[i] = task.getSignature();
                if (debug) {
                    logger.debug("Tracing[INSIDE_TASKS_EVENTS]: Event " + i + "=> value: " + values[i] + ", Desc: " + descriptionValues[i]);
                }
                ++i;
            }
        }

        Wrapper.defineEventType(INSIDE_TASKS_TYPE, insideTaskDesc, values, descriptionValues);

        // Definition of Scheduling and Transfer time events
        size = 0;
        values = new long[size];

        descriptionValues = new String[size];

        Wrapper.defineEventType(TASKS_ID_TYPE, taskIdDesc, values, descriptionValues);

        // Definition of Data transfers
        size = 0;
        values = new long[size];

        descriptionValues = new String[size];

        Wrapper.defineEventType(DATA_TRANSFERS, dataTransfersDesc, values, descriptionValues);
    }

    private static void generateMasterPackage() {
        if (debug) {
            logger.debug("Tracing: generating master package");
        }

        String script = System.getenv(ITConstants.IT_HOME) + TRACE_SCRIPT_PATH;
        ProcessBuilder pb = new ProcessBuilder(script, "package", ".", "master");
        pb.environment().remove(LD_PRELOAD);
        Process p;
        try {
            p = pb.start();
        } catch (IOException e) {
            ErrorManager.warn("Error generating master package", e);
            return;
        }

        if (debug) {
            StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), System.out, logger);
            StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), System.err, logger);
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

    private static void transferMasterPackage() {
        if (debug) {
            logger.debug("Tracing: Transferring master package");
        }

        // Create source and target locations for tar.gz file
        String filename = "master_compss_trace.tar.gz";
        DataLocation source = null;
        String sourcePath = DataLocation.Protocol.FILE_URI.getSchema() + filename;
        try {
            SimpleURI uri = new SimpleURI(sourcePath);
            source = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + sourcePath, e);
        }
        DataLocation target = null;
        String targetPath = DataLocation.Protocol.FILE_URI.getSchema() + traceDirPath + filename;
        try {
            SimpleURI uri = new SimpleURI(targetPath);
            target = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + targetPath, e);
        }

        // Ask for data
        Semaphore sem = new Semaphore(0);
        TracingCopyListener tracingListener = new TracingCopyListener(sem);
        tracingListener.addOperation();

        Comm.getAppHost().getNode().obtainData(new LogicalData("tracing master package"), source, target,
                new LogicalData("tracing master package"), new TracingCopyTransferable(), tracingListener);

        // Wait for data
        tracingListener.enable();
        try {
            sem.acquire();
        } catch (InterruptedException ex) {
            ErrorManager.warn("Error waiting for tracing files in master to get saved");
        }
    }

    private static void generateTrace() {
        if (debug) {
            logger.debug("Tracing: Generating trace");
        }
        String script = System.getenv(ITConstants.IT_HOME) + TRACE_SCRIPT_PATH;
        String appName = System.getProperty(ITConstants.IT_APP_NAME);
        ProcessBuilder pb = new ProcessBuilder(script, "gentrace", System.getProperty(ITConstants.IT_APP_LOG_DIR), appName,
                String.valueOf(hostToSlots.size() + 1));
        Process p;
        pb.environment().remove(LD_PRELOAD);
        try {
            p = pb.start();
        } catch (IOException e) {
            ErrorManager.warn("Error generating trace", e);
            return;
        }

        StreamGobbler outputGobbler = new StreamGobbler(p.getInputStream(), System.out, logger);
        StreamGobbler errorGobbler = new StreamGobbler(p.getErrorStream(), System.err, logger);
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

        String lang = System.getProperty(ITConstants.IT_LANG);
        if (exitCode == 0 && lang.equalsIgnoreCase(ITConstants.Lang.PYTHON.name())) {
            try {
                new TraceMerger(System.getProperty(ITConstants.IT_APP_LOG_DIR), appName).merge();
            } catch (IOException e) {
                ErrorManager.warn("Error while trying to merge files", e);
            }
        }
    }

    private static void cleanMasterPackage() {
        String filename = DataLocation.Protocol.FILE_URI.getSchema() + "master_compss_trace.tar.gz";

        DataLocation source;
        try {
            SimpleURI uri = new SimpleURI(filename);
            source = DataLocation.createLocation(Comm.getAppHost(), uri);
        } catch (Exception e) {
            ErrorManager.error(DataLocation.ERROR_INVALID_LOCATION + " " + filename, e);
            return;
        }

        if (debug) {
            logger.debug("Tracing: Removing tracing master package: " + source.getPath());
        }

        File f;
        try {
            f = new File(source.getPath());
            boolean deleted = f.delete();
            if (!deleted) {
                ErrorManager.warn("Unable to remove tracing temporary files of master node.");
            }
        } catch (Exception e) {
            ErrorManager.warn("Unable to remove tracing temporary files of master node.", e);
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
