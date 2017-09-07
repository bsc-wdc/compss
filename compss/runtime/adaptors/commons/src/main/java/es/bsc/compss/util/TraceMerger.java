package es.bsc.compss.util;

import es.bsc.compss.log.Loggers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TraceMerger {

    protected static final Logger logger = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean debug = logger.isDebugEnabled();

    // Info used for matching sync events
    private static final Integer SYNC_TYPE = 8000666;
    private String syncRegex = "(^\\d+:\\d+:\\d+):(\\d+):(\\d+):(\\d+).*:" + SYNC_TYPE + ":(\\d+)";
    private Pattern syncPattern = Pattern.compile(syncRegex);
    // Selectors for replace Pattern
    private static final Integer R_ID_INDEX = 1;
    private static final Integer TIMESTAMP_INDEX = 4;
    private static final Integer WORKER_ID_INDEX = 2; // could be wrong this regex (designed for matching tasks not
                                                      // workers)

    private String workerThreadInfo = "(^\\d+):(\\d+):(\\d+):(\\d+):(\\d+):(\\d+):(.*)";
    private Pattern workerThreadInfoPattern = Pattern.compile(workerThreadInfo);
    private static final Integer STATE_TYPE = 1;
    private static final Integer WORKER_THREAD_ID = 2;
    private static final Integer WORKER_TIMESTAMP = 6;
    private static final Integer WORKER_LINE_INFO = 7;

    private static final String masterTraceSuffix = "_compss_trace_";
    private static final String traceExtension = ".prv";
    private static final String workerTraceSuffix = "_python_trace" + traceExtension;
    private static final String traceSubDir = "trace";
    private static final String workerSubDir = "python";
    private static String workingDir;

    private FileWriter fw;
    private BufferedWriter bw;
    private PrintWriter masterWriter;

    private File masterTrace;
    private File[] workersTraces;

    private String masterTracePath;
    private String[] workersTracePath;


    private class LineInfo {

        private final String resourceId;
        private final Long timestamp;


        public LineInfo(String resourceID, Long timestamp) {
            this.resourceId = resourceID;
            this.timestamp = timestamp;
        }

        public Long getTimestamp() {
            return timestamp;
        }
    }


    public TraceMerger(String workingDir, String appName) throws IOException {
        initMasterTraceInfo(workingDir, appName);
        initWorkersTracesInfo(workingDir);

        fw = new FileWriter(masterTracePath, true);
        bw = new BufferedWriter(fw);
        masterWriter = new PrintWriter(bw);

        logger.debug("Trace's merger initialization successful");

    }

    private void initMasterTraceInfo(String workingDir, String appName) throws FileNotFoundException {
        final String traceNamePrefix = appName + masterTraceSuffix;

        File f = new File(workingDir + File.separator + traceSubDir);
        File[] matchingFiles = f.listFiles((File dir, String name) -> name.startsWith(traceNamePrefix) && name.endsWith(traceExtension));

        if (matchingFiles == null) {
            throw new FileNotFoundException("Master trace " + traceNamePrefix + "*" + traceExtension + " not found.");
        }
        if (!(matchingFiles.length < 1)) {
            masterTrace = matchingFiles[0];
            masterTracePath = masterTrace.getAbsolutePath();
            if (matchingFiles.length > 1) {
                logger.warn("Found more than one master trace, using " + masterTrace + " to merge.");
            }
        } else {
            throw new FileNotFoundException("Master trace " + traceNamePrefix + "*" + traceExtension + " not found.");
        }

    }

    private void initWorkersTracesInfo(String workingDir) throws FileNotFoundException {
        TraceMerger.workingDir = workingDir;
        File f = new File(workingDir + File.separator + traceSubDir + File.separator + workerSubDir);
        File[] matchingFiles = f.listFiles((File dir, String name) -> name.endsWith(workerTraceSuffix));

        if (matchingFiles == null) {
            throw new FileNotFoundException("No workers traces to merge found.");
        } else {
            workersTraces = matchingFiles;
        }

        workersTracePath = new String[workersTraces.length];
        for (int i = 0; i < workersTracePath.length; ++i) {
            workersTracePath[i] = workersTraces[i].getAbsolutePath();
        }
    }

    public void merge() throws IOException {
        logger.debug("Parsing master sync events");
        HashMap<Integer, List<LineInfo>> masterSyncEvents = getSyncEvents(masterTracePath, -1);

        logger.debug("Proceeding to merge task traces into master which contains " + masterSyncEvents.size() + " lines.");
        for (File workerFile : workersTraces) {
            logger.debug("Merging worker " + workerFile);
            String workerFileName = workerFile.getName();
            String wID = "";

            for (int i = 0; workerFileName.charAt(i) != '_'; ++i) {
                wID += workerFileName.charAt(i);
            }

            Integer workerID = Integer.parseInt(wID);
            workerID++; // first worker is resource number 2

            List<String> cleanLines = getWorkerEvents(workerFile);
            HashMap<Integer, List<LineInfo>> workerSyncEvents = getSyncEvents(workerFile.getPath(), workerID);

            writeWorkerEvents(masterSyncEvents, workerSyncEvents, cleanLines, workerID);
            
            if (!debug) {
                logger.debug("Removing folder " + workingDir + File.separator + traceSubDir + File
                    .separator + workerSubDir);
                try{
                    removeFolder(workingDir + File.separator + traceSubDir + File.separator +
                            workerSubDir);
                } catch (Exception e) {
                    logger.warn("Could not remove python temporal tracing folder.\n" + e.toString());
                }
            }
        }
        masterWriter.close();
        logger.debug("Merging finished.");
    }

    private void removeFolder(String sandBox) throws IOException {
        File wdirFile = new File(sandBox);
        remove(wdirFile);
    }

    private void remove(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                for (File child : f.listFiles()) {
                    remove(child);
                }
            }
            Files.delete(f.toPath());
        }
    }

    private void add(HashMap<Integer, List<LineInfo>> map, Integer key, LineInfo newValue) {
        List<LineInfo> currentValue = map.computeIfAbsent(key, k -> new ArrayList<>());

        currentValue.add(newValue);
    }

    private HashMap<Integer, List<LineInfo>> getSyncEvents(String tracePath, Integer workerID) throws IOException {
        FileInputStream inputStream = null;
        Scanner sc = null;
        HashMap<Integer, List<LineInfo>> idToSyncInfo = new HashMap<>();
        try {
            inputStream = new FileInputStream(tracePath);
            sc = new Scanner(inputStream, "UTF-8");
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                Matcher m = syncPattern.matcher(line);
                if (m.find()) {
                    Integer wID = (workerID == -1) ? Integer.parseInt(m.group(WORKER_ID_INDEX)) : workerID;
                    String resourceID = m.group(R_ID_INDEX);
                    Long timestamp = Long.parseLong(m.group(TIMESTAMP_INDEX));

                    add(idToSyncInfo, wID, new LineInfo(resourceID, timestamp));
                }
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        } finally {
            if (inputStream != null) {
                inputStream.close();
            }
            if (sc != null) {
                sc.close();
            }
        }
        return idToSyncInfo;
    }

    private List<String> getWorkerEvents(File worker) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(worker.getAbsolutePath()), StandardCharsets.UTF_8);
        int startIndex = 1; // Remove header
        int endIndex = lines.size() - 1;

        return lines.subList(startIndex, endIndex);
    }

    private void writeWorkerEvents(HashMap<Integer, List<LineInfo>> masterSyncEvents, HashMap<Integer, List<LineInfo>> workerSyncEvents,
            List<String> eventsLine, Integer workerID) {

        logger.debug("Writing " + eventsLine.size() + " lines from worker " + workerID);
        LineInfo workerHeader = getWorkerInfo(masterSyncEvents.get(workerID), workerSyncEvents.get(workerID));

        for (String line : eventsLine) {
            String newEvent = updateEvent(workerHeader, line, workerID);
            masterWriter.println(newEvent);
        }
    }

    private String updateEvent(LineInfo workerHeader, String line, Integer workerID) {
        Matcher taskMatcher = workerThreadInfoPattern.matcher(line);
        String newLine = "";
        if (taskMatcher.find()) {

            Integer threadID = Integer.parseInt(taskMatcher.group(WORKER_THREAD_ID));
            Integer stateID = Integer.parseInt(taskMatcher.group(STATE_TYPE));
            String eventHeader = stateID + ":" + threadID + ":1:" + workerID + ":" + threadID;
            Long timestamp = workerHeader.getTimestamp() + Long.parseLong(taskMatcher.group(WORKER_TIMESTAMP));
            String lineInfo = taskMatcher.group(WORKER_LINE_INFO);
            newLine = eventHeader + ":" + timestamp + ":" + lineInfo;
        }
        return newLine;
    }

    private LineInfo getWorkerInfo(List<LineInfo> masterSyncEvents, List<LineInfo> workerSyncEvents) {

        LineInfo javaStart = masterSyncEvents.get(0);
        LineInfo javaEnd = masterSyncEvents.get(1);

        LineInfo workerStart = workerSyncEvents.get(0);
        LineInfo workerEnd = workerSyncEvents.get(1);

        Long javaTime = Math.abs(javaStart.getTimestamp() - javaEnd.getTimestamp());
        Long workerTime = Math.abs(workerStart.getTimestamp() - workerEnd.getTimestamp());

        Long overhead = (javaTime - workerTime) / 2;

        return new LineInfo(javaStart.resourceId, javaStart.getTimestamp() + overhead);

    }

}
