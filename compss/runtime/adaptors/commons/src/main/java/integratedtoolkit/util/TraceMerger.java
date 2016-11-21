package integratedtoolkit.util;

import integratedtoolkit.log.Loggers;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.*;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.*;
import java.util.regex.Matcher;
import java.util.regex.Pattern;


public class TraceMerger {

    protected static final Logger logger = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean debug = logger.isDebugEnabled();

    private static final Integer SYNC_TYPE = 8000666;
    private static final Integer R_ID_INDEX = 1;
    private static final Integer TIMESTAMP_INDEX = 2;
    private static final Integer TASK_ID_INDEX = 3;

    private static final String masterTraceSuffix = "_compss_trace_";
    private static final String taskTracePrefix = "task";
    private static final String traceExtension = ".prv";
    private static final String traceSubDir = "trace";
    private static final String taskSubDir = "tasks";
    private static String workingDir;

    private FileWriter fw;
    private BufferedWriter bw;
    private PrintWriter masterWriter;

    private String replaceRegex = "(^\\d+:\\d+:\\d+:\\d+:\\d+):(\\d+)";
    private Pattern replacePattern = Pattern.compile(replaceRegex);
    private String syncRegex = "(^\\d+:\\d+:\\d+:\\d+:\\d+):(\\d+).*:" + SYNC_TYPE + ":(\\d+)";
    private Pattern syncPattern = Pattern.compile(syncRegex);

    private File masterTrace;
    private File[] taskTraces;

    private String masterTracePath;
    private String[] tasksTracePath;

    private HashMap<Integer, LineInfo> taskIdToStartSyncInfo = new HashMap<>();
    private HashMap<Integer, LineInfo> taskIdToEndSyncInfo = new HashMap<>();


    private class LineInfo {

        private final String resourceId;
        private final Long timestamp;


        public LineInfo(String resourceID, Long timestamp) {
            this.resourceId = resourceID;
            this.timestamp = timestamp;
        }

        public String getResourceId() {
            return resourceId;
        }

        public Long getTimestamp() {
            return timestamp;
        }
    }


    public TraceMerger(String workingDir, String appName) throws IOException {
        initMasterTraceInfo(workingDir, appName);
        initTaskTracesInfo(workingDir);

        fw = new FileWriter(masterTracePath, true);
        bw = new BufferedWriter(fw);
        masterWriter = new PrintWriter(bw);

        logger.debug("Trace's merger initialization successful");

    }

    private void initMasterTraceInfo(String workingDir, String appName) throws FileNotFoundException {
        final String traceNamePrefix = appName + masterTraceSuffix;

        File f = new File(workingDir + File.separator + traceSubDir);
        File[] matchingFiles = f.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.startsWith(traceNamePrefix) && name.endsWith(traceExtension);
            }
        });

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

    private void initTaskTracesInfo(String workingDir) throws FileNotFoundException {
        TraceMerger.workingDir = workingDir;
        File f = new File(workingDir + File.separator + traceSubDir + File.separator + taskSubDir);
        File[] matchingFiles = f.listFiles(new FilenameFilter() {

            public boolean accept(File dir, String name) {
                return name.startsWith(taskTracePrefix) && name.endsWith(traceExtension);
            }
        });

        if (matchingFiles == null) {
            throw new FileNotFoundException("No task traces to merge found.");
        } else {
            taskTraces = matchingFiles;
        }

        tasksTracePath = new String[taskTraces.length];
        for (int i = 0; i < tasksTracePath.length; ++i) {
            tasksTracePath[i] = taskTraces[i].getAbsolutePath();
        }
    }

    public void merge() throws IOException {
        logger.debug("Parsing master sync events");
        parseMasterSyncEvents();
        logger.debug("Proceeding to merge task traces into master");
        for (File taskFile : taskTraces) {
            List<String> cleanLines = getTaskEvents(taskFile);
            updateTasksInfo(cleanLines);
            if (!debug) {
                if (!taskFile.delete()) {
                    logger.error("Error deleting trace file " + taskFile);
                }
            }
        }
        masterWriter.close();
        logger.debug("Merging finished,");
        if (!debug){
            File f = new File(TraceMerger.workingDir + File.separator + traceSubDir + File.separator + taskSubDir);
            if (f.delete()){
                logger.debug("Temporal task folder removed.");
            } else {
                logger.warn("Could not remove temporal task folder: " + f);
            }
        }
    }

    private void parseMasterSyncEvents() throws IOException {
        FileInputStream inputStream = null;
        Scanner sc = null;
        try {
            inputStream = new FileInputStream(masterTracePath);
            sc = new Scanner(inputStream, "UTF-8");
            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                Matcher m = syncPattern.matcher(line);
                if (m.find()) {
                    updateTaskIdSyncInfo(line);
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

    }

    private void updateTaskIdSyncInfo(String line) {
        Matcher matcher = syncPattern.matcher(line);
        if (matcher.find()) {
            String resourceID = matcher.group(R_ID_INDEX);

            Long timestamp = Long.parseLong(matcher.group(TIMESTAMP_INDEX));
            Integer taskID = Integer.parseInt(matcher.group(TASK_ID_INDEX));

            LineInfo lineInfo = new LineInfo(resourceID, timestamp);

            LineInfo startLine = taskIdToStartSyncInfo.get(taskID);
            if (startLine != null) {
                taskIdToEndSyncInfo.put(taskID, lineInfo);
            } else {
                taskIdToStartSyncInfo.put(taskID, lineInfo);
            }
        }
    }

    private List<String> getTaskEvents(File task) throws IOException {
        List<String> lines = Files.readAllLines(Paths.get(task.getAbsolutePath()), StandardCharsets.UTF_8);
        int startIndex = 0;
        int endIndex = lines.size() - 1;
        for (int i = 0; i < lines.size(); i++) {
            Matcher m = syncPattern.matcher(lines.get(i));
            if (m.find()) {
                startIndex = i;
                break;
            }
        }
        for (int i = lines.size() - 1; i > 0; i--) {
            Matcher m = syncPattern.matcher(lines.get(i));
            if (m.find()) {
                endIndex = i + 1;
                break;
            }
        }

        return lines.subList(startIndex, endIndex);
    }

    private void updateTasksInfo(List<String> eventsLine) {
        Matcher matcher = syncPattern.matcher(eventsLine.get(0));
        if (eventsLine.size() > 0 && matcher.find()) {

            Integer taskID = Integer.parseInt(matcher.group(TASK_ID_INDEX));
            Long offset = null;
            try {
                offset = getTaskOffset(taskID, eventsLine.get(0), eventsLine.get(eventsLine.size() - 1));
                String resourceID = taskIdToStartSyncInfo.get(taskID).getResourceId();
                for (String line : eventsLine) {
                    Matcher taskMatcher = replacePattern.matcher(line);
                    if (taskMatcher.find()) {
                        String newHeader = resourceID + ":" + (offset + Long.parseLong(taskMatcher.group(TIMESTAMP_INDEX)));
                        String newEvent = taskMatcher.replaceFirst(newHeader);

                        masterWriter.println(newEvent);
                    }

                }

            } catch (Exception e) {
                logger.error("Exception on uptade tasks info",  e);
            }

        }
    }

    private Long getTaskOffset(Integer taskID, String firstLine, String lastLine) throws Exception {
        Long outer_start = taskIdToStartSyncInfo.get(taskID).getTimestamp();
        Long outer_duration = (taskIdToEndSyncInfo.get(taskID).getTimestamp()) - (taskIdToStartSyncInfo.get(taskID).getTimestamp());

        Long inner_duration;

        Matcher endMatcher = syncPattern.matcher(lastLine);

        if (endMatcher.find()) {
            inner_duration = Long.parseLong(endMatcher.group(TIMESTAMP_INDEX));
            Long offset = outer_start + ((outer_duration - inner_duration) / 2);
            return offset;
        } else {
            logger.error("Could not calculate offset for task " + taskID);
            throw new Exception("Could not calculate offset for task " + taskID);
        }
    }

}
