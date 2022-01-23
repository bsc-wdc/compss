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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.types.PrvHeader;
import es.bsc.compss.util.types.PrvLine;
import es.bsc.compss.util.types.RowFile;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.PrintWriter;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class TraceMerger {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Info used for matching sync events
    private static final Integer SYNC_TYPE = TraceEventType.SYNC.code;
    private static final String SYNC_REGEX = "(^\\d+:\\d+:\\d+):(\\d+):(\\d+):(\\d+).*:" + SYNC_TYPE + ":(\\d+)";
    private static final Pattern SYNC_PATTERN = Pattern.compile(SYNC_REGEX);
    // Selectors for replace Pattern
    private static final Integer R_ID_INDEX = 1;
    private static final Integer TIMESTAMP_INDEX = 4;
    private static final Integer WORKER_ID_INDEX = 2;
    private static final Integer VALUE_INDEX = 5;

    // Could be wrong this regex (designed for matching tasks not workers)
    private static final String WORKER_THREAD_INFO_REGEX = "(^\\d+):(\\d+):(\\d+):(\\d+):(\\d+):(\\d+):(.*)";
    private static final Pattern WORKER_THREAD_INFO_PATTERN = Pattern.compile(WORKER_THREAD_INFO_REGEX); // NOSONAR
    private static final Integer STATE_TYPE = 1;
    private static final Integer WORKER_THREAD_ID = 2;
    private static final Integer WORKER_TIMESTAMP = 6;
    private static final Integer WORKER_LINE_INFO = 7;

    // CE identifier in .pcf
    private static final String TASKS_FUNC_TYPE_STRING = Integer.toString(TraceEventType.TASKS_FUNC.code);
    private static final String CE_FIXED_COUNTER = "0    " + TASKS_FUNC_TYPE_STRING + "    Task";
    private static final String CE_ID_VALUE_SEPARATOR = "      ";

    // Hardware counters pattern
    private static final String COUNTER_HEADER = "EVENT_TYPE";

    private static final String HW_FIXED_COUNTER = "7  41999999 Active hardware counter set";
    private static final String HW_COUNTER_LINE_HEADER = "7  4200";

    // File names patterns

    private File masterTrace;
    private String masterTracePath;
    private String masterTracePcfPath;
    private String masterTraceRowPath;
    private File[] workersTraces;
    private String[] workersTracePath;
    private String[] workersTracePcfPath;
    private String[] workersTraceRowPath;
    private PrintWriter masterWriter;
    private PrintWriter masterPcfWriter;


    private class LineInfo {

        private final String resourceId;

        // can be a timestamp and the number of cores of the worker (e.g. sync event
        // atend).
        private final Long value;

        private final Long timestamp;


        public LineInfo(String resourceID, Long timestamp, long value) {
            this.resourceId = resourceID;
            this.timestamp = timestamp;
            this.value = value;
        }

        public String getResourceId() {
            return this.resourceId;
        }

        public Long getTimestamp() {
            return this.timestamp;
        }

        public long getValue() {
            return this.value;
        }
    }


    /**
     * Initilizes master trace information .
     * 
     * @param masterDir Master trace's directory
     * @throws IOException Error managing files
     */
    protected void setUpMaster(File masterTrace) throws IOException {
        // get master File or create it
        if (masterTrace == null || !masterTrace.exists()) {
            throw new FileNotFoundException("Master trace not found.");
        }

        this.masterTrace = masterTrace;
        this.masterTracePath = this.masterTrace.getAbsolutePath();
        this.masterTracePcfPath =
            this.masterTracePath.replace(Tracer.TRACE_PRV_FILE_EXTENTION, Tracer.TRACE_PCF_FILE_EXTENTION);
        this.masterTraceRowPath =
            this.masterTracePath.replace(Tracer.TRACE_PRV_FILE_EXTENTION, Tracer.TRACE_ROW_FILE_EXTENTION);

        // Initialize the writer for the final master trace
        this.masterWriter = new PrintWriter(new FileWriter(this.masterTracePath, true));
        this.masterPcfWriter = new PrintWriter(new FileWriter(this.masterTracePcfPath, true));
    }

    /**
     * Initializes worker(s) trace information .
     * 
     * @param workersTraces list of traces to merge
     * @throws IOException Error managing files
     */
    protected void setUpWorkers(File[] workersTraces) throws IOException {
        // set this.workersTraces
        this.workersTraces = workersTraces;
        if (this.workersTraces == null || this.workersTraces.length == 0) {
            throw new FileNotFoundException("No workers traces to merge found.");
        }
        // setUp workers paths
        this.workersTracePath = new String[this.workersTraces.length];
        this.workersTracePcfPath = new String[this.workersTraces.length];
        this.workersTraceRowPath = new String[this.workersTraces.length];
        for (int i = 0; i < this.workersTracePath.length; ++i) {
            this.workersTracePath[i] = this.workersTraces[i].getAbsolutePath();
            this.workersTracePcfPath[i] =
                this.workersTracePath[i].replace(Tracer.TRACE_PRV_FILE_EXTENTION, Tracer.TRACE_PCF_FILE_EXTENTION);
            this.workersTraceRowPath[i] =
                this.workersTracePath[i].replace(Tracer.TRACE_PRV_FILE_EXTENTION, Tracer.TRACE_ROW_FILE_EXTENTION);
        }
    }

    /**
     * Adds the workers PRV files lines into the master PRV file .
     * 
     * @throws Exception errors managing files
     */
    protected void mergePRVsWithTraceNumAndSyncEvents() throws Exception {
        LOGGER.debug("Parsing master sync events");
        Map<Integer, List<LineInfo>> masterSyncEvents = getSyncEvents(this.masterTracePath, -1);
        LOGGER.debug("Merging task traces into master which contains " + masterSyncEvents.size() + " lines.");
        for (File workerFile : this.workersTraces) {
            LOGGER.debug("Merging worker " + workerFile);
            String workerFileName = workerFile.getName();
            String wID = "";
            for (int i = 0; workerFileName.charAt(i) != '_'; ++i) {
                wID += workerFileName.charAt(i);
            }
            Integer workerID = Integer.parseInt(wID);
            workerID++; // first worker is resource number 2
            List<String> cleanLines = getWorkerEvents(workerFile);
            Map<Integer, List<LineInfo>> workerSyncEvents = getSyncEvents(workerFile.getPath(), workerID);
            writeWorkerEvents(masterSyncEvents, workerSyncEvents, cleanLines, workerID);
        }
        this.masterWriter.close();
    }

    /**
     * Return true if all the elements are not null.
     */
    private boolean isEmpty(String[] array) {
        for (String s : array) {
            if (s != null) {
                return false;
            }
        }
        return true;
    }

    /**
     * Adds the workers PRV files lines into the master PRV file .
     *
     * @throws Exception errors managing files
     */
    protected void mergePRVsIntoNewFile() throws Exception {
        LOGGER.debug("Generating trace header");
        if (DEBUG) {
            LOGGER.debug("Adding data from .prv header from file: " + this.workersTraceRowPath[0]);
        }
        BufferedReader[] readersArray = new BufferedReader[this.workersTraces.length];

        // Next line to write from each of the agents, we have to read in paralel to mantain the order
        String[] lines = new String[readersArray.length];
        for (int i = 0; i < this.workersTraces.length; i++) {
            LOGGER.debug("Adding data from .prv header from file: " + this.workersTraceRowPath[i]);
            readersArray[i] = new BufferedReader(new FileReader(this.workersTraces[i]));
        }

        // we use the first worker as base trace header
        String masterTrace = readersArray[0].readLine();
        PrvHeader masterHeader = new PrvHeader(masterTrace);

        for (int i = 1; i < this.workersTraces.length; i++) {
            String headerString = readersArray[i].readLine();
            PrvHeader header = new PrvHeader(headerString);
            masterHeader.addAsAplication(header);
        }

        for (int i = 0; i < this.workersTraces.length; i++) {
            lines[i] = readersArray[i].readLine();
        }

        this.masterWriter.println(masterHeader.toString());

        while (!isEmpty(lines)) {
            int earliestLinePos = 0;
            earliestLinePos = 0;
            for (int i = 0; i < lines.length; i++) {
                if (lines[i] != null && !lines[i].isEmpty()) {
                    PrvLine prvLine = new PrvLine(lines[i]);
                    if (prvLine.goesBefore(lines[earliestLinePos])) {
                        earliestLinePos = i;
                    }
                }
            }
            String toWrite = lines[earliestLinePos];
            lines[earliestLinePos] = readersArray[earliestLinePos].readLine();
            this.masterWriter.println(toWrite);
        }
        for (int i = 0; i < this.workersTraces.length; i++) {
            readersArray[i].close();
        }
        this.masterWriter.close();
    }

    /**
     * Adds the workers PRV files lines into the master PRV file .
     *
     * @throws Exception errors managing files
     */
    protected void mergeROWsIntoNewFile() throws Exception {
        LOGGER.debug("Merging .row files");
        LOGGER.debug("Adding data from .row file: " + this.workersTraceRowPath[0]);
        File firstFile = new File(this.workersTraceRowPath[0]);
        RowFile masterRow = new RowFile(firstFile);
        for (int i = 1; i < this.workersTraceRowPath.length; i++) {
            LOGGER.debug("Adding data from .row file: " + this.workersTraceRowPath[i]);
            File file = new File(this.workersTraceRowPath[i]);
            RowFile rowFile = new RowFile(file);
            masterRow.mergeAgentRow(rowFile, i + 1); // Agent 0 has id 1 and so on
        }
        File masterFile = new File(masterTraceRowPath);
        masterFile.createNewFile();
        masterRow.printInfo(masterFile);
    }

    protected void mergePCFsHardwareCounters() throws IOException {
        // Get master hardware counters
        LOGGER.debug("Merging PCF Hardware Counters into master");
        ArrayList<String> masterHWCounters = getHWCounters(this.masterTracePcfPath);
        // Get all workers hardware counters
        ArrayList<ArrayList<String>> allWorkersHWCounters = new ArrayList<ArrayList<String>>();
        for (String workerPcf : this.workersTracePcfPath) {
            allWorkersHWCounters.add(getHWCounters(workerPcf));
        }
        // Extract unique counters
        ArrayList<String> newHWCounters = new ArrayList<String>();
        for (ArrayList<String> workerHWCounters : allWorkersHWCounters) {
            int differentLines = 0;
            for (String line : workerHWCounters) {
                if (!masterHWCounters.contains(line) && !newHWCounters.contains(line)) {
                    if (DEBUG) {
                        LOGGER.debug("Found new PCF counter line" + line);
                    }
                    newHWCounters.add(line);
                    differentLines++;
                }
            }
            if (DEBUG) {
                LOGGER.debug("Analised worker had " + differentLines + " lines to be included");
            }
        }
        // Append new hardware counters labels to master pcf
        if (newHWCounters.size() > 0) {
            if (DEBUG) {
                LOGGER.debug("Adding " + newHWCounters.size() + " new counters to master PCF file.");
            }
            this.masterPcfWriter.println(TraceMerger.COUNTER_HEADER);
            if (masterHWCounters.size() == 0) {
                // The master did not contain hardware counter labels: requires fixed
                if (DEBUG) {
                    LOGGER.debug("Master PCF did not contain any hardware counter.");
                }
                this.masterPcfWriter.println(HW_FIXED_COUNTER);
            }
            for (String line : newHWCounters) {
                this.masterPcfWriter.println(line);
            }
        } else {
            if (DEBUG) {
                LOGGER.debug("No hardware counters to include in PCF.");
            }
        }
        this.masterPcfWriter.close();
    }

    /**
     * Creates a global CE in the master PCF and updates the values of the workers PRV to match this new PCF and reflect
     * the agent identifier number.
     * 
     * @throws Exception Errors in the files
     */
    protected void createPRVswithGlobalCE() throws Exception {

        // Get map CE id -> CE name from the workers
        LOGGER.debug("Updating workers PRVs with global PCF index");
        List<Map<String, String>> workersCEIndex = new ArrayList<Map<String, String>>();
        for (String workerPcf : this.workersTracePcfPath) {
            workersCEIndex.add(getCE(workerPcf));
        }

        // Creating global CE index
        Map<String, String> globalCE = createGlobalCoreElementsIndex(workersCEIndex);

        // Creating global PCF file
        createGlobalPCF(globalCE);

        // Creating local PRV files with global CE index
        for (int i = 0; i < this.workersTracePath.length; i++) {
            File translatedPRV = new File(workersTracePath[i] + "_tmp_GlobalCE");
            boolean fileCreated = translatedPRV.createNewFile();
            if (!fileCreated) {
                throw new Exception("ERROR: couldn't create new PRV file with global CE identifiers at "
                    + translatedPRV.getAbsolutePath());
            }
            PrintWriter writer = new PrintWriter(new FileWriter(translatedPRV, true));
            Map<String, String> workerCE = workersCEIndex.get(i);
            int wokerId = i + 1;

            // Applies writeTranslatedPRVLine to each line of the file
            BufferedReader br = new BufferedReader(new FileReader(workersTracePath[i]));
            String line;
            while ((line = br.readLine()) != null) {
                writeTranslatedPRVLine(line, wokerId, globalCE, workerCE, writer);
            }
            br.close();

            writer.close();
            translatedPRV.renameTo(this.workersTraces[i]);
        }
    }

    /**
     * Returns a map from the CE names to a number to act as a global CE index for the merged .pfc.
     */
    protected Map<String, String> createGlobalCoreElementsIndex(List<Map<String, String>> workersCEIndex)
        throws Exception {
        // get master CE and maximum CE value
        LOGGER.debug("Creating global CE index");
        Map<String, String> globalCE = new HashMap<String, String>();
        globalCE.put("End", "0");
        int maxCEValue = 1;

        // add new CE fond in workers
        for (Map<String, String> workerCE : workersCEIndex) {
            for (Map.Entry<String, String> ce : workerCE.entrySet()) {
                String value = ce.getValue();
                if (!globalCE.containsKey(value)) {
                    globalCE.put(value, Integer.toString(maxCEValue));
                    LOGGER.debug("Added " + value + " to global CE index with id: " + maxCEValue);
                    maxCEValue++;
                }
            }
        }
        return globalCE;
    }

    private void createGlobalPCF(Map<String, String> globalCE) throws IOException {
        LOGGER.debug("Creating global PCF index");
        List<String> lines = Files.readAllLines(Paths.get(this.workersTracePcfPath[0]), StandardCharsets.UTF_8);
        int currentLineNumber = 0;
        // Copy non CE lines
        while (currentLineNumber < lines.size() && !CE_FIXED_COUNTER.equals(lines.get(currentLineNumber))) {
            this.masterPcfWriter.println(lines.get(currentLineNumber));
            currentLineNumber++;
        }
        // Ignore de CE lines
        while (currentLineNumber < lines.size() - 1 && !lines.get(currentLineNumber).isEmpty()) {
            currentLineNumber++;
        }

        this.masterPcfWriter.println(CE_FIXED_COUNTER);
        this.masterPcfWriter.println("VALUES");
        // Write global CE values
        for (Map.Entry<String, String> ce : globalCE.entrySet()) {
            this.masterPcfWriter.println(ce.getValue() + CE_ID_VALUE_SEPARATOR + ce.getKey());
        }
        // Copy non CE lines
        while (currentLineNumber < lines.size()) {
            this.masterPcfWriter.println(lines.get(currentLineNumber));
            currentLineNumber++;
        }

        this.masterPcfWriter.close();
    }

    /**
     * Writes the PRV line line with the writer writer and translates it if it's a core element using globalCE and
     * workerCE.
     * 
     * @throws Exception Errors in the files
     */

    private static void writeTranslatedPRVLine(String line, int workerId, Map<String, String> globalCE,
        Map<String, String> workerCE, PrintWriter writer) throws Exception {
        if (!line.startsWith("#")) {
            PrvLine prvLine = new PrvLine(line);
            prvLine.translateLineToGlobalIndex(TASKS_FUNC_TYPE_STRING, globalCE, workerCE);
            prvLine.setAgentNumber(Integer.toString(workerId));
            writer.println(prvLine.toString());
        } else {
            writer.println(line);
        }
    }

    protected void removeTmpAgentFiles() throws IOException {
        for (String path : this.workersTracePath) {
            removeFolder(new File(path).getParent());
        }
    }

    protected static void removeFolder(String sandBox) throws IOException {
        File wdirFile = new File(sandBox);
        remove(wdirFile);
    }

    private static void remove(File f) throws IOException {
        if (f.exists()) {
            if (f.isDirectory()) {
                for (File child : f.listFiles()) {
                    remove(child);
                }
            }
            Files.delete(f.toPath());
        }
    }

    private static void add(Map<Integer, List<LineInfo>> map, Integer key, LineInfo newValue) {
        List<LineInfo> currentValue = map.computeIfAbsent(key, k -> new ArrayList<>());
        currentValue.add(newValue);
    }

    private static List<String> getWorkerEvents(File worker) throws IOException {
        if (DEBUG) {
            LOGGER.debug("Getting worker events from: " + worker.getAbsolutePath());
        }
        List<String> lines = Files.readAllLines(Paths.get(worker.getAbsolutePath()), StandardCharsets.UTF_8);
        int startIndex = 1; // Remove header
        int endIndex = lines.size();

        return lines.subList(startIndex, endIndex);
    }

    private Map<Integer, List<LineInfo>> getSyncEvents(String tracePath, Integer workerID) throws IOException {
        if (DEBUG) {
            LOGGER.debug("Getting sync events from: " + tracePath + " for worker " + workerID);
        }
        Map<Integer, List<LineInfo>> idToSyncInfo = new HashMap<>();
        try (FileInputStream inputStream = new FileInputStream(tracePath);
            Scanner sc = new Scanner(inputStream, "UTF-8")) {

            while (sc.hasNextLine()) {
                String line = sc.nextLine();
                Matcher m = SYNC_PATTERN.matcher(line);
                if (m.find()) {
                    Integer wID = (workerID == -1) ? Integer.parseInt(m.group(WORKER_ID_INDEX)) : workerID;
                    String resourceID = m.group(R_ID_INDEX);
                    Long timestamp = Long.parseLong(m.group(TIMESTAMP_INDEX));
                    Long value = Long.parseLong(m.group(VALUE_INDEX));

                    add(idToSyncInfo, wID, new LineInfo(resourceID, timestamp, value));
                }
            }
            // note that Scanner suppresses exceptions
            if (sc.ioException() != null) {
                throw sc.ioException();
            }
        }
        // Exceptions are raised automatically, we add the try clause to automatically
        // close the streams

        return idToSyncInfo;
    }

    private void writeWorkerEvents(Map<Integer, List<LineInfo>> masterSyncEvents,
        Map<Integer, List<LineInfo>> workerSyncEvents, List<String> eventsLine, Integer workerID) throws Exception {

        LineInfo workerHeader = getWorkerInfo(masterSyncEvents.get(workerID), workerSyncEvents.get(workerID));

        LOGGER.debug("Writing " + eventsLine.size() + " lines from worker " + workerID + " with "
            + workerHeader.getValue() + " threads");

        for (String line : eventsLine) {
            String newEvent = updateEvent(workerHeader, line, workerID);
            if (!newEvent.isEmpty()) {
                this.masterWriter.println(newEvent);
            }
        }
    }

    private static String updateEvent(LineInfo workerHeader, String line, Integer workerID) {
        int numThreads = (int) workerHeader.getValue();
        Matcher taskMatcher = WORKER_THREAD_INFO_PATTERN.matcher(line);
        String newLine = "";
        if (taskMatcher.find()) {
            Integer threadID = Integer.parseInt(taskMatcher.group(WORKER_THREAD_ID));
            Integer stateID = Integer.parseInt(taskMatcher.group(STATE_TYPE));
            int newThreadID = threadID;
            if (threadID > 1) {
                newThreadID = numThreads + 4 - threadID;
            }
            String eventHeader = stateID + ":" + newThreadID + ":1:" + workerID + ":" + newThreadID;
            Long timestamp = workerHeader.getTimestamp() + Long.parseLong(taskMatcher.group(WORKER_TIMESTAMP));
            String lineInfo = taskMatcher.group(WORKER_LINE_INFO);
            newLine = eventHeader + ":" + timestamp + ":" + lineInfo;
        }
        return newLine;
    }

    private LineInfo getWorkerInfo(List<LineInfo> masterSyncEvents, List<LineInfo> workerSyncEvents) throws Exception {
        if (masterSyncEvents.size() < 3) {
            throw new Exception("ERROR: Malformed master trace. Master sync events not found");
        }
        if (workerSyncEvents.size() < 3) {
            throw new Exception("ERROR: Malformed worker trace. Worker sync events not found");
        }

        LineInfo javaStart = masterSyncEvents.get(0); // numero de threads del master al arrancar el runtime
        // LineInfo javaEnd = masterSyncEvents.get(1);
        LineInfo javaSync = masterSyncEvents.get(2);
        // LineInfo workerStart = workerSyncEvents.get(0);
        // LineInfo workerEnd = workerSyncEvents.get(1);
        LineInfo workerSync = workerSyncEvents.get(2);

        // Take the sync event emitted by the runtime and worker and compare their value
        // (timestamp)
        // The worker events real start is the difference between java and the worker
        // minus the timestamp difference.
        Long syncDifference = Math.abs((javaSync.getValue() / 1000) - workerSync.getValue());
        Long realStart = Math.abs(javaSync.getTimestamp() - workerSync.getTimestamp()) - syncDifference;

        return new LineInfo(javaStart.getResourceId(), realStart, javaStart.getValue());
    }

    private static ArrayList<String> getHWCounters(String tracePcfPath) throws IOException {
        if (DEBUG) {
            LOGGER.debug("Getting pcf hw counters from: " + tracePcfPath);
        }
        ArrayList<String> hwCounters = new ArrayList<String>();
        List<String> lines = Files.readAllLines(Paths.get(tracePcfPath), StandardCharsets.UTF_8);
        for (String line : lines) {
            if (line.startsWith(TraceMerger.HW_COUNTER_LINE_HEADER)) {
                hwCounters.add(line);
            }
        }
        return hwCounters;
    }

    private static Map<String, String> getCE(String tracePcfPath) throws Exception {
        if (DEBUG) {
            LOGGER.debug("Getting PCF CE from: " + tracePcfPath);
        }
        Pattern numberPattern = Pattern.compile("[0-9]+");
        Map<String, String> coreElements = new HashMap<String, String>();
        List<String> lines = Files.readAllLines(Paths.get(tracePcfPath), StandardCharsets.UTF_8);
        int headerLine = lines.indexOf(CE_FIXED_COUNTER);
        if (headerLine != -1) {
            int ceLine = headerLine + 2;
            while (ceLine < lines.size() - 1 && !lines.get(ceLine).isEmpty()) {
                String[] values = lines.get(ceLine).split(CE_ID_VALUE_SEPARATOR);
                if (values.length != 2 || values[1].isEmpty() || !numberPattern.matcher(values[0]).matches()) {
                    throw new Exception("ERROR: Malformed CE in PFC " + tracePcfPath + "  line " + ceLine);
                }
                coreElements.put(values[0], values[1]);
                ceLine++;
            }
        }
        return coreElements;
    }

    private static Map<String, String> getReversedCE(String tracePcfPath) throws Exception {
        if (DEBUG) {
            LOGGER.debug("Getting PCF CE from: " + tracePcfPath);
        }
        Pattern numberPattern = Pattern.compile("[0-9]+");
        Map<String, String> coreElements = new HashMap<String, String>();
        List<String> lines = Files.readAllLines(Paths.get(tracePcfPath), StandardCharsets.UTF_8);
        int headerLine = lines.indexOf(CE_FIXED_COUNTER);
        if (headerLine != -1) {
            int ceLine = headerLine + 2;
            while (ceLine < lines.size() - 1 && !lines.get(ceLine).isEmpty()) {
                String[] values = lines.get(ceLine).split(CE_ID_VALUE_SEPARATOR);
                if (values.length != 2 || values[1].isEmpty() || !numberPattern.matcher(values[0]).matches()) {
                    throw new Exception("ERROR: Malformed CE in PFC " + tracePcfPath + "  line " + ceLine);
                }
                coreElements.put(values[1], values[0]);
                ceLine++;
            }
        }
        return coreElements;
    }

    protected static void copyFile(File sourceFile, File destinationFile) throws IOException {
        try (InputStream in = new FileInputStream(sourceFile);
            OutputStream out = new FileOutputStream(destinationFile)) {
            byte[] buf = new byte[1024];
            int length;
            while ((length = in.read(buf)) > 0) {
                out.write(buf, 0, length);
            }
        }
    }
}
