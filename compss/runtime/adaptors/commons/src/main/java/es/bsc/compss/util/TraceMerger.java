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
    private static final Integer SYNC_TYPE = Tracer.SYNC_TYPE;
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

    private static final String AGENT_IDENTIFIER_REGEX = "^agent\\d";
    private static final Pattern AGENT_IDENTIFIER_PATTERN = Pattern.compile(AGENT_IDENTIFIER_REGEX);

    // Hardware counters pattern
    private static final String COUNTER_HEADER = "EVENT_TYPE";

    private static final String HW_FIXED_COUNTER = "7  41999999 Active hardware counter set";
    private static final String HW_COUNTER_LINE_HEADER = "7  4200";

    private static final String TASKS_FUNC_TYPE_STRING = Integer.toString(Tracer.TASKS_FUNC_TYPE);
    private static final String CE_FIXED_COUNTER = "0    " + TASKS_FUNC_TYPE_STRING + "    Task";
    private static final String CE_ID_VALUE_SEPARATOR = "      ";

    // File names patterns
    protected static final String MASTER_TRACE_SUFFIX = "_compss_trace_";
    protected static final String TRACE_EXTENSION = ".prv";
    protected static final String TRACE_PCF_EXTENSION = ".pcf";
    protected static final String TRACE_ROW_EXTENSION = ".row";

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
    private PrintWriter masterRowWriter;


    private class LineInfo {

        private final String resourceId;

        // can be a timestamp and the number of cores of the worker (e.g. sync event atend).
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
        this.masterTracePcfPath = this.masterTracePath.replace(TRACE_EXTENSION, TRACE_PCF_EXTENSION);
        this.masterTracePcfPath = this.masterTracePath.replace(TRACE_EXTENSION, TRACE_PCF_EXTENSION);
        this.masterTraceRowPath = this.masterTracePath.replace(TRACE_EXTENSION, TRACE_ROW_EXTENSION);

        // Initialize the writer for the final master trace
        this.masterWriter = new PrintWriter(new FileWriter(this.masterTracePath, true));
        this.masterPcfWriter = new PrintWriter(new FileWriter(this.masterTracePcfPath, true));
        this.masterRowWriter = new PrintWriter(new FileWriter(this.masterTraceRowPath, true));
    }

    /**
     * Initilizes worker(s) trace information .
     * 
     * @param workingDir Working directory
     * @throws IOException Error managing files
     */
    protected void setUpWorkers(File[] workersTraces) throws IOException {
        // set this.workersTraces
        this.workersTraces = workersTraces;
        System.out.println("______ this.workersTraces[0] " + this.workersTraces[0]);
        System.out.println("______ this.workersTraces == null " + (this.workersTraces == null));
        if (this.workersTraces == null || this.workersTraces.length == 0) {
            throw new FileNotFoundException("No workers traces to merge found.");
        }

        // setUp workers paths
        this.workersTracePath = new String[this.workersTraces.length];
        this.workersTracePcfPath = new String[this.workersTraces.length];
        this.workersTraceRowPath = new String[this.workersTraces.length];
        for (int i = 0; i < this.workersTracePath.length; ++i) {
            System.out.println("______this.workersTraces[i]" + this.workersTraces[i]);

            this.workersTracePath[i] = this.workersTraces[i].getAbsolutePath();
            System.out.println("______this.workersTracePath[i]" + this.workersTracePath[i]);

            this.workersTracePcfPath[i] = this.workersTracePath[i].replace(TRACE_EXTENSION, TRACE_PCF_EXTENSION);
            this.workersTraceRowPath[i] = this.workersTracePath[i].replace(TRACE_EXTENSION, TRACE_ROW_EXTENSION);
            System.out.println("______this.workersTracePcfPath" + this.workersTracePcfPath);
        }
        System.out.println("______acabado bucle" + Arrays.toString(this.workersTracePath));
        System.out.println("______acabado bucle" + Arrays.toString(this.workersTracePcfPath));
    }

    /**
     * Adds the workers PRV files lines into the master PRV file .
     * 
     * @throws Exception errors managing files
     */
    protected void mergePRVsWithTraceNumAndSyncEvents() throws Exception {
        System.out.println("Parsing master sync events");
        Map<Integer, List<LineInfo>> masterSyncEvents = getSyncEvents(this.masterTracePath, -1);
        System.out.println("Merging task traces into master which contains " + masterSyncEvents.size() + " lines.");
        for (File workerFile : this.workersTraces) {
            System.out.println("Merging worker " + workerFile);
            String workerFileName = workerFile.getName();
            String wID = "";

            for (int i = 0; workerFileName.charAt(i) != '_'; ++i) {
                wID += workerFileName.charAt(i);
            }

            System.out.println("______wID = " + wID);

            Integer workerID = Integer.parseInt(wID);
            workerID++; // first worker is resource number 2

            List<String> cleanLines = getWorkerEvents(workerFile);
            Map<Integer, List<LineInfo>> workerSyncEvents = getSyncEvents(workerFile.getPath(), workerID);
            writeWorkerEvents(masterSyncEvents, workerSyncEvents, cleanLines, workerID);
        }
        this.masterWriter.close();
    }

    /**
     * Adds the workers PRV files lines into the master PRV file .
     * /*  */
     * @throws Exception errors managing files
     */
    protected void mergePRVs() throws Exception {
        for (File workerFile : this.workersTraces) {
            System.out.println("______copiando eventos directamente de " + workerFile.getAbsolutePath() + " to "
                + this.masterTracePath);
            List<String> events = getWorkerEvents(workerFile);
            for (String line : events) {
                System.out.println("      ______copiando PRV linea " + line);
                this.masterWriter.println(line);
            }
        }
        this.masterWriter.close();
    }

    /**
     * Removes temporal files.
     */
    public void removeTemporalFiles() {
        System.out.println("Removing folder " + "");
        try {
            removeFolder("");
        } catch (IOException ioe) {
            LOGGER.warn("Could not remove python temporal tracing folder" + ioe.toString());
        }
    }

    protected void mergePCFsHardwareCounters() throws IOException {
        // Get master hardware counters
        System.out.println("Merging PCF Hardware Counters into master");
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
                        System.out.println("Found PCF counter line not at master: " + line);
                    }
                    newHWCounters.add(line);
                    differentLines++;
                }
            }
            if (DEBUG) {
                System.out.println("Analised worker had " + differentLines + " lines to be included");
            }
        }
        // Append new hardware counters labels to master pcf
        if (newHWCounters.size() > 0) {
            if (DEBUG) {
                System.out.println("Adding " + newHWCounters.size() + " new counters to master PCF file.");
            }
            this.masterPcfWriter.println(TraceMerger.COUNTER_HEADER);
            if (masterHWCounters.size() == 0) {
                // The master did not contain hardware counter labels: requires fixed
                if (DEBUG) { // ______mirar como hago para que el comentario sea generico
                    System.out.println("Master PCF did not contain any hardware counter.");
                }
                this.masterPcfWriter.println(HW_FIXED_COUNTER);
            }
            for (String line : newHWCounters) {
                this.masterPcfWriter.println(line);
            }
        } else {
            if (DEBUG) { // ______mirar como hago para que el comentario sea generico
                System.out.println("No hardware counters to include in PCF.");
            }
        }
        this.masterPcfWriter.close();
    }

    /**
     * Creates a global CE in the master PCF and updates the values of he workers PRV to match this new PCF.
     * 
     * @throws Exception Errors in the files
     */
    protected void createPRVswithGlobalCE() throws Exception {
        System.out.println("______masterTrace" + this.masterTrace);
        System.out.println("______workersTraces" + this.workersTraces);
        System.out.println("______masterTracePath" + this.masterTracePath);
        System.out.println("______masterTracePcfPath" + this.masterTracePcfPath);
        System.out.println("______workersTracePath" + this.workersTracePath);
        System.out.println("______workersTracePcfPath" + this.workersTracePcfPath);
        System.out.println("______masterWriter" + this.masterWriter);
        System.out.println("______masterPcfWriter" + this.masterPcfWriter);

        // Get map CE id -> CE name from the workers
        System.out.println("Updating workers PRVs with global PCF index");
        List<Map<Integer, String>> workersCEIndex = new ArrayList<Map<Integer, String>>();
        for (String workerPcf : this.workersTracePcfPath) {
            System.out.println("______generando workersCEIndex " + workerPcf);
            workersCEIndex.add(getCE(workerPcf)); // :____esta al revers reversed y no
        }

        // Get map CE id -> CE name from the workers
        System.out.println("______generando masterReversedCEIndex " + this.masterTracePcfPath);
        Map<String, Integer> masterReversedCEIndex = getReversedCE(this.masterTracePcfPath);

        // Creating global CE index
        Map<String, Integer> globalCE = createGlobalCoreElementsIndex(masterReversedCEIndex, workersCEIndex);

        // Creating global PCF file
        createGlobalPCF(globalCE);

        // Creating local PRV files with global CE index
        for (int i = 0; i < this.workersTracePath.length; i++) {
            System.out.println("______intentando crear archivo");
            System.out.println("______nombre del archivo: "
                + workersTracePath[i].substring(0, workersTracePath[i].lastIndexOf('.')) + "translatedPRV.prv");
            File translatedPRV =
                new File(workersTracePath[i].substring(0, workersTracePath[i].lastIndexOf('.')) + "translatedPRV.prv");
            System.out.println("______creando translated PRV " + translatedPRV.getAbsolutePath());
            boolean fileCreated = translatedPRV.createNewFile();
            if (!fileCreated) { // ______he de borrar estos archivos si no es DEBUG luego
                throw new Exception("ERROR: couldn't create new PRV file with global CE identifiers at "
                    + translatedPRV.getAbsolutePath());
            }
            PrintWriter writer = new PrintWriter(new FileWriter(translatedPRV, true));
            Map<Integer, String> workerCE = workersCEIndex.get(i);
            int wokerId = getAgentIdFromTrace(this.workersTracePath[i]);
            Files.lines(Paths.get(this.workersTracePath[i]))
                .forEach(l -> writeTranslatedPRVLine(l, wokerId, globalCE, workerCE, writer));
            // ______comentar lambda vodoo
            writer.close();
            this.workersTraces[i] = translatedPRV;
            this.workersTracePath[i] = translatedPRV.getAbsolutePath();
            this.workersTracePcfPath[i] = this.workersTracePath[i].replace(TRACE_EXTENSION, TRACE_PCF_EXTENSION);
        }
    }

    protected void createGlobalRow() throws IOException {
        // List<List<String>> allWorkerLines = new ArrayList<List<String>>(); //______quizas es un poco aventuresco
        // cargar todo pero deberian ser chiquititos(?)
        // int[] lastReadLineNumber;
        // for(String workerRowPath : this.workersTraceRowPath){
        // List<String> lines = Files.readAllLines(Paths.get(workerRowPath), StandardCharsets.UTF_8);
        // allWorkerLines.add(lines);
        // }
        // for(int i = 0; i < allWorkerLines.size(); i++) {
        // int j = 0;
        // while(j < allWorkerLines.get(i).size() && ){

        // }
        // }
    }

    private static int getAgentIdFromTrace(String path) throws Exception {
        String[] dirs = path.split("/");
        for (int i = 0; i < dirs.length; i++) {
            if (AGENT_IDENTIFIER_PATTERN.matcher(dirs[i]).matches()) {
                int agentNum = Character.getNumericValue(dirs[i].charAt(dirs[i].length() - 1));
                if (agentNum < 1 || agentNum > 9) {
                    throw new Exception("Malformed agent trace path " + path
                        + " expected directory called agent<agentNumber>. Found " + dirs[i]);
                }
                System.out.println("El agentId de la traza " + path + " es " + agentNum);
                return agentNum;
            }
        }
        throw new Exception(
            "Malformed agent trace path " + path + " expected directory called agent<agentNumber>. Found ");
    }

    protected Map<String, Integer> createGlobalCoreElementsIndex(Map<String, Integer> masterReversedCEIndex,
        List<Map<Integer, String>> workersCEIndex) throws Exception {
        // get master CE and maximum CE value
        System.out.println("Creating global PCF index");
        Map<String, Integer> globalCE = masterReversedCEIndex;
        int maxCEValue = 0;
        for (Map.Entry<String, Integer> entry : globalCE.entrySet()) {
            if (entry.getValue() > maxCEValue) {
                maxCEValue = entry.getValue();
            }
        }

        // add new CE fond in workers
        for (Map<Integer, String> workerCE : workersCEIndex) {
            for (Map.Entry<Integer, String> ce : workerCE.entrySet()) {
                if (!globalCE.containsKey(ce.getValue())) {
                    globalCE.put(ce.getValue(), maxCEValue);
                    maxCEValue++;
                }
            }
        }
        return globalCE;
    }

    private void createGlobalPCF(Map<String, Integer> globalCE) throws IOException {
        System.out.println("______Creating global PCF starting from " + this.workersTracePcfPath[0] + " to "
            + this.masterTracePcfPath);
        List<String> lines = Files.readAllLines(Paths.get(this.workersTracePcfPath[0]), StandardCharsets.UTF_8);
        int headerLine = lines.indexOf(CE_FIXED_COUNTER);
        int currentLineNumber = 0;
        while (currentLineNumber <= headerLine && currentLineNumber < lines.size()) { // copy non CE
            this.masterPcfWriter.println(lines.get(currentLineNumber));
            currentLineNumber++;
        }
        while (currentLineNumber < lines.size() - 1 && !lines.get(currentLineNumber).isEmpty()) { // Ignore de CE lines
            currentLineNumber++;
        }
        this.masterPcfWriter.println("VALUES");
        for (Map.Entry<String, Integer> ce : globalCE.entrySet()) { // Write global CE values
            this.masterPcfWriter.println(ce.getValue() + CE_ID_VALUE_SEPARATOR + ce.getKey());
        }
        while (currentLineNumber < lines.size()) { // copy non CE lines
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

    private static void writeTranslatedPRVLine(String line, int workerId, Map<String, Integer> globalCE,
        Map<Integer, String> workerCE, PrintWriter writer) {
        String[] values = line.split(":");
        System.out.println("______asignando a la linea " + line + " worker id " + workerId);
        // Position of the first event group identifier (position 5 is timestamp)
        for (int i = 6; i < values.length; i += 2) {
            if (TASKS_FUNC_TYPE_STRING.equals(values[i])) {
                Integer workerCeId = Integer.parseInt(values[i + 1]);
                String ceName = workerCE.get(workerCeId);
                Integer globalCEId = globalCE.get(ceName);
                values[i + 1] = Integer.toString(globalCEId);
                System.out.println("CE id " + workerCeId + " hacia referencia al CE  " + ceName + " convirtiendolo en  "
                    + globalCEId + " del CE global ");
            }
        }
        System.out.println("______procesando linea" + line);
        if (values.length > 1) {
            values[3] = Integer.toString(workerId);
            writer.println(String.join(":", values));
            System.out.println("______escribiendo en translatedPRV " + String.join(":", values));
        } else {
            writer.println(line);
            System.out.println("______escribiendo en translatedPRV " + line);
        }
    }

    private static void removeFolder(String sandBox) throws IOException {
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
            System.out.println("Getting worker events from: " + worker.getAbsolutePath());
        }
        List<String> lines = Files.readAllLines(Paths.get(worker.getAbsolutePath()), StandardCharsets.UTF_8);
        int startIndex = 1; // Remove header
        int endIndex = lines.size(); // ______esto antes era size-1 pero no creo que tenga sentido

        return lines.subList(startIndex, endIndex);
    }

    private Map<Integer, List<LineInfo>> getSyncEvents(String tracePath, Integer workerID) throws IOException {
        if (DEBUG) {
            System.out.println("Getting sync events from: " + tracePath + " for worker " + workerID);
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

        if (DEBUG) {
            LOGGER.debug("Writing " + eventsLine.size() + " lines from worker " + workerID + " with "
                + workerHeader.getValue() + " threads");
        }

        for (String line : eventsLine) {
            String newEvent = updateEvent(workerHeader, line, workerID);
            this.masterWriter.println(newEvent);
        }
    }

    // workerHeader tiene identificadorWorker, offset, numero de executors del
    // worker (tareas posibles en paralelo)
    private static String updateEvent(LineInfo workerHeader, String line, Integer workerID) {
        int numThreads = (int) workerHeader.getValue();
        Matcher taskMatcher = WORKER_THREAD_INFO_PATTERN.matcher(line);
        String newLine = "";
        if (DEBUG) {
            System.out.println("_____updateEvent() worker " + workerID);
        }
        if (taskMatcher.find()) {
            Integer threadID = Integer.parseInt(taskMatcher.group(WORKER_THREAD_ID));
            Integer stateID = Integer.parseInt(taskMatcher.group(STATE_TYPE));
            int newThreadID = threadID;
            if (threadID > 1) {
                newThreadID = numThreads + 4 - threadID; // TODO: cambiar ese 4 a ver que pasa
            }
            String eventHeader = stateID + ":" + newThreadID + ":1:" + workerID + ":" + newThreadID;
            // TODO: mirar como puede newThreadID != newThreadID
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
            System.out.println("Getting pcf hw counters from: " + tracePcfPath);
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

    private static Map<Integer, String> getCE(String tracePcfPath) throws Exception {
        if (DEBUG) {
            System.out.println("Getting PCF CE from: " + tracePcfPath);
        }
        Pattern numberPattern = Pattern.compile("[0-9]+");
        Map<Integer, String> coreElements = new HashMap<Integer, String>();
        List<String> lines = Files.readAllLines(Paths.get(tracePcfPath), StandardCharsets.UTF_8);
        int headerLine = lines.indexOf(CE_FIXED_COUNTER);
        if (headerLine != -1) {
            int ceLine = headerLine + 2;
            while (ceLine < lines.size() - 1 && !lines.get(ceLine).isEmpty()) {
                System.out.println("______parseando linea  " + ceLine + " del archivo " + tracePcfPath);
                String[] values = lines.get(ceLine).split(CE_ID_VALUE_SEPARATOR);
                if (values.length != 2 || values[1].isEmpty() || !numberPattern.matcher(values[0]).matches()) {
                    throw new Exception("ERROR: Malformed CE in PFC " + tracePcfPath + "  line " + ceLine);
                }
                coreElements.put(Integer.parseInt(values[0]), values[1]);
                System.out.println("______added   " + values[0] + " --> " + values[1]);
                ceLine++;
            }
        }
        return coreElements;
    }

    private static Map<String, Integer> getReversedCE(String tracePcfPath) throws Exception {
        if (DEBUG) {
            System.out.println("Getting PCF CE from: " + tracePcfPath);
        }
        Pattern numberPattern = Pattern.compile("[0-9]+");
        Map<String, Integer> coreElements = new HashMap<String, Integer>();
        List<String> lines = Files.readAllLines(Paths.get(tracePcfPath), StandardCharsets.UTF_8);
        int headerLine = lines.indexOf(CE_FIXED_COUNTER);
        if (headerLine != -1) {
            int ceLine = headerLine + 2;
            while (ceLine < lines.size() - 1 && !lines.get(ceLine).isEmpty()) {
                System.out.println("______parseando linea  " + ceLine + " del archivo " + tracePcfPath);
                String[] values = lines.get(ceLine).split(CE_ID_VALUE_SEPARATOR);
                if (values.length != 2 || values[1].isEmpty() || !numberPattern.matcher(values[0]).matches()) {
                    throw new Exception("ERROR: Malformed CE in PFC " + tracePcfPath + "  line " + ceLine);
                }
                coreElements.put(values[1], Integer.parseInt(values[0]));
                System.out.println("______added   " + values[1] + " --> " + values[0]);
                ceLine++;
            }
        }
        return coreElements;
    }
}