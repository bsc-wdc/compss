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
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Scanner;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class TraceMerger {

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

    // Hardware counters pattern
    private static final String HW_COUNTER_HEADER = "EVENT_TYPE";
    private static final String HW_FIXED_COUNTER = "7  41999999 Active hardware counter set";
    private static final String HW_COUNTER_LINE_HEADER = "7  4200";

    // File names patterns
    private static final String MASTER_TRACE_SUFFIX = "_compss_trace_";
    private static final String TRACE_EXTENSION = ".prv";
    private static final String TRACE_PCF_EXTENSION = ".pcf";
    private static final String WORKER_TRACE_SUFFIX = "_python_trace" + TRACE_EXTENSION;
    private static final String TRACE_SUBDIR = "trace";
    private static final String WORKER_SUBDIR = "python";

    private static String workingDir;

    private final File masterTrace;
    private final File[] workersTraces;
    private final String masterTracePath;
    private final String masterTracePcfPath;
    private final String[] workersTracePath;
    private final String[] workersTracePcfPath;
    private final PrintWriter masterWriter;
    private final PrintWriter masterPcfWriter;


    private class LineInfo {

        private final String resourceId;
        private final Long value; // can be a timestamp (e.g. sync event at end).
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
     * Trace Merger constructor.
     * 
     * @param workingDir Working directory
     * @param appName Application name
     * @throws IOException Error managing files
     */
    public TraceMerger(String workingDir, String appName) throws IOException {
        // Init master trace information
        final String traceNamePrefix = appName + MASTER_TRACE_SUFFIX;
        final File masterF = new File(workingDir + File.separator + TRACE_SUBDIR);
        final File[] matchingMasterFiles = masterF
            .listFiles((File dir, String name) -> name.startsWith(traceNamePrefix) && name.endsWith(TRACE_EXTENSION));

        if (matchingMasterFiles == null || matchingMasterFiles.length < 1) {
            throw new FileNotFoundException("Master trace " + traceNamePrefix + "*" + TRACE_EXTENSION + " not found.");
        } else {
            this.masterTrace = matchingMasterFiles[0];
            this.masterTracePath = this.masterTrace.getAbsolutePath();
            this.masterTracePcfPath = this.masterTracePath.replace(TRACE_EXTENSION, TRACE_PCF_EXTENSION);
            if (matchingMasterFiles.length > 1) {
                LOGGER.warn("Found more than one master trace, using " + this.masterTrace + " to merge.");
            }
        }

        // Init workers traces information
        TraceMerger.workingDir = workingDir;
        final File workerF = new File(workingDir + File.separator + TRACE_SUBDIR + File.separator + WORKER_SUBDIR);
        File[] matchingWorkerFiles = workerF.listFiles((File dir, String name) -> name.endsWith(WORKER_TRACE_SUFFIX));

        if (matchingWorkerFiles == null) {
            throw new FileNotFoundException("No workers traces to merge found.");
        } else {
            this.workersTraces = matchingWorkerFiles;
        }

        this.workersTracePath = new String[this.workersTraces.length];
        this.workersTracePcfPath = new String[this.workersTraces.length];
        for (int i = 0; i < this.workersTracePath.length; ++i) {
            this.workersTracePath[i] = this.workersTraces[i].getAbsolutePath();
            this.workersTracePcfPath[i] = this.workersTracePath[i].replace(TRACE_EXTENSION, TRACE_PCF_EXTENSION);
        }

        // Initialize the writer for the final master trace
        this.masterWriter = new PrintWriter(new FileWriter(this.masterTracePath, true));
        this.masterPcfWriter = new PrintWriter(new FileWriter(this.masterTracePcfPath, true));

        LOGGER.debug("Trace's merger initialization successful");
    }

    /**
     * Merge traces.
     * 
     * @throws Exception Error managing traces
     */
    public void merge() throws Exception {
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

        LOGGER.debug("Merging PCF Hardware Counters into master");
        this.mergePCFs();

        LOGGER.debug("Merging finished.");

        if (!DEBUG) {
            String workerFolder = workingDir + File.separator + TRACE_SUBDIR + File.separator + WORKER_SUBDIR;
            LOGGER.debug("Removing folder " + workerFolder);
            try {
                removeFolder(workerFolder);
            } catch (IOException ioe) {
                LOGGER.warn("Could not remove python temporal tracing folder" + ioe.toString());
            }
        }
    }

    private void mergePCFs() throws IOException {
        // Check master hardware counters
        ArrayList<String> masterHWCounters = getHWCounters(this.masterTracePcfPath);
        // Check worker hardware counters
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
                        LOGGER.debug("Found PCF counter line not at master: " + line);
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
            this.masterPcfWriter.println(HW_COUNTER_HEADER);
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

    private void add(Map<Integer, List<LineInfo>> map, Integer key, LineInfo newValue) {
        List<LineInfo> currentValue = map.computeIfAbsent(key, k -> new ArrayList<>());
        currentValue.add(newValue);
    }

    private List<String> getWorkerEvents(File worker) throws IOException {
        if (DEBUG) {
            LOGGER.debug("Getting worker events from: " + worker.getAbsolutePath());
        }
        List<String> lines = Files.readAllLines(Paths.get(worker.getAbsolutePath()), StandardCharsets.UTF_8);
        int startIndex = 1; // Remove header
        int endIndex = lines.size() - 1;

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
        } // Exceptions are raised automatically, we add the try clause to automatically close the streams

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

    private String updateEvent(LineInfo workerHeader, String line, Integer workerID) {
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

        LineInfo javaStart = masterSyncEvents.get(0);
        // LineInfo javaEnd = masterSyncEvents.get(1);
        LineInfo javaSync = masterSyncEvents.get(2);
        // LineInfo workerStart = workerSyncEvents.get(0);
        // LineInfo workerEnd = workerSyncEvents.get(1);
        LineInfo workerSync = workerSyncEvents.get(2);

        // Take the sync event emitted by the runtime and worker and compare their value (timestamp)
        // The worker events real start is the difference between java and the worker minus the timestamp difference.
        Long syncDifference = Math.abs((javaSync.getValue() / 1000) - workerSync.getValue());
        Long realStart = Math.abs(javaSync.getTimestamp() - workerSync.getTimestamp()) - syncDifference;

        return new LineInfo(javaStart.getResourceId(), realStart, javaStart.getValue());
    }

    private ArrayList<String> getHWCounters(String tracePcfPath) throws IOException {
        if (DEBUG) {
            LOGGER.debug("Getting pcf hw counters from: " + tracePcfPath);
        }
        ArrayList<String> hwCounters = new ArrayList<String>();
        List<String> lines = Files.readAllLines(Paths.get(tracePcfPath), StandardCharsets.UTF_8);
        for (String line : lines) {
            if (line.startsWith(this.HW_COUNTER_LINE_HEADER)) {
                hwCounters.add(line);
            }
        }
        return hwCounters;
    }

}
