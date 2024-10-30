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
package es.bsc.compss.util.tracing;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.tracing.EventsDefinition;
import es.bsc.compss.types.tracing.MalformedException;
import es.bsc.compss.types.tracing.Trace;
import es.bsc.compss.types.tracing.Trace.RecordAppender;
import es.bsc.compss.types.tracing.Trace.RecordScanner;
import es.bsc.compss.types.tracing.paraver.PRVLine;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.nio.file.Files;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class TraceMerger {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);
    protected static final boolean DEBUG = LOGGER.isDebugEnabled();

    // Traces
    protected final Trace[] inputTraces;


    /**
     * Constructs a new TraceMerger.
     *
     * @param inputTraces Set of traces to be merged
     * @throws java.io.FileNotFoundException Some of the traces does not exists
     * @throws java.io.IOException Error managing the files
     */
    public TraceMerger(Trace[] inputTraces) throws FileNotFoundException, IOException {
        if (inputTraces == null || inputTraces.length == 0) {
            throw new FileNotFoundException("No traces to merge found.");
        }
        this.inputTraces = inputTraces;
    }

    /**
     * Computes a unified index for the Core elements.
     *
     * @return returns the correspondence between CE name and CE ID.
     * @throws IOException could not read the trace CEs.
     * @throws MalformedException a CE definition is malformed
     */
    public Map<String, String> getUnifiedCoreElementDefinition() throws IOException, MalformedException {
        // Creating global CE index
        Map<String, String> globalCE = new HashMap<>();
        globalCE.put("End", "0");
        int maxCEValue = 1;

        // add new CE found in workers
        for (Trace workerTrace : this.inputTraces) {
            EventsDefinition traceEvents = workerTrace.getEventsDefinition();
            Map<String, String> traceCEs = traceEvents.getCEsMapping();
            for (Map.Entry<String, String> ce : traceCEs.entrySet()) {
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

    /**
     * Gathers all the hardware counter in the traces.
     * 
     * @return list containing the hardware counters appearing in any trace to merge
     * @throws IOException could not read the pcf file
     * @throws MalformedException the pcf file contains a malformed HW Counter definition
     */
    protected Collection<String> getAllHWCounters() throws IOException, MalformedException {
        Collection<String> allHWCounters = new HashSet<>();
        for (Trace trace : this.inputTraces) {
            EventsDefinition traceEvents = trace.getEventsDefinition();
            allHWCounters.addAll(traceEvents.getHWCounters());
        }
        return allHWCounters;
    }

    /**
     * Merges the events of the input traces in the outputTrace applying the corresponding transformations.
     *
     * @param inputTraces traces to merge
     * @param transformations transformation to apply to each input trace
     * @param output trace where to leave the results
     * @throws Exception Could not read/write an event
     */
    protected static void mergeEvents(Trace[] inputTraces, TraceTransformation[][] transformations, Trace output)
        throws Exception {
        try (RecordAppender eventAppedner = output.getRecordAppender()) {
            int numTraces = inputTraces.length;
            int fullyRead = 0;
            RecordScanner[] records = new RecordScanner[numTraces];
            // Next line to write from each trace, we have to read in parallel to maintain the order
            String[] topRecords = new String[records.length];

            for (int traceIdx = 0; traceIdx < numTraces; traceIdx++) {
                records[traceIdx] = inputTraces[traceIdx].getRecords();
                topRecords[traceIdx] = records[traceIdx].next();
                if (topRecords[traceIdx] == null) {
                    fullyRead++;
                    records[traceIdx].close();
                } else {
                    PRVLine prvLine = PRVLine.parse(topRecords[traceIdx]);
                    for (TraceTransformation transformation : transformations[traceIdx]) {
                        transformation.apply(prvLine);
                    }
                    topRecords[traceIdx] = prvLine.toString();
                }
            }

            LOGGER.debug("Populating trace content");
            // Merging Prv contents
            while (fullyRead < numTraces) {
                int earliestLinePos = 0;
                for (int traceIdx = 0; traceIdx < topRecords.length; traceIdx++) {
                    if (topRecords[traceIdx] != null && !topRecords[traceIdx].isEmpty()) {
                        PRVLine prvLine = PRVLine.parse(topRecords[traceIdx]);
                        if (prvLine.goesBefore(topRecords[earliestLinePos])) {
                            earliestLinePos = traceIdx;
                        }
                    } else {
                        LOGGER.debug("WARNING: topRecords is empty or null.");
                    }
                }

                String toWrite = topRecords[earliestLinePos];
                eventAppedner.append(toWrite);
                String newLine = records[earliestLinePos].next();
                if (newLine != null) {
                    PRVLine prvLine = PRVLine.parse(newLine);
                    for (TraceTransformation transformation : transformations[earliestLinePos]) {
                        transformation.apply(prvLine);
                    }
                    newLine = prvLine.toString();
                } else {
                    fullyRead++;
                    records[earliestLinePos].close();
                }
                topRecords[earliestLinePos] = newLine;
            }
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

}
