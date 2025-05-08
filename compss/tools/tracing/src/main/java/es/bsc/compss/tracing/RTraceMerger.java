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
package es.bsc.compss.tracing;

import es.bsc.compss.types.tracing.ApplicationComposition;
import es.bsc.compss.types.tracing.ApplicationStructure;
import es.bsc.compss.types.tracing.EventsDefinition;
import es.bsc.compss.types.tracing.SynchEvent;
import es.bsc.compss.types.tracing.SystemComposition;
import es.bsc.compss.types.tracing.SystemStructure;
import es.bsc.compss.types.tracing.Thread;
import es.bsc.compss.types.tracing.ThreadIdentifier;
import es.bsc.compss.types.tracing.Threads;
import es.bsc.compss.types.tracing.Trace;
import es.bsc.compss.types.tracing.Trace.RecordScanner;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.types.tracing.paraver.PRVLine;
import es.bsc.compss.types.tracing.paraver.PRVNode;
import es.bsc.compss.types.tracing.paraver.PRVTask;
import es.bsc.compss.types.tracing.paraver.PRVThreadIdentifier;
import es.bsc.compss.types.tracing.paraver.PRVTrace;
import es.bsc.compss.util.tracing.ThreadTranslator;
import es.bsc.compss.util.tracing.TraceMerger;
import es.bsc.compss.util.tracing.TraceTransformation;
import es.bsc.compss.util.tracing.transformations.CPUOffset;
import es.bsc.compss.util.tracing.transformations.ThreadTranslation;
import es.bsc.compss.util.tracing.transformations.TimeOffset;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;


public class RTraceMerger extends TraceMerger {

    private final Trace mergeOnTrace;

    private static final String THREAD_ID_EVENT_TYPE = Integer.toString(TraceEventType.THREAD_IDENTIFICATION.code);
    private static final String EXEC_ID_EVENT_TYPE = Integer.toString(TraceEventType.EXECUTOR_IDENTIFICATION.code);


    /**
     * Initializes class attributes for R trace merging.
     *
     * @param outputTrace Working directory
     * @param workerTraces set of traces with R events to be merged into the main
     * @throws java.io.FileNotFoundException Master PRVTrace or workers traces not found
     * @throws IOException Error merging files
     */
    public RTraceMerger(Trace[] workerTraces, Trace outputTrace) throws FileNotFoundException, IOException {
        super(workerTraces);
        LOGGER.debug("Trace's merger initialization successful");
        this.mergeOnTrace = outputTrace;
    }

    /**
     * Merges the R traces with the master.
     */
    public void merge() throws Exception {
        Trace masterTrace = this.mergeOnTrace;
        String dir;
        dir = masterTrace.getDirectory();
        String tmpName;
        tmpName = masterTrace.getName() + ".tmp";
        String date;
        date = masterTrace.getDate();
        String duration;
        duration = masterTrace.getDuration();
        ApplicationComposition masterThreads;
        masterThreads = masterTrace.getThreadOrganization();
        SystemComposition infrastructure;
        infrastructure = masterTrace.getInfrastructure();

        EventsDefinition events;
        events = masterTrace.getEventsDefinition();
        events.defineNewHWCounters(getAllHWCounters());

        LOGGER.debug("Parsing master sync events");
        Map<Integer, List<SynchEvent>> masterSyncEvents = masterTrace.getSyncEvents(-1);
        LOGGER.debug("Merging task traces into master which contains " + masterSyncEvents.size() + " lines.");

        int numApps = masterThreads.getSubComponents().size();
        ThreadIdentifier[][][] executorIds = new ThreadIdentifier[masterThreads.getSubComponents().size()][][];
        for (int appId = 0; appId < numApps; appId++) {
            ApplicationComposition app = (ApplicationComposition) masterThreads.getSubComponents().get(appId);
            int numWorkers = app.getSubComponents().size();
            LOGGER.debug("numWorkers: " + numWorkers);
            executorIds[appId] = new ThreadIdentifier[numWorkers][];
            for (int workerId = 0; workerId < numWorkers; workerId++) {
                ApplicationComposition worker = (ApplicationComposition) app.getSubComponents().get(workerId);
                int numThreads = worker.getSubComponents().size();
                executorIds[appId][workerId] = new ThreadIdentifier[numThreads];
                LOGGER.debug("-appId: " + appId);
                LOGGER.debug("-workerId: " + workerId);
                LOGGER.debug("-numThreads: " + numThreads);
            }
        }

        try (RecordScanner records = masterTrace.getRecords()) {
            String line;
            // the isEmpty check should not be necessary if the .prv files are well constructed
            while ((line = records.next()) != null && !line.isEmpty()) {
                PRVLine prvLine = PRVLine.parse(line);
                String executorIdValue = prvLine.getEventValue(EXEC_ID_EVENT_TYPE);
                if (executorIdValue != null) {
                    PRVThreadIdentifier threadId = prvLine.getEmisorThreadIdentifier();
                    int appId = Integer.parseInt(threadId.getApp()) - 1;
                    int workerId = Integer.parseInt(threadId.getTask()) - 1;
                    int executorId = Integer.parseInt(executorIdValue);
                    executorIds[appId][workerId][executorId] = threadId;
                    LOGGER.debug("+appId: " + appId);
                    LOGGER.debug("+workerId: " + workerId);
                    LOGGER.debug("+executorId: " + executorId);
                    LOGGER.debug("+threadId: " + threadId);
                }
            }
        }

        TraceTransformation[][] modifications = new TraceTransformation[this.inputTraces.length + 1][];
        for (int idx = 0; idx < this.inputTraces.length; idx++) {
            Trace workerTrace = this.inputTraces[idx];
            if (workerTrace != masterTrace) {
                Integer workerIdx;
                LOGGER.debug("Merging worker " + workerTrace);
                String workerFileName = workerTrace.getName();
                try {
                    String wID = "";
                    for (int i = 0; workerFileName.charAt(i) != '_'; ++i) {
                        wID += workerFileName.charAt(i);
                    }
                    workerIdx = Integer.parseInt(wID);
                } catch (Exception e) {
                    // If workerId cannot be retrieved it is the master
                    workerIdx = 0;
                }
                Integer workerID = workerIdx + 1;

                LOGGER.debug("Master " + workerID + " sync events:");
                for (Map.Entry<Integer, List<SynchEvent>> entry : masterSyncEvents.entrySet()) {
                    LOGGER.debug(">> " + entry.getKey() + " -+- " + entry.getValue().size());
                    for (SynchEvent syncEntry : entry.getValue()) {
                        LOGGER.debug(">>  - " + syncEntry.getResourceId() + ":" + syncEntry.getWorkerId() + ":"
                            + syncEntry.getTimestamp() + ":" + syncEntry.getValue());
                    }
                }

                Map<Integer, List<SynchEvent>> workerSyncEvents = workerTrace.getSyncEvents(workerID);

                LOGGER.debug("Worker " + workerID + " sync events:");
                for (Map.Entry<Integer, List<SynchEvent>> entry : workerSyncEvents.entrySet()) {
                    LOGGER.debug(">> " + entry.getKey() + " -+- " + entry.getValue().size());
                    for (SynchEvent syncEntry : entry.getValue()) {
                        LOGGER.debug(">>  - " + syncEntry.getResourceId() + ":" + syncEntry.getWorkerId() + ":"
                            + syncEntry.getTimestamp() + ":" + syncEntry.getValue());
                    }
                }

                SynchEvent synchOffset = computeOffset(masterSyncEvents.get(workerID), workerSyncEvents.get(workerID));
                long timeOffset = synchOffset.getTimestamp();
                LOGGER.debug("Time offset: " + timeOffset);
                modifications[idx] = new TraceTransformation[3];
                modifications[idx][0] = new TimeOffset(timeOffset);

                HashMap<PRVThreadIdentifier, Integer> rRuntime = new HashMap<>();
                HashMap<PRVThreadIdentifier, String> rExecutors = new HashMap<>();
                try (RecordScanner records = workerTrace.getRecords()) {
                    String line;
                    // the isEmpty check should not be necessary if the .prv files are well constructed
                    while ((line = records.next()) != null && !line.isEmpty()) {
                        PRVLine prvLine = PRVLine.parse(line);
                        String identifierEventValue = prvLine.getEventValue(THREAD_ID_EVENT_TYPE);
                        if (identifierEventValue != null) {
                            PRVThreadIdentifier threadId = prvLine.getEmisorThreadIdentifier();
                            Integer threadTypeId = new Integer(identifierEventValue);
                            if (threadTypeId != Threads.EXEC.id) {
                                rRuntime.put(threadId, threadTypeId);
                            }
                        }
                        String executorIdValue = prvLine.getEventValue(EXEC_ID_EVENT_TYPE);
                        if (executorIdValue != null) {
                            PRVThreadIdentifier threadId = prvLine.getEmisorThreadIdentifier();
                            rExecutors.put(threadId, executorIdValue);
                        }
                    }
                }

                ThreadIdentifier[] wExec2Thread = executorIds[0][workerIdx];
                ApplicationComposition rThreads = workerTrace.getThreadOrganization();
                RMergeTranslation translation =
                    new RMergeTranslation(masterThreads, rThreads, workerIdx, rRuntime, rExecutors, wExec2Thread);
                modifications[idx][1] = new ThreadTranslation(translation);
                modifications[idx][2] = computeCPUOffset(masterThreads, infrastructure, workerIdx);
            } else {
                modifications[idx] = new TraceTransformation[0];
            }
        }
        if (LOGGER.isDebugEnabled()) {
            for (int i = 0; i < this.inputTraces.length; i++) {
                LOGGER.debug("*** Modifications applied to trace " + this.inputTraces[i].getName());
                for (TraceTransformation mod : modifications[i]) {
                    LOGGER.debug(mod.getDescription());
                }
            }
        }

        // Maintain trace structure from master Trace
        PRVTrace tmpTrace = PRVTrace.generateNew(dir, tmpName, date, duration, infrastructure, masterThreads, events);
        LOGGER.debug("Merge events...");
        mergeEvents(this.inputTraces, modifications, tmpTrace);
        tmpTrace.renameAs(masterTrace.getDirectory(), masterTrace.getName());
        LOGGER.debug("Merging finished.");
    }

    private SynchEvent computeOffset(List<SynchEvent> referenceSyncEvents, List<SynchEvent> localSyncEvents)
        throws Exception {
        if (referenceSyncEvents.size() < 3) {
            throw new Exception("ERROR: Malformed master trace. Master sync events not found");
        }
        if (localSyncEvents.size() < 3) {
            throw new Exception("ERROR: Malformed worker trace. Worker sync events not found");
        }

        SynchEvent refStart = referenceSyncEvents.get(0); // numero de threads del master al arrancar el runtime
        // SynchEvent refEnd = referenceSyncEvents.get(1);
        SynchEvent refSync = referenceSyncEvents.get(2);
        // SynchEvent localStart = localSyncEvents.get(0); // worker sync events
        // SynchEvent localEnd = localSyncEvents.get(1);
        SynchEvent localSync = localSyncEvents.get(2);

        LOGGER.debug("Computing offset:");
        LOGGER.debug("-- refStart: " + refStart.getValue());
        // LOGGER.debug("-- refEnd: " + refEnd.getValue());
        LOGGER.debug("-- refSync: " + refSync.getValue());
        // LOGGER.debug("-- localStart: " + localStart.getValue());
        // LOGGER.debug("-- localEnd: " + localEnd.getValue());
        LOGGER.debug("-- localSync: " + localSync.getValue());

        // Take the sync event emitted by the reference (master) and local(worker) and compare their value (timestamp)
        // The worker events real start is the difference between reference and the local
        // minus the timestamp difference.
        Long syncDifference = Math.abs((refSync.getValue() / 1000) - localSync.getValue());
        Long realStart = Math.abs(refSync.getTimestamp() - localSync.getTimestamp()) - syncDifference;

        LOGGER.debug("*** refSync.getTimestamp()  : " + refSync.getTimestamp());
        LOGGER.debug("*** localSync.getTimestamp(): " + localSync.getTimestamp());
        LOGGER.debug("*** syncDifference: " + syncDifference);
        LOGGER.debug("*** realStart: " + realStart);

        return new SynchEvent(refStart.getResourceId(), "", realStart, refStart.getValue());
    }

    private CPUOffset computeCPUOffset(ApplicationComposition<?> mainTO, SystemComposition<?> mainInf, int workerId) {
        for (SystemStructure s : mainInf.getSubComponents()) {
            PRVNode node = (PRVNode) s;
        }
        ApplicationComposition app = (ApplicationComposition) mainTO.getSubComponents().get(0);
        PRVTask task = (PRVTask) app.getSubComponents().get(workerId);
        PRVNode node = task.getNode();
        int cpuOffset = getNodeCPUOffset(node, mainInf);
        return new CPUOffset(cpuOffset);
    }

    private int getNodeCPUOffset(PRVNode node, SystemComposition<?> infra) {
        int cpuOffset = 0;
        for (SystemStructure s : infra.getSubComponents()) {
            if (s == node) {
                return cpuOffset;
            }
            cpuOffset += s.getNumberOfDirectSubcomponents();
        }
        return cpuOffset;
    }


    private static class RMergeTranslation implements ThreadTranslator {

        private final ApplicationComposition threads;
        private final ApplicationComposition task;
        private final ThreadIdentifier[] appToExec;


        public RMergeTranslation(ApplicationComposition<?> mainTO, ApplicationComposition<?> rTO, int workerId,
            HashMap<PRVThreadIdentifier, Integer> rRuntime, HashMap<PRVThreadIdentifier, String> rExecutors,
            ThreadIdentifier[] exec2Thread) {
            this.threads = mainTO;
            ApplicationComposition app = (ApplicationComposition) mainTO.getSubComponents().get(0);
            task = (ApplicationComposition) app.getSubComponents().get(workerId);
            int numMainThreads = task.getSubComponents().size();

            int numRThreads = 0;
            Set<PRVThreadIdentifier> unknownThreads = new HashSet<>();
            for (ApplicationStructure a : rTO.getSubComponents()) {
                ApplicationComposition<?> rApp = (ApplicationComposition) a;
                for (ApplicationStructure tk : rApp.getSubComponents()) {
                    ApplicationComposition<?> rTask = (ApplicationComposition) tk;
                    for (ApplicationStructure t : rTask.getSubComponents()) {
                        Thread thread = (Thread) t;
                        PRVThreadIdentifier tId = (PRVThreadIdentifier) thread.getIdentifier();
                        unknownThreads.add(tId);
                        numRThreads++;
                    }
                }
            }

            LOGGER.debug("numMainThreads: " + numMainThreads);
            LOGGER.debug("numRThreads: " + numRThreads);

            appToExec = new ThreadIdentifier[numRThreads];
            for (java.util.Map.Entry<PRVThreadIdentifier, String> entry : rExecutors.entrySet()) {
                PRVThreadIdentifier id = entry.getKey();
                int appId = Integer.parseInt(id.getApp()) - 1;
                int executorId = Integer.parseInt(entry.getValue());
                LOGGER.debug("appId: " + appId);
                LOGGER.debug("executorId: " + executorId);
                appToExec[appId] = exec2Thread[executorId];
                unknownThreads.remove(id);
            }

            Thread refThread = (Thread) task.getSubComponents().get(0);
            PRVThreadIdentifier reference = (PRVThreadIdentifier) refThread.getIdentifier();
            String refLabel = refThread.getLabel();

            String refApp = reference.getApp();
            String refTask = reference.getTask();
            for (java.util.Map.Entry<PRVThreadIdentifier, Integer> entry : rRuntime.entrySet()) {
                PRVThreadIdentifier rId = entry.getKey();
                unknownThreads.remove(rId);
                String newThreadId = Integer.toString(numMainThreads + 1);
                PRVThreadIdentifier newId = new PRVThreadIdentifier(refApp, refTask, newThreadId);
                String newLabel = refLabel.substring(0, refLabel.length() - 1) + newThreadId;
                task.appendComponent(new Thread(newId, newLabel));
                numMainThreads++;
                int appId = Integer.parseInt(rId.getApp()) - 1;
                appToExec[appId] = newId;
            }
            for (PRVThreadIdentifier rId : unknownThreads) {
                String newThreadId = Integer.toString(numMainThreads + 1);
                PRVThreadIdentifier newId = new PRVThreadIdentifier(refApp, refTask, newThreadId);
                String newLabel = refLabel.substring(0, refLabel.length() - 1) + newThreadId;
                task.appendComponent(new Thread(newId, newLabel));
                numMainThreads++;
                int appId = Integer.parseInt(rId.getApp()) - 1;
                appToExec[appId] = newId;
            }
        }

        @Override
        public ThreadIdentifier getNewThreadId(ThreadIdentifier threadId) {
            PRVThreadIdentifier prvId = (PRVThreadIdentifier) threadId;
            String oldId = prvId.getApp();
            int appId = Integer.parseInt(oldId) - 1;
            return appToExec[appId];
        }

        @Override
        public ApplicationComposition getNewThreadOrganization() {
            return threads;
        }

        @Override
        public String getDescription() {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < appToExec.length; i++) {
                ThreadIdentifier tid = appToExec[i];
                sb.append("\t * ").append(i + 1).append(".1.1").append("->").append(tid).append("\n");
            }
            return sb.toString();
        }

    }


    /**
     * Main method to start the merging of R traces into a main trace.
     *
     * @param args Tracer arguments: 0 - tracing folder 1 - main trace name 2> - R traces to merge
     * @throws Exception The merger raised an error
     */
    public static void main(String[] args) throws Exception {
        String workingDir = args[0];
        String traceName = args[1];
        final PRVTrace mainTrace = new PRVTrace(workingDir, traceName);
        if (!mainTrace.exists()) {
            throw new FileNotFoundException("Master trace " + traceName + " not found at directory " + workingDir);
        }
        int numRTraces = args.length - 2;
        if (numRTraces > 0) {
            PRVTrace[] traces = new PRVTrace[numRTraces + 1];
            traces[0] = mainTrace;
            for (int i = 2; i < args.length; i++) {
                File trace = new File(args[i]);
                traces[i - 1] = new PRVTrace(trace);
            }

            RTraceMerger merger = new RTraceMerger(traces, mainTrace);
            merger.merge();
        }
    }

}
