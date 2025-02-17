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


public class PythonTraceMerger extends TraceMerger {

    private final Trace mergeOnTrace;

    private static final String THREAD_ID_EVENT_TYPE = Integer.toString(TraceEventType.THREAD_IDENTIFICATION.code);
    private static final String EXEC_ID_EVENT_TYPE = Integer.toString(TraceEventType.EXECUTOR_IDENTIFICATION.code);


    /**
     * Initializes class attributes for python trace merging.
     *
     * @param outputTrace Working directory
     * @param workerTraces set of traces with python events to be merged into the main
     * @throws java.io.FileNotFoundException Master PRVTrace or workers traces not found
     * @throws IOException Error merging files
     */
    public PythonTraceMerger(Trace[] workerTraces, Trace outputTrace) throws FileNotFoundException, IOException {
        super(workerTraces);
        LOGGER.debug("Trace's merger initialization successful");
        this.mergeOnTrace = outputTrace;
    }

    /**
     * Merges the python traces with the master.
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
            executorIds[appId] = new ThreadIdentifier[numWorkers][];
            for (int workerId = 0; workerId < numWorkers; workerId++) {
                ApplicationComposition worker = (ApplicationComposition) app.getSubComponents().get(workerId);
                int numThreads = worker.getSubComponents().size();
                executorIds[appId][workerId] = new ThreadIdentifier[numThreads];
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
                Map<Integer, List<SynchEvent>> workerSyncEvents = workerTrace.getSyncEvents(workerID);

                SynchEvent synchOffset = computeOffset(masterSyncEvents.get(workerID), workerSyncEvents.get(workerID));
                long timeOffset = synchOffset.getTimestamp();
                modifications[idx] = new TraceTransformation[3];
                modifications[idx][0] = new TimeOffset(timeOffset);

                HashMap<PRVThreadIdentifier, Integer> pythonRuntime = new HashMap<>();
                HashMap<PRVThreadIdentifier, String> pythonExecutors = new HashMap<>();
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
                                pythonRuntime.put(threadId, threadTypeId);
                            }
                        }
                        String executorIdValue = prvLine.getEventValue(EXEC_ID_EVENT_TYPE);
                        if (executorIdValue != null) {
                            PRVThreadIdentifier threadId = prvLine.getEmisorThreadIdentifier();
                            pythonExecutors.put(threadId, executorIdValue);
                        }
                    }
                }

                ThreadIdentifier[] wExec2Thread = executorIds[0][workerIdx];
                ApplicationComposition pyThreads = workerTrace.getThreadOrganization();
                PythonMergeTranslation translation = new PythonMergeTranslation(masterThreads, pyThreads, workerIdx,
                    pythonRuntime, pythonExecutors, wExec2Thread);
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
        // LineInfo refEnd = masterSyncEvents.get(1);
        SynchEvent refSync = referenceSyncEvents.get(2);
        // LineInfo localStart = workerSyncEvents.get(0);
        // LineInfo localEnd = workerSyncEvents.get(1);
        SynchEvent localSync = localSyncEvents.get(2);

        // Take the sync event emitted by the reference (master) and local(worker) and compare their value (timestamp)
        // The worker events real start is the difference between reference and the local
        // minus the timestamp difference.
        Long syncDifference = Math.abs((refSync.getValue() / 1000) - localSync.getValue());
        Long realStart = Math.abs(refSync.getTimestamp() - localSync.getTimestamp()) - syncDifference;

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


    private static class PythonMergeTranslation implements ThreadTranslator {

        private final ApplicationComposition threads;
        private final ApplicationComposition task;
        private final ThreadIdentifier[] appToExec;


        public PythonMergeTranslation(ApplicationComposition<?> mainTO, ApplicationComposition<?> pyTO, int workerId,
            HashMap<PRVThreadIdentifier, Integer> pythonRuntime, HashMap<PRVThreadIdentifier, String> pythonExecutors,
            ThreadIdentifier[] exec2Thread) {
            this.threads = mainTO;
            ApplicationComposition app = (ApplicationComposition) mainTO.getSubComponents().get(0);
            task = (ApplicationComposition) app.getSubComponents().get(workerId);
            int numMainThreads = task.getSubComponents().size();

            int numPyThreads = 0;
            Set<PRVThreadIdentifier> unknownThreads = new HashSet<>();
            for (ApplicationStructure a : pyTO.getSubComponents()) {
                ApplicationComposition<?> pyApp = (ApplicationComposition) a;
                for (ApplicationStructure tk : pyApp.getSubComponents()) {
                    ApplicationComposition<?> pyTask = (ApplicationComposition) tk;
                    for (ApplicationStructure t : pyTask.getSubComponents()) {
                        Thread thread = (Thread) t;
                        PRVThreadIdentifier tId = (PRVThreadIdentifier) thread.getIdentifier();
                        unknownThreads.add(tId);
                        numPyThreads++;
                    }
                }
            }

            appToExec = new ThreadIdentifier[numPyThreads];
            for (java.util.Map.Entry<PRVThreadIdentifier, String> entry : pythonExecutors.entrySet()) {
                PRVThreadIdentifier id = entry.getKey();
                int appId = Integer.parseInt(id.getApp()) - 1;
                int executorId = Integer.parseInt(entry.getValue());
                appToExec[appId] = exec2Thread[executorId];
                unknownThreads.remove(id);
            }

            Thread refThread = (Thread) task.getSubComponents().get(0);
            PRVThreadIdentifier reference = (PRVThreadIdentifier) refThread.getIdentifier();
            String refLabel = refThread.getLabel();

            String refApp = reference.getApp();
            String refTask = reference.getTask();
            for (java.util.Map.Entry<PRVThreadIdentifier, Integer> entry : pythonRuntime.entrySet()) {
                PRVThreadIdentifier pythonId = entry.getKey();
                unknownThreads.remove(pythonId);
                String newThreadId = Integer.toString(numMainThreads + 1);
                PRVThreadIdentifier newId = new PRVThreadIdentifier(refApp, refTask, newThreadId);
                String newLabel = refLabel.substring(0, refLabel.length() - 1) + newThreadId;
                task.appendComponent(new Thread(newId, newLabel));
                numMainThreads++;
                int appId = Integer.parseInt(pythonId.getApp()) - 1;
                appToExec[appId] = newId;
            }
            for (PRVThreadIdentifier pythonId : unknownThreads) {
                String newThreadId = Integer.toString(numMainThreads + 1);
                PRVThreadIdentifier newId = new PRVThreadIdentifier(refApp, refTask, newThreadId);
                String newLabel = refLabel.substring(0, refLabel.length() - 1) + newThreadId;
                task.appendComponent(new Thread(newId, newLabel));
                numMainThreads++;
                int appId = Integer.parseInt(pythonId.getApp()) - 1;
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
     * Main method to start the merging of python traces into a main trace.
     *
     * @param args Tracer arguments: 0 - tracing folder 1 - main trace name 2> - Python traces to merge
     * @throws Exception The merger raised an error
     */
    public static void main(String[] args) throws Exception {
        String workingDir = args[0];
        String traceName = args[1];
        final PRVTrace mainTrace = new PRVTrace(workingDir, traceName);
        if (!mainTrace.exists()) {
            throw new FileNotFoundException("Master trace " + traceName + " not found at directory " + workingDir);
        }
        int numPythonTraces = args.length - 2;
        if (numPythonTraces > 0) {
            PRVTrace[] traces = new PRVTrace[numPythonTraces + 1];
            traces[0] = mainTrace;
            for (int i = 2; i < args.length; i++) {
                File trace = new File(args[i]);
                traces[i - 1] = new PRVTrace(trace);
            }

            PythonTraceMerger merger = new PythonTraceMerger(traces, mainTrace);
            merger.merge();
        }
    }

}
