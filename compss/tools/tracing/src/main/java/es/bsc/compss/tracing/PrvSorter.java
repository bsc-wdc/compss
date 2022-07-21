/*
 *  Copyright 2002-2022 Barcelona Supercomputing Center (www.bsc.es)
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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.tracing.ApplicationComposition;
import es.bsc.compss.types.tracing.Thread;
import es.bsc.compss.types.tracing.ThreadIdentifier;
import es.bsc.compss.types.tracing.Threads;
import es.bsc.compss.types.tracing.Trace.RecordScanner;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.types.tracing.paraver.PRVLine;
import es.bsc.compss.types.tracing.paraver.PRVThreadIdentifier;
import es.bsc.compss.types.tracing.paraver.PRVTrace;
import es.bsc.compss.util.tracing.ThreadTranslator;
import es.bsc.compss.util.tracing.transformations.ThreadTranslation;
import java.io.FileNotFoundException;
import java.io.IOException;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores the threads information with addThread and creates the maps needed to translateThreads the threads in both the
 * .prv and .row files.
 *
 * @throws Exception Exception parsing the line
 */
public class PrvSorter implements ThreadTranslator {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);

    private static final String THREAD_ID_EVENT_TYPE = Integer.toString(TraceEventType.THREAD_IDENTIFICATION.code);

    private Map<ThreadIdentifier, ThreadIdentifier> threadTranslations;
    private ApplicationComposition system;


    /**
     * Constructs a new ThreadTranslator and sets up the mappings with the content of a trace.
     * 
     * @param trace trace with events to translateThreads
     * @throws FileNotFoundException prvFile doesn't exist
     * @throws IOException error raised during prv file reading
     */
    public PrvSorter(PRVTrace trace) throws FileNotFoundException, IOException {
        List<Machine> machines = identifyThreads(trace);
        computeTranslationMap(machines);
    }

    /**
     * Parses the prv file and to set up the translation mappings.
     * 
     * @param trace trace to be parsed
     * @throws FileNotFoundException prvFile doesn't exist
     * @throws IOException error raised during prv file reading
     */
    private static List<Machine> identifyThreads(PRVTrace trace) throws FileNotFoundException, IOException {
        List<Machine> machines = new ArrayList<>();
        try (RecordScanner events = trace.getRecords()) {
            String line;
            // the isEmpty check should not be necessary if the .prv files are well constructed
            while ((line = events.next()) != null && !line.isEmpty()) {
                PRVLine prvLine = PRVLine.parse(line);
                String identifierEventValue = prvLine.getEventValue(THREAD_ID_EVENT_TYPE);
                if (identifierEventValue != null) {
                    PRVThreadIdentifier threadId = prvLine.getEmisorThreadIdentifier();

                    int machineId = Integer.parseInt(threadId.getTask());
                    while (machines.size() < machineId) {
                        machines.add(new Machine());
                    }
                    Machine machine = machines.get(machineId - 1);
                    machine.registerThread(threadId, identifierEventValue);
                }
            }
        } // we don't need the header right now // we don't need the header right now
        return machines;
    }

    private void computeTranslationMap(List<Machine> machines) {
        threadTranslations = new HashMap<>();
        system = new ApplicationComposition();

        for (int i = 0; i < machines.size(); i++) {
            ApplicationComposition runtime = new ApplicationComposition();

            String machineId = Integer.toString(i + 1);
            // for thread 1.X.1 -> X.1.1, main thread has no event and thus is not by addThread()
            PRVThreadIdentifier oldMainId = new PRVThreadIdentifier("1", machineId, "1");
            PRVThreadIdentifier newMainId = computeNewThreadId(oldMainId, Threads.ExtraeTaskType.RUNTIME, 1);
            threadTranslations.put(oldMainId, newMainId);
            String label;
            if (i == 0) {
                label = "MAIN APP (1.1.1)";
            } else {
                label = "WORKER MAIN (" + machineId + ".1.1)";
            }
            Thread main = new Thread(newMainId, label);
            runtime.appendComponent(main);

            Machine m = machines.get(i);
            int runtimeThreadsNum = 2;

            Map<Integer, PRVThreadIdentifier> runtimeIdentifiedThreads = m.getRuntimeIdentifiers();
            for (Threads t : Threads.values()) {
                if (t.isRuntime()) {
                    int idEvent = t.id;
                    PRVThreadIdentifier oldThread = runtimeIdentifiedThreads.get(idEvent);
                    if (oldThread != null) {
                        int threadId = runtimeThreadsNum++;
                        Threads.ExtraeTaskType task = Threads.ExtraeTaskType.RUNTIME;
                        ThreadIdentifier newThread = computeNewThreadId(oldThread, task, threadId);
                        threadTranslations.put(oldThread, newThread);

                        String oldLabel = newThread.toString();
                        String newThreadId = oldLabel.replace(":", ".");
                        String newLabel = createLabel(newThreadId, idEvent);
                        Thread runtimeThread = new Thread(newThread, newLabel);
                        runtime.appendComponent(runtimeThread);
                    }
                }
            }

            ApplicationComposition executors = new ApplicationComposition();
            int executorsNum = 1;
            for (PRVThreadIdentifier oldThread : m.getExecutors()) {
                int threadId = executorsNum++;
                Threads.ExtraeTaskType task = Threads.ExtraeTaskType.EXECUTOR;
                ThreadIdentifier newThread = computeNewThreadId(oldThread, task, threadId);
                threadTranslations.put(oldThread, newThread);

                String oldLabel = newThread.toString();
                String newThreadId = oldLabel.replace(":", ".");
                String newLabel = createLabel(newThreadId, Threads.EXEC.id);

                Thread executorThread = new Thread(newThread, newLabel);
                executors.appendComponent(executorThread);
            }
            for (PRVThreadIdentifier oldThread : m.getThreads()) {
                if (!threadTranslations.containsKey(oldThread)) {
                    int threadId = runtimeThreadsNum++;
                    Threads.ExtraeTaskType task = Threads.ExtraeTaskType.RUNTIME;
                    PRVThreadIdentifier newThread = computeNewThreadId(oldThread, task, threadId);
                    threadTranslations.put(oldThread, newThread);

                    String oldLabel = newThread.toString();
                    String newLabel = "THREAD " + oldLabel.replace(":", ".");
                    Thread runtimeThread = new Thread(newThread, newLabel);
                    runtime.appendComponent(runtimeThread);
                }
            }

            ApplicationComposition machine = new ApplicationComposition();
            if (runtime.getNumberOfDirectSubcomponents() > 0) {
                machine.appendComponent(runtime);
            }
            if (executors.getNumberOfDirectSubcomponents() > 0) {
                machine.appendComponent(executors);
            }
            if (machine.getNumberOfDirectSubcomponents() > 0) {
                system.appendComponent(machine);
            }
        }
    }

    private static PRVThreadIdentifier computeNewThreadId(PRVThreadIdentifier id, Threads.ExtraeTaskType type,
        int threadId) {
        String thread = Integer.toString(threadId++);
        String task = type.getLabel();
        String app = id.getTask();
        return new PRVThreadIdentifier(app, task, thread);
    }

    @Override
    public ThreadIdentifier getNewThreadId(ThreadIdentifier oldThreadId) {
        return threadTranslations.get(oldThreadId);
    }

    private String createLabel(String threadId, int identifierEvent) {
        String label = Threads.getLabelByID(identifierEvent);
        return label + " (" + threadId + ")";
    }

    @Override
    public ApplicationComposition getNewThreadOrganization() {
        return system;
    }


    private static class Machine {

        private Map<Integer, PRVThreadIdentifier> runtimeIdentifiers;
        private Set<PRVThreadIdentifier> threads;
        private Set<PRVThreadIdentifier> executors;


        public Machine() {
            threads = new HashSet<>();
            executors = new HashSet<>();
            runtimeIdentifiers = new HashMap<>();
        }

        private Set<PRVThreadIdentifier> getThreads() {
            return this.threads;
        }

        private Set<PRVThreadIdentifier> getExecutors() {
            return this.executors;
        }

        private Map<Integer, PRVThreadIdentifier> getRuntimeIdentifiers() {
            return this.runtimeIdentifiers;
        }

        public void registerThread(PRVThreadIdentifier threadId, String threadTypeIdString) {
            this.threads.add(threadId);
            if (threadTypeIdString != null) {
                Integer threadTypeId = new Integer(threadTypeIdString);
                if (threadTypeId == Threads.EXEC.id) {
                    this.executors.add(threadId);
                } else {
                    if (threadTypeId != 0) { // != end event
                        this.runtimeIdentifiers.put(threadTypeId, threadId);
                    }
                }
            }
        }
    }


    /**
     * Main method to reorganize the threads of a COMPSs trace.
     *
     * @param args Tracer arguments: 0 - tracing folder 1 - main trace name
     * @throws java.lang.Exception error while reordering the trace
     */
    public static void main(String[] args) throws Exception {
        String workingDir = args[0];
        String traceName = args[1];

        final PRVTrace trace = new PRVTrace(workingDir, traceName);
        if (!trace.exists()) {
            throw new FileNotFoundException("Trace " + traceName + " not found at directory " + workingDir);
        }
        ThreadTranslator sorter = new PrvSorter(trace);
        trace.applyTransformations(new ThreadTranslation(sorter));

    }
}
