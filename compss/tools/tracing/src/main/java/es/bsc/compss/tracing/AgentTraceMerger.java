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
import es.bsc.compss.types.tracing.SystemComposition;
import es.bsc.compss.types.tracing.SystemStructure;
import es.bsc.compss.types.tracing.Thread;
import es.bsc.compss.types.tracing.ThreadIdentifier;
import es.bsc.compss.types.tracing.Trace;
import es.bsc.compss.types.tracing.paraver.PRVApplication;
import es.bsc.compss.types.tracing.paraver.PRVNode;
import es.bsc.compss.types.tracing.paraver.PRVTask;
import es.bsc.compss.types.tracing.paraver.PRVThreadIdentifier;
import es.bsc.compss.types.tracing.paraver.PRVTrace;
import es.bsc.compss.util.tracing.EventTranslator;
import es.bsc.compss.util.tracing.ThreadTranslator;
import es.bsc.compss.util.tracing.TraceMerger;
import es.bsc.compss.util.tracing.TraceTransformation;
import es.bsc.compss.util.tracing.transformations.CETranslation;
import es.bsc.compss.util.tracing.transformations.CPUOffset;
import es.bsc.compss.util.tracing.transformations.ThreadTranslation;

import java.io.File;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class AgentTraceMerger extends TraceMerger {

    private final String directory;
    private final String name;


    /**
     * Initializes class attributes for agents trace merging .
     *
     * @param agentTraces Traces of the agents to be merged
     * @param directory path of the folder where to leave the merged trace
     * @param name name of the trace to store the result
     * @throws IOException Error managing files
     */
    public AgentTraceMerger(Trace[] agentTraces, String directory, String name) throws IOException {
        super(agentTraces);
        this.directory = directory;
        this.name = name;
    }

    /**
     * Merges the traces .
     */
    public void merge() throws Exception {
        LOGGER.debug("Starting merge process.");
        System.out.println("Starting merge process.");

        // Determine tmp trace location
        String dir;
        dir = directory;
        String tmpName;
        tmpName = name;

        // Determine date
        String date;
        date = this.inputTraces[0].getDate();

        ApplicationComposition threads;
        threads = new ApplicationComposition();

        SystemComposition infrastructure;
        infrastructure = new SystemComposition("");

        EventsDefinition events;
        events = this.inputTraces[0].getEventsDefinition();

        long maxDuration = this.inputTraces[0].getDurationInNS();
        // Add Agent1
        mergeTraceNodes(this.inputTraces[0], infrastructure);
        mergeTraceThreads(this.inputTraces[0], threads);

        Map<String, String> globalCEs = getUnifiedCoreElementDefinition();
        events.redefineCEs(globalCEs);

        TraceTransformation[][] modifications = new TraceTransformation[this.inputTraces.length][];

        TraceTransformation[] traceModifications = new TraceTransformation[1];
        EventsDefinition traceEvents = this.inputTraces[0].getEventsDefinition();
        Map<String, String> traceCEs = traceEvents.getCEsMapping();
        EventTranslator traceCETranslation = new EventTranslator(traceCEs, globalCEs);
        traceModifications[0] = new CETranslation(traceCETranslation, globalCEs);
        modifications[0] = traceModifications;

        for (int traceId = 1; traceId < this.inputTraces.length; traceId++) {
            Trace trace = this.inputTraces[traceId];
            traceModifications = new TraceTransformation[3];

            long traceDuration = trace.getDurationInNS();
            if (traceDuration > maxDuration) {
                maxDuration = traceDuration;
            }

            traceEvents = trace.getEventsDefinition();
            traceCEs = traceEvents.getCEsMapping();
            traceCETranslation = new EventTranslator(traceCEs, globalCEs);
            traceModifications[0] = new CETranslation(traceCETranslation, globalCEs);

            // Appending infrastructure
            TraceTransformation cpuOffset = mergeTraceNodes(trace, infrastructure);
            traceModifications[1] = cpuOffset;

            // Appending Threads
            traceModifications[2] = mergeTraceThreads(trace, threads);
            modifications[traceId] = traceModifications;
        }

        if (LOGGER.isDebugEnabled()) {
            for (int i = 0; i < this.inputTraces.length; i++) {
                LOGGER.debug("*** Modifications applied to trace " + this.inputTraces[i].getName());
                for (TraceTransformation mod : modifications[i]) {
                    LOGGER.debug(mod.getDescription());
                }
            }
        }

        String duration = Long.toString(maxDuration);
        PRVTrace output = PRVTrace.generateNew(dir, tmpName, date, duration + "_ns", infrastructure, threads, events);
        mergeEvents(this.inputTraces, modifications, output);

        LOGGER.debug("Merge finished.");
        System.out.println("Merge finished.");
    }

    private static TraceTransformation mergeTraceNodes(Trace trace, SystemComposition<SystemStructure> global) {
        int cpuOffset = 0;
        for (SystemStructure node : global.getSubComponents()) {
            cpuOffset += node.getNumberOfDirectSubcomponents();
        }
        int nodeOffset = global.getNumberOfDirectSubcomponents();
        SystemComposition<PRVNode> traceNodes = trace.getInfrastructure();
        for (PRVNode node : traceNodes.getSubComponents()) {
            node.applyOffset(nodeOffset);
            global.appendComponent(node);
        }
        return new CPUOffset(cpuOffset);
    }

    private static TraceTransformation mergeTraceThreads(Trace trace, ApplicationComposition global) {
        AgentThreadTranslator translator = new AgentThreadTranslator(global);

        int appOffset = global.getNumberOfDirectSubcomponents();
        appOffset++;
        ApplicationComposition<PRVApplication> system = trace.getThreadOrganization();

        for (PRVApplication app : system.getSubComponents()) {
            PRVApplication newApp = new PRVApplication();

            int taskOffset = 1;
            for (PRVTask task : app.getSubComponents()) {
                PRVTask newTask = new PRVTask();
                newTask.setNode(task.getNode());
                int threadOffset = 1;
                for (Thread thread : task.getSubComponents()) {
                    ThreadIdentifier oldThreadID = thread.getIdentifier();
                    ThreadIdentifier newThreadID = new PRVThreadIdentifier(appOffset, taskOffset, threadOffset);
                    translator.registerTranslation(oldThreadID, newThreadID);
                    String oldLabel = thread.getLabel();
                    String newLabel = computeNewLabel(oldLabel, oldThreadID, newThreadID);
                    Thread newThread = new Thread(newThreadID, newLabel);
                    newTask.appendComponent(newThread);
                    threadOffset++;
                }
                newApp.appendComponent(newTask);
                taskOffset++;
            }
            global.appendComponent(newApp);
            appOffset++;
        }
        return new ThreadTranslation(translator);
    }

    private static String computeNewLabel(String label, ThreadIdentifier oldId, ThreadIdentifier newId) {
        String oldLabel = oldId.toString();
        String newLabel = newId.toString();
        label = label.replace(oldLabel, newLabel);
        oldLabel = oldLabel.replace(":", ".");
        newLabel = newLabel.replace(":", ".");
        label = label.replace(oldLabel, newLabel);
        return label;
    }


    public static class AgentThreadTranslator implements ThreadTranslator {

        private final HashMap<ThreadIdentifier, ThreadIdentifier> translations;
        private final ApplicationComposition organization;


        private AgentThreadTranslator(ApplicationComposition global) {
            this.organization = global;
            this.translations = new HashMap<>();
        }

        public void registerTranslation(ThreadIdentifier oldThreadID, ThreadIdentifier newThreadID) {
            this.translations.put(oldThreadID, newThreadID);
        }

        @Override
        public ThreadIdentifier getNewThreadId(ThreadIdentifier t) {
            return this.translations.get(t);
        }

        @Override
        public ApplicationComposition getNewThreadOrganization() {
            return this.organization;
        }

        @Override
        public String getDescription() {
            StringBuilder sb = new StringBuilder();
            for (Map.Entry<ThreadIdentifier, ThreadIdentifier> entry : this.translations.entrySet()) {
                sb.append("\t * ").append(entry.getKey()).append("->").append(entry.getValue()).append("\n");
            }
            return sb.toString();
        }

    }


    /**
     * Merges the traces generated in the traceDirs directories and leaving the resulting trace in outputDir.
     * 
     * @param args String[] containing {appName, outputFolder, jobId and a list of folder paths containing the trace
     *            files to merge}
     * @throws IOException error managing files
     */
    public static void main(String[] args) throws IOException {
        // Parse arguments
        final String outputDir = args[0];
        final String resTraceName = args[1];

        final PRVTrace[] traces = new PRVTrace[args.length - 2];

        // Setting up logger
        // loggerSetUp(outputDir);

        System.out.println("----------------------------------------");
        System.out.println("Initiating agent trace merging");
        System.out.println("----------------------------------------");
        System.out.println("Merging the following traces:");

        LOGGER.debug("----------------------------------------");
        LOGGER.debug("Initiating agent trace merging");
        LOGGER.debug("----------------------------------------");
        LOGGER.debug("Merging the following traces:");

        for (int idx = 2; idx < args.length; idx++) {
            String agentPrv = args[idx];
            PRVTrace agentTrace = new PRVTrace(new File(agentPrv + ".prv"));
            traces[idx - 2] = agentTrace;
            System.out.println("    " + agentPrv);
            LOGGER.debug("    " + agentPrv);
        }

        System.out.println("Result trace name: " + resTraceName);
        System.out.println("OutputDir: " + outputDir);
        LOGGER.debug("Result trace name: " + resTraceName);
        LOGGER.debug("OutputDir: " + outputDir);

        try {
            LOGGER.debug("Initializing AgentTraceMerger");
            AgentTraceMerger tm = new AgentTraceMerger(traces, outputDir, resTraceName);
            LOGGER.debug("AgentTraceMerger initialization successful");
            tm.merge();
        } catch (Throwable t) {
            System.err.println("Failed to correctly merge traces: exception risen");
            LOGGER.error("Failed to correctly merge traces: exception risen", t);
        }

    }
}
