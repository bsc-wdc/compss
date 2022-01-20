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
package es.bsc.compss.util.types;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.util.tracing.Threads;
import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintWriter;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Stores the threads information with addThread and creates the maps needed to translate the threads in both the .prv
 * and .row files.
 *
 * @throws Exception Exception parsing the line
 */
public class ThreadTranslator {

    protected static final Logger LOGGER = LogManager.getLogger(Loggers.TRACING);

    private static final String THREAD_ID_EVENT_TYPE = Integer.toString(Tracer.getThreadIdEventsType());

    // machineRuntimeIdentifiers returns for each machineId a map from a threadIdEvent to the thread
    private final List<Machine> machines = new ArrayList<>();
    public Map<String, String> threadTranslations;


    public ThreadTranslator() {
    }

    /**
     * Constructs a new ThreadTranslator and sets up the mappings with the content of a prv file.
     * 
     * @param prvFile prv file to parse
     * @throws FileNotFoundException prvFile doesn't exist
     * @throws IOException error raised during prv file reading
     */
    public ThreadTranslator(File prvFile) throws FileNotFoundException, IOException {
        this();
        parsePrvFile(prvFile);
    }

    /**
     * Parses the prv file and to set up the translation mappings.
     * 
     * @param prvFile PRV file to parse.
     * @throws FileNotFoundException prvFile doesn't exist
     * @throws IOException error raised during prv file reading
     */
    public final void parsePrvFile(File prvFile) throws FileNotFoundException, IOException {
        try (BufferedReader br = new BufferedReader(new FileReader(prvFile))) {
            br.readLine(); // we don't need the header right now
            String line;
            // the isEmpty check should not be necessary if the .prv files are well constructed
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                PrvLine prvLine = new PrvLine(line);
                String threadId = prvLine.getStateLineThreadIdentifier();

                int machineId = Integer.parseInt(PrvLine.getNodeId(threadId));

                Map<String, String> events = prvLine.getEvents();
                String identifierEventValue = events.get(THREAD_ID_EVENT_TYPE);

                registerThread(machineId, threadId, identifierEventValue);
            }
        } // we don't need the header right now
        computeTranslationMap();
    }

    /**
     * Updates the threads in .prv with the information from translations.
     *
     * @param prvFile prv file to update
     * @throws FileNotFoundException prvFile doesn't exist
     * @throws IOException error raised during prv file reading or new file writing
     * @throws Exception PRV line has a wrong format
     */
    public void translatePrvFile(File prvFile) throws FileNotFoundException, IOException, Exception {
        LOGGER.debug("Tracing: Updating thread identifiers in .prv file");
        final String oldFilePath = prvFile.getAbsolutePath();
        final String newFilePath = oldFilePath + "_tmp_updatedThreadsId";
        final File updatedPrvFile = new File(newFilePath);
        if (!updatedPrvFile.exists()) {
            updatedPrvFile.createNewFile();
        }
        final PrintWriter prvWriter;
        try (BufferedReader br = new BufferedReader(new FileReader(prvFile))) {
            prvWriter = new PrintWriter(new FileWriter(updatedPrvFile.getAbsolutePath(), true));
            PrvHeader header = new PrvHeader(br.readLine());
            // Needed in the case of the runcompss, won't do anything in agents
            header.transformNodesToAplications();
            header.splitRuntimeExecutors(createRuntimeThreadNumberPerApp());
            prvWriter.println(header.toString());
            String line;
            // the isEmpty check should not be necessary if the .prv files are well constructed
            while ((line = br.readLine()) != null && !line.isEmpty()) {
                PrvLine prvLine = new PrvLine(line);
                prvLine.translateLineThreads(this.threadTranslations);
                prvWriter.println(prvLine.toString());
            }
        }
        prvWriter.close();
        updatedPrvFile.renameTo(new File(oldFilePath));
    }

    /**
     * Updates a row file according to the translator setup.
     *
     * @param rowFile Row file to update
     * @throws IOException error arised writing new row file
     * @throws Exception could not update the row file
     */
    public final void translateRowFile(File rowFile) throws IOException, Exception {
        List<String> labels = getRowLabels();
        RowFile rFile = new RowFile(rowFile);
        rFile.updateRowLabels(labels);
        rFile.printInfo(rowFile);
    }

    private void registerThread(int machineId, String threadId, String threadTypeIdString) {
        while (machines.size() < (machineId)) {
            machines.add(new Machine());
        }
        Machine machine = machines.get(machineId - 1);
        machine.addThread(threadId);
        if (threadTypeIdString != null) {
            Integer threadTypeId = new Integer(threadTypeIdString);
            if (threadTypeId == Threads.EXEC.id) {
                machine.addExecutor(threadId);
            } else {
                if (threadTypeId != 0) { // != end event
                    machine.putRuntimeIdentifier(threadTypeId, threadId);
                }
            }
        }
    }

    private void computeTranslationMap() {

        threadTranslations = new HashMap<>();
        for (int i = 0; i < machines.size(); i++) {
            Machine m = machines.get(i);
            // for thread 1.X.1 -> X.1.1, main thread has no event and thus is not by addThread()
            String iString = Integer.toString(i + 1);
            threadTranslations.put("1:" + iString + ":1", iString + ":1:1");
            Map<Integer, String> runtimeIdentifiers = m.getRuntimeIdentifiers();
            Set<String> runtimeList = m.getThreads();
            Set<String> executorList = m.getExecutors();
            Set<String> translatedUnknownList = m.getTranslatedMachineUnknowns();
            int runtimeThreadsNum = 2;
            for (int ident = 0; ident < Threads.EXEC.id; ident++) {
                if (runtimeIdentifiers.containsKey(ident)) {
                    String oldThread = runtimeIdentifiers.get(ident);
                    String newThread = PrvLine.changeThreadNumber(oldThread, runtimeThreadsNum++);
                    newThread = PrvLine.moveNodeIdToFirstPosition(newThread);
                    newThread = PrvLine.changeRuntimeNumber(newThread, true);
                    threadTranslations.put(oldThread, newThread);
                }
            }
            int executorsNum = 1;
            for (String oldThread : executorList) {
                String newThread = PrvLine.changeThreadNumber(oldThread, executorsNum++);
                newThread = PrvLine.moveNodeIdToFirstPosition(newThread);
                newThread = PrvLine.changeRuntimeNumber(newThread, false);
                threadTranslations.put(oldThread, newThread);
            }
            for (String oldThread : runtimeList) {
                if (!threadTranslations.containsKey(oldThread)) {
                    String newThread = PrvLine.changeThreadNumber(oldThread, runtimeThreadsNum++);
                    newThread = PrvLine.moveNodeIdToFirstPosition(newThread);
                    newThread = PrvLine.changeRuntimeNumber(newThread, true);
                    translatedUnknownList.add(newThread);
                    threadTranslations.put(oldThread, newThread);
                }
            }
        }
    }

    private String createLabel(String threadId, int identifierEvent) {
        String label = Threads.getLabelByID(identifierEvent);
        return label + " (" + threadId + ")";
    }

    /**
     * Returns the maps needed to translate the threads of the .row based on the information received with addThread().
     *
     * @throws Exception createThreadTranslationMap() not called before
     */
    private List<String> getRowLabels() throws Exception {
        List<String> labels = new ArrayList<>();
        labels.add("MAIN APP (1.1.1)");
        for (int i = 1; i < machines.size(); i++) {
            String iString = Integer.toString(i + 1);
            labels.add("WORKER MAIN (" + iString + ".1.1)");
        }

        for (Machine m : machines) {
            for (Entry<Integer, String> identifier : m.getRuntimeIdentifiers().entrySet()) {
                int eventIdentifier = identifier.getKey();
                String newThreadId = threadTranslations.get(identifier.getValue()).replace(":", ".");
                String newLabel = createLabel(newThreadId, eventIdentifier);
                labels.add(newLabel);
            }
        }

        for (Machine m : machines) {
            for (String exec : m.getExecutors()) {
                String newThreadId = threadTranslations.get(exec).replace(":", ".");
                String newLabel = createLabel(newThreadId, Threads.EXEC.id);
                labels.add(newLabel);
            }
        }
        for (Machine m : machines) {
            for (String unkn : m.translatedMachineUnknowns) {
                labels.add("THREAD " + unkn.replace(":", "."));
            }
        }
        return labels;
    }

    /**
     * Returns the number of runtime threads that each app should have.
     */
    private int[] createRuntimeThreadNumberPerApp() {
        int[] result = new int[machines.size()];
        for (int i = 0; i < result.length; i++) {
            Machine machine = machines.get(i);
            result[i] = machine.getNumThreads() - machine.getNumExecutors();
        }
        return result;
    }


    private static class Machine {

        private Map<Integer, String> runtimeIdentifiers;
        private Set<String> threads;
        private Set<String> executors;
        private Set<String> translatedMachineUnknowns;


        public Machine() {
            threads = new HashSet<>();
            executors = new HashSet<>();
            translatedMachineUnknowns = new HashSet<>();
            runtimeIdentifiers = new HashMap<>();
        }

        private void addThread(String threadId) {
            threads.add(threadId);
        }

        private Set<String> getThreads() {
            return this.threads;
        }

        private int getNumThreads() {
            return this.threads.size();
        }

        private void addExecutor(String threadId) {
            this.executors.add(threadId);
        }

        private Set<String> getExecutors() {
            return this.executors;
        }

        private int getNumExecutors() {
            return this.executors.size();
        }

        private void putRuntimeIdentifier(Integer threadTypeId, String threadId) {
            runtimeIdentifiers.put(threadTypeId, threadId);
        }

        private Map<Integer, String> getRuntimeIdentifiers() {
            return this.runtimeIdentifiers;
        }

        private Set<String> getTranslatedMachineUnknowns() {
            return this.translatedMachineUnknowns;
        }

    }
}
