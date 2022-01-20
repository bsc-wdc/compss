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
import es.bsc.compss.util.tracing.Threads;

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
    // machineRuntimeIdentifiers returns for each machineId a map from a threadIdEvent to the thread
    private List<Map<Integer, String>> machineRuntimeIdentifiers = new ArrayList<Map<Integer, String>>();
    private List<Set<String>> machineThreads = new ArrayList<Set<String>>();
    private List<Set<String>> machineExecutors = new ArrayList<Set<String>>();
    private List<Set<String>> translatedMachineUnknowns = new ArrayList<Set<String>>();
    public Map<String, String> threadTranslations;


    public ThreadTranslator() {
    }

    /**
     * Processes the thread information.
     */
    public void addThread(String threadId, String threadTypeIdString) {
        int machineId = Integer.parseInt(PrvLine.getNodeId(threadId));
        while (machineThreads.size() < (machineId)) {
            machineThreads.add(new HashSet<String>());
            machineExecutors.add(new HashSet<String>());
            translatedMachineUnknowns.add(new HashSet<String>());
            machineRuntimeIdentifiers.add(new HashMap<Integer, String>());
        }
        machineThreads.get(machineId - 1).add(threadId);
        if (threadTypeIdString != null) {
            Integer threadTypeId = new Integer(threadTypeIdString);
            if (threadTypeId == Threads.EXEC.id) {
                machineExecutors.get(machineId - 1).add(threadId);
            } else if (threadTypeId != 0) { // != end event
                machineRuntimeIdentifiers.get(machineId - 1).put(threadTypeId, threadId);
            }
        }
    }

    /**
     * Returns the maps needed to translate the threads of the .prv based on the information received with addThread().
     */
    public Map<String, String> createThreadTranslationMap() {

        threadTranslations = new HashMap<String, String>();
        for (int i = 0; i < machineRuntimeIdentifiers.size(); i++) {
            // for thread 1.X.1 -> X.1.1, main thread has no event and thus is not by addThread()
            String iString = Integer.toString(i + 1);
            threadTranslations.put("1:" + iString + ":1", iString + ":1:1");
            Map<Integer, String> runtimeIdentifiers = machineRuntimeIdentifiers.get(i);
            Set<String> runtimeList = machineThreads.get(i);
            Set<String> executorList = machineExecutors.get(i);
            Set<String> translatedUnknownList = translatedMachineUnknowns.get(i);
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
        return threadTranslations;
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
    public List<String> getRowLabels() throws Exception {
        if (this.threadTranslations == null) {
            throw new Exception(
                "createThreadTranslationMap() must be created before invocking createLabelTranslationMap()");
        }
        List<String> labels = new ArrayList<String>();
        labels.add("MAIN APP (1.1.1)");
        for (int i = 1; i < machineRuntimeIdentifiers.size(); i++) {
            String iString = Integer.toString(i + 1);
            labels.add("WORKER MAIN (" + iString + ".1.1)");
        }

        for (Map<Integer, String> runtimeIdentifiers : machineRuntimeIdentifiers) {
            for (Entry<Integer, String> identifier : runtimeIdentifiers.entrySet()) {
                int eventIdentifier = identifier.getKey();
                String newThreadId = threadTranslations.get(identifier.getValue()).replace(":", ".");
                String newLabel = createLabel(newThreadId, eventIdentifier);
                labels.add(newLabel);
            }
        }
        for (Set<String> executorIdentifiers : machineExecutors) {
            for (String exec : executorIdentifiers) {
                String newThreadId = threadTranslations.get(exec).replace(":", ".");
                String newLabel = createLabel(newThreadId, Threads.EXEC.id);
                labels.add(newLabel);
            }
        }
        for (Set<String> unknownIdentifiers : translatedMachineUnknowns) {
            for (String unkn : unknownIdentifiers) {
                labels.add("THREAD " + unkn.replace(":", "."));
            }
        }
        return labels;
    }

    /**
     * Returns the number of runtime threads that each app should have.
     */
    public int[] createRuntimeThreadNumberPerApp() {
        int[] result = new int[machineRuntimeIdentifiers.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = machineThreads.get(i).size() - machineExecutors.get(i).size();
        }
        return result;
    }
}
