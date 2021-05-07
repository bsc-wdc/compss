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

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

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
    private List<List<String>> machineExecutors = new ArrayList<List<String>>();
    public Map<String, String> threadTranslations;


    public ThreadTranslator() {
    }

    /**
     * Processes the thread information.
     */
    public void addThread(String threadId, int threadTypeId) {
        int machineId = Integer.parseInt(PrvLine.getNodeId(threadId));
        while (machineExecutors.size() <= (machineId)) {
            machineExecutors.add(new ArrayList<String>());
        }
        while (machineRuntimeIdentifiers.size() < (machineId)) {
            machineRuntimeIdentifiers.add(new HashMap<Integer, String>());
        }
        if (threadTypeId == Tracer.EXECUTOR_ID) {
            machineExecutors.get(machineId - 1).add(threadId);
        } else {
            machineRuntimeIdentifiers.get(machineId - 1).put(threadTypeId, threadId);
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
            List<String> executorIdentifiers = machineExecutors.get(i);
            int runtimeThreadsNum = 1;
            for (int ident = 0; ident < Tracer.EXECUTOR_ID; ident++) {
                if (runtimeIdentifiers.containsKey(ident)) {
                    String oldThread = runtimeIdentifiers.get(ident);
                    String newThread = PrvLine.changeThreadNumber(oldThread, runtimeThreadsNum++);
                    newThread = PrvLine.moveNodeIdToFirstPosition(newThread);
                    newThread = PrvLine.changeRuntimeNumber(newThread, true);
                    threadTranslations.put(oldThread, newThread);
                }
            }
            int executorsNum = 1;
            for (String oldThread : executorIdentifiers) {
                String newThread = PrvLine.changeThreadNumber(oldThread, executorsNum++);
                newThread = PrvLine.moveNodeIdToFirstPosition(newThread);
                newThread = PrvLine.changeRuntimeNumber(newThread, false);
                threadTranslations.put(oldThread, newThread);
            }
        }
        return threadTranslations;
    }

    private String createLabel(String threadId, int identifierEvent) {
        String label;
        switch (identifierEvent) {
            case Tracer.AP_ID:
                label = "RUNTIME AP";
                break;
            case Tracer.TD_ID:
                label = "RUNTIME TD";
                break;
            case Tracer.FS_ID:
                label = "RUNTIME FS";
                break;
            case Tracer.TIMER_ID:
                label = "RUNTIME TIMER";
                break;
            case Tracer.EXECUTOR_ID:
                label = "EXECUTOR";
                break;

            default:
                label = "";
                break;
        }
        return label + " (" + threadId + ")";
    }

    /**
     * Returns the maps needed to translate the threads of the .row based on the information received with addThread().
     * 
     * @throws Exception createThreadTranslationMap() not called before
     */
    public Map<String, String> createLabelTranslationMap() throws Exception {
        if (this.threadTranslations == null) {
            throw new Exception(
                "createThreadTranslationMap() must be created before invocking createLabelTranslationMap()");
        }
        Map<String, String> labelTranslations = new HashMap<String, String>();
        labelTranslations.put("THREAD 1.1.1", "MAIN APP (1.1.1)");
        for (int i = 1; i < machineRuntimeIdentifiers.size(); i++) {
            String iString = Integer.toString(i + 1);
            labelTranslations.put("THREAD 1." + iString + ".1", "WORKER MAIN (" + iString + ".1.1)");
        }

        for (Map<Integer, String> runtimeIdentifiers : machineRuntimeIdentifiers) {
            for (Entry<Integer, String> identifier : runtimeIdentifiers.entrySet()) {
                int eventIdentifier = identifier.getKey();
                String oldLabel = "THREAD " + identifier.getValue().replace(":", ".");
                String newThreadId = threadTranslations.get(identifier.getValue()).replace(":", ".");
                String newLabel = createLabel(newThreadId, eventIdentifier);
                labelTranslations.put(oldLabel, newLabel);
            }
        }
        for (List<String> executorIdentifiers : machineExecutors) {
            for (String exec : executorIdentifiers) {
                String oldLabel = "THREAD " + exec.replace(":", ".");
                String newThreadId = threadTranslations.get(exec).replace(":", ".");
                String newLabel = createLabel(newThreadId, Tracer.EXECUTOR_ID);
                labelTranslations.put(oldLabel, newLabel);
            }
        }
        return labelTranslations;
    }

    /**
     * Returns the number of runtime threads that each app should have.
     */
    public int[] createRuntimeThreadNumberPerApp() {
        int[] result = new int[machineRuntimeIdentifiers.size()];
        for (int i = 0; i < result.length; i++) {
            result[i] = machineRuntimeIdentifiers.get(i).size();
        }
        return result;
    }
}
