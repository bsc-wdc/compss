/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
package es.bsc.compss.scheduler.predefined.types;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.scheduler.types.AllocatableAction;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;

public class PredefinedData {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TS_COMP);
    private static final String LOG_PREFIX = "[PredefinedData] ";

    private final Map<Integer, String> resourceMap = new HashMap<>();
    private final Map<Integer, List<String>> multiNodeResourcesMap = new HashMap<>(); // For multi-node tasks
    private final Map<Integer, Integer> implMap = new HashMap<>();
    private final Map<Integer, List<Integer>> predMap = new HashMap<>();
    private final Map<Integer, List<Integer>> succMap = new HashMap<>();

    private final Map<Integer, AllocatableAction> waitingActions = new HashMap<>();
    private final Map<Integer, List<AllocatableAction>> waitingMultiNodeActions = new HashMap<>();
    private boolean[] finished;
    private int maxActionId = 0;


    /**
     * Constructs and loads predefined data from the given JSON file path.
     * 
     * @param jsonPath Path to the JSON file
     * @throws IOException if file reading fails
     */
    public PredefinedData(String jsonPath) throws IOException {
        LOGGER.info(LOG_PREFIX + "Loading Predefined Data from " + jsonPath);
        loadFromJson(jsonPath);
        initializeFinishedArray();
        buildSuccessors();
    }

    private void loadFromJson(String filename) throws IOException {
        try (InputStream is = new FileInputStream(filename)) {
            JSONTokener tokener = new JSONTokener(is);
            JSONArray tasks = new JSONArray(tokener);

            for (int i = 0; i < tasks.length(); i++) {
                JSONObject task = tasks.getJSONObject(i);
                int id = task.getInt("taskId");
                if (id > maxActionId) {
                    maxActionId = id;
                }

                int implId = task.optInt("implementationId", -1); // Default to -1 if not present

                // Check if it's a multi-node task (has "resources" array) or single-node (has
                // "resource" string)
                if (task.has("resources")) {
                    // Multi-node task
                    JSONArray resourcesJson = task.getJSONArray("resources");
                    List<String> resources = new ArrayList<>();
                    for (int j = 0; j < resourcesJson.length(); j++) {
                        resources.add(resourcesJson.getString(j));
                    }
                    multiNodeResourcesMap.put(id, resources);
                    LOGGER.debug(LOG_PREFIX + "Task " + id + " is multi-node with " + resources.size() + " resources");
                } else {
                    // Single-node task
                    String resource = task.getString("resource");
                    resourceMap.put(id, resource);
                }

                if (implId != -1) {
                    implMap.put(id, implId);
                }

                JSONArray predsJson = task.optJSONArray("predecessors");
                List<Integer> preds = new ArrayList<>();
                if (predsJson != null) {
                    for (int j = 0; j < predsJson.length(); j++) {
                        preds.add(predsJson.getInt(j));
                    }
                }
                predMap.put(id, preds);
            }
        } catch (Exception e) {
            throw new IOException("Error parsing JSON file: " + filename, e);
        }
    }

    private void initializeFinishedArray() {
        // Creamos array de tamaño maxActionId+1, valores false por defecto
        finished = new boolean[maxActionId + 1];
    }

    private void buildSuccessors() {
        // Pre-dimensionar el mapa para evitar rehashing
        int estimatedSize = (int) (predMap.size() * 1.25);
        Map<Integer, List<Integer>> tempSuccMap = new HashMap<>(estimatedSize);

        for (Map.Entry<Integer, List<Integer>> entry : predMap.entrySet()) {
            int taskId = entry.getKey();
            List<Integer> predecessors = entry.getValue();

            for (Integer pred : predecessors) {
                tempSuccMap.computeIfAbsent(pred, k -> new ArrayList<>()).add(taskId);
            }
        }

        // Optimizar listas de successors
        for (List<Integer> succList : tempSuccMap.values()) {
            ((ArrayList<Integer>) succList).trimToSize();
        }

        succMap.putAll(tempSuccMap);
        LOGGER.debug(LOG_PREFIX + "Built " + succMap.size() + " successor relationships");
    }

    public boolean containsAction(int actionId) {
        return resourceMap.containsKey(actionId) || multiNodeResourcesMap.containsKey(actionId);
    }

    /**
     * Returns whether the given action is a multi-node task.
     */
    public boolean isMultiNodeAction(int actionId) {
        return multiNodeResourcesMap.containsKey(actionId);
    }

    /**
     * Returns a list of predecessor action IDs for the given action ID.
     */
    public List<Integer> getPredecessors(int actionId) {
        return predMap.getOrDefault(actionId, Collections.emptyList());
    }

    /**
     * Returns a list of successor action IDs for the given action ID.
     */
    public List<Integer> getSuccessors(int actionId) {
        return succMap.getOrDefault(actionId, Collections.emptyList());
    }

    /**
     * Returns a map of all waiting actions.
     */
    public Map<Integer, AllocatableAction> getWaitingActions() {
        return waitingActions;
    }

    /**
     * Returns a map of all waiting multi-node actions.
     */
    public Map<Integer, List<AllocatableAction>> getWaitingMultiNodeActions() {
        return waitingMultiNodeActions;
    }

    /**
     * Marks an action as finished.
     * 
     * @param actionId Action Identifier.
     */
    public void setFinished(int actionId) {
        if (actionId >= 0 && actionId < finished.length) {
            finished[actionId] = true;
        }
    }

    /**
     * Returns whether an action is finished or not.
     * 
     * @param actionId Action Identifier.
     * @return True if finished, false otherwise.
     */
    public boolean isFinished(int actionId) {
        if (actionId >= 0 && actionId < finished.length) {
            return finished[actionId];
        }
        return false;
    }

    /**
     * Returns the resource name assigned to the given action ID (for single-node tasks).
     */
    public String getResourceForAction(int actionId) {
        return resourceMap.get(actionId);
    }

    /**
     * Returns the list of resources assigned to the given action ID (for multi-node tasks).
     */
    public List<String> getResourcesForMultiNodeAction(int actionId) {
        return multiNodeResourcesMap.getOrDefault(actionId, Collections.emptyList());
    }

    /**
     * Returns the implementation ID assigned to the given action ID.
     * 
     * @param actionId Action Identifier.
     * @return Implementation ID or null if not set.
     */
    public Integer getImplementationId(int actionId) {
        return implMap.get(actionId);
    }
}
