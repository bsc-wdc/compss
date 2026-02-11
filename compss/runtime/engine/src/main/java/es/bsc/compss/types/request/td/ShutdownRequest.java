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
package es.bsc.compss.types.request.td;

import es.bsc.compss.components.impl.TaskDispatcher;
import es.bsc.compss.components.impl.TaskScheduler;
import es.bsc.compss.log.LoggerManager;
import es.bsc.compss.types.request.exceptions.ShutdownException;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.util.ResourceManager;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;

import org.json.JSONArray;
import org.json.JSONObject;
import org.json.JSONTokener;


/**
 * This class represents a notification to end the execution.
 */
public class ShutdownRequest extends TaskDispatcher.SynchTDRequest<Void> {

    /**
     * Constructs a new ShutdownRequest.
     *
     * @param td TaskDispatcher processing the event
     */
    public ShutdownRequest(TaskDispatcher td) {
        td.super();
    }

    @Override
    public void process(TaskScheduler ts) throws ShutdownException {
        TaskDispatcher.LOGGER.debug("Processing ShutdownRequest request...");

        // Shutdown TaskScheduler
        ts.shutdown(new TaskScheduler.ShutdownListener() {

            @Override
            public void onShutdown() {
                new ShutdownConfirmation(getTaskDispatcher()).offer("shutdown confirmation");
            }
        });
    }


    private class ShutdownConfirmation extends TaskDispatcher.AsynchTDRequest {

        public ShutdownConfirmation(TaskDispatcher td) {
            td.super();
        }

        @Override
        public void process(TaskScheduler ts) throws ShutdownException {
            ts.confirmShutdown();

            ResourceManager.stopNodes();

            // All nodes are stopped and analysis&debug data has been collected
            mergeCacheProfiles();

            // The semaphore is released after emitting the end event to prevent race conditions
            throw new ShutdownException(getSemaphore());
        }

        @Override
        public TraceEvent getEvent() {
            return TraceEvent.TD_SHUTDOWN;
        }
    }


    @Override
    public TraceEvent getEvent() {
        return TraceEvent.TD_SHUTDOWN;
    }

    private static void mergeCacheProfiles() {
        String logPath = LoggerManager.getWorkersLogDir();
        File folder = new File(logPath);
        HashMap<String, HashMap<String, HashMap<String, Object>>> globalProfile = new HashMap<>();

        boolean filesExist = false;
        for (String f : folder.list()) {
            if (f.startsWith("cache_profiler")) {
                filesExist = true;
                try {
                    JSONTokener tokener = new JSONTokener(new FileReader(logPath + f));
                    JSONObject localProfile = new JSONObject(tokener);

                    for (String function : localProfile.keySet()) {
                        JSONObject localFuncProfile = localProfile.getJSONObject(function);

                        HashMap<String, HashMap<String, Object>> globalFuncProf = globalProfile.get(function);
                        if (globalFuncProf == null) {
                            globalFuncProf = new HashMap<>();
                            globalProfile.put(function, globalFuncProf);
                        }

                        for (String parameter : localFuncProfile.keySet()) {
                            JSONObject localParProf = localFuncProfile.getJSONObject(parameter);
                            HashMap<String, Object> globalParProf = globalFuncProf.get(parameter);
                            if (globalParProf == null) {
                                globalParProf = new HashMap<>();
                                globalFuncProf.put(parameter, globalParProf);
                            }

                            for (String key : localParProf.keySet()) {
                                if (!globalParProf.containsKey(key)) {
                                    if (key.equals("USED")) {
                                        ArrayList<String> globalUsed = new ArrayList<>();
                                        for (Object o : (JSONArray) localParProf.get(key)) {
                                            String s = o.toString();
                                            if (!globalUsed.contains(s)) {
                                                globalUsed.add(s);
                                            }
                                        }
                                        globalParProf.put(key, globalUsed);
                                    } else {
                                        globalParProf.put(key, Integer.valueOf(localParProf.get(key).toString()));
                                    }
                                } else {
                                    if (key.equals("USED")) {
                                        ArrayList<String> globalUsed = (ArrayList<String>) globalParProf.get(key);
                                        for (Object o : (JSONArray) localParProf.get(key)) {
                                            String s = o.toString();
                                            if (!globalUsed.contains(s)) {
                                                globalUsed.add(s);
                                            }
                                        }
                                    } else {
                                        int globalKeyValue = (int) globalParProf.get(key);
                                        int localKeyValue = Integer.parseInt(localParProf.get(key).toString());
                                        globalKeyValue += localKeyValue;
                                        globalParProf.put(key, globalKeyValue);
                                    }
                                }
                            }
                        }
                    }
                } catch (FileNotFoundException e) {
                    TaskDispatcher.LOGGER.debug("Could not merge cache profiles. File not found", e);
                }
                File file = new File(logPath + f);
                file.delete();
            }

        }
        if (filesExist) {
            try {
                int totalGets = 0;
                int totalPuts = 0;
                String filename = "profiler_cache_summary.out";
                FileWriter writer = new FileWriter(logPath + filename);
                writer.write("PROFILER SUMMARY" + "\n");
                for (String function : globalProfile.keySet()) {
                    writer.write('\t' + "FUNCTION: " + function + "\n");

                    HashMap<String, HashMap<String, Object>> funcProf = globalProfile.get(function);
                    for (String parameter : funcProf.keySet()) {
                        HashMap<String, Object> parProf = funcProf.get(parameter);
                        writer.write('\t' + " " + '\t' + " " + '\t' + "PARAMETER: " + parameter + "\n");
                        int puts = (int) parProf.get("PUT");
                        int gets = (int) parProf.get("GET");
                        totalGets += gets;
                        totalPuts += puts;
                        ArrayList<String> used = (ArrayList<String>) parProf.get("USED");
                        if (used.size() > 0 || gets > 0) {
                            writer.write('\t' + " " + '\t' + " " + '\t' + " " + '\t' + "PUTS: " + puts + " GETS: "
                                + gets + ". USED IN: " + used + "\n");
                        } else {
                            writer.write('\t' + " " + '\t' + " " + '\t' + " " + '\t' + "[NOT USED]  PUTS: " + puts
                                + " GETS: " + gets + "\n");
                        }
                    }
                }
                writer.write("TOTAL GETS: " + totalGets + "\n");
                writer.write("TOTAL PUTS: " + totalPuts + "\n");
                writer.close();
            } catch (IOException e) {
                TaskDispatcher.LOGGER.debug("Could not merge cache profiles.", e);
            }
        }
    }
}
