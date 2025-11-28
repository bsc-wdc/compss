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
package es.bsc.compss.checkpoint;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.util.Classpath;

import java.io.File;
import java.lang.reflect.Constructor;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CheckpointBuilder {

    // Component logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.TP_COMP);
    private static final boolean DEBUG = LOGGER.isDebugEnabled();

    private static final String CHECKPOINTER_REL_PATH = File.separator + "Runtime" + File.separator + "checkpointer";


    /**
     * Constructs the checkpoint Manager setting the parameters.
     * 
     * @param user User of the Checkpointing
     */
    public static CheckpointManager constructCheckpointManager(CheckpointManager.User user) throws Exception {
        loadCheckpointingPoliciesJars();

        CheckpointManager checkpointer = null;

        String parameters = System.getProperty(COMPSsConstants.CHECKPOINT_PARAMS);
        Map<String, String> paramsMap;
        if (parameters != null && !parameters.equals("")) {
            paramsMap = getCheckpointingConfig(parameters);
        } else {
            paramsMap = new HashMap<>();
        }

        String cpFQN = System.getProperty(COMPSsConstants.CHECKPOINT_POLICY);
        if (cpFQN == null || cpFQN.isEmpty()) {
            cpFQN = COMPSsDefaults.CHECKPOINT;
        }
        Class<?> cpClass = Class.forName(cpFQN);
        Constructor<?> cpCnstr = cpClass.getConstructor(HashMap.class, CheckpointManager.User.class);
        checkpointer = (CheckpointManager) cpCnstr.newInstance(paramsMap, user);
        if (DEBUG) {
            LOGGER.debug("Loaded checkpointer " + checkpointer);
        }
        return checkpointer;
    }

    /**
     * Loads the checkpoint policy.
     */
    private static void loadCheckpointingPoliciesJars() {
        LOGGER.info("Loading checkpointers...");
        String compssHome = System.getenv(COMPSsConstants.COMPSS_HOME);

        if (compssHome == null || compssHome.isEmpty()) {
            LOGGER.warn("WARN: COMPSS_HOME not defined, no checkpointers loaded.");
            return;
        }

        Classpath.loadJarsInPath(compssHome + CHECKPOINTER_REL_PATH, LOGGER);
    }

    /**
     * Converts a string describing the Checkpointer configuration into a Map.
     *
     * @param parameters String with all the configuration parameters
     * @return map of properties
     */
    private static Map<String, String> getCheckpointingConfig(String parameters) {
        HashMap<String, String> paramsMap = new HashMap<>();

        if (DEBUG) {
            LOGGER.debug("Reading Checkpointing policy parameters  " + parameters);
        }
        int index = parameters.indexOf("avoid.checkpoint");
        List<String> params;

        if (index != -1) {
            params = new ArrayList<>(Arrays.asList(parameters.substring(0, index).split(",")));
            String avoidTasks = parameters.substring(index);
            params.add(avoidTasks);

        } else {
            params = new ArrayList<>(Arrays.asList(parameters.split(",")));
        }
        for (String param : params) {
            String[] values = param.split(":");
            if (values[0].equals("period.time")) {
                int unit;
                if (values[1].endsWith("h")) {
                    unit = 3_600_000;
                } else {
                    if (values[1].endsWith("s")) {
                        unit = 1_000;
                    } else {
                        unit = 60_000;
                    }
                }
                int period = Integer.parseInt(values[1].substring(0, values[1].length() - 1)) * unit;
                values[1] = String.valueOf(period);
            }
            paramsMap.put(values[0], values[1]);
        }

        return paramsMap;
    }

}
