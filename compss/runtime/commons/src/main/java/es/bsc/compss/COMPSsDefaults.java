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
package es.bsc.compss;

import es.bsc.compss.types.exceptions.NonInstantiableException;

import java.io.File;
import java.util.UUID;


/**
 * COMPSS Runtime Default values.
 */
public class COMPSsDefaults {

    // DEFAULT PLUG-IN COMPONENTS
    public static final String CHECKPOINT = "es.bsc.compss.checkpoint.policies.NoCheckpoint";

    public static final String SCHEDULER = "es.bsc.compss.components.impl.TaskScheduler";
    public static final String CONNECTOR = "es.bsc.compss.connectors.DefaultSSHConnector";

    public static final String ADAPTOR = "es.bsc.compss.nio.master.NIOAdaptor";

    // EXECUTION CONFIG
    public static final String DEPLOYMENT_ID = UUID.randomUUID().toString();
    public static final String WORKING_DIR = File.separator + "tmp" + File.separator + DEPLOYMENT_ID;

    public static final String PERSISTENT_C = "false";

    public static final String REUSE_RESOURCES_ON_BLOCK = "true";
    public static final String ENABLED_NESTED_TASKS_DETECTION = "false";

    public static final String PYTHON_INTERPRETER = "python3";
    public static final String PYTHON_VERSION = "3";
    public static final String PYTHON_VIRTUAL_ENVIRONMENT = "null";
    public static final String PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT = "true";
    public static final String PYTHON_MPI_WORKER = "false";
    public static final String PYTHON_WORKER_CACHE = "false";
    public static final String PYTHON_CACHE_PROFILER = "false";

    // EAR
    public static final String EAR = "false";
    // PROVENANCE
    public static final String DP_ENABLED = "false";

    // ANALYSIS TOOLS CONFIG
    // LOG
    public static final String LOG_DIR = System.getProperty("user.home") + File.separator + ".COMPSs" + File.separator;
    // TRACING
    public static final String TRACING = "false";
    public static final String CUSTOM_EXTRAE_FILE = "null";
    public static final String PYTHON_CUSTOM_EXTRAE_FILE = "null";
    // MONITOR
    public static final long MONITOR_INTERVAL = 0;


    /**
     * Private constructor to avoid instantiation.
     */
    private COMPSsDefaults() {
        throw new NonInstantiableException("COMPSsDefaults");
    }

}
