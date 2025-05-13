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


/**
 * COMPSS Runtime Constants.
 */
public class COMPSsConstants {

    /**
     * Accepted Execution modes.
     */
    public static enum TaskExecution {
        COMPSS, // Internal Execution
        STORAGE // External execution
    }

    /**
     * Languages.
     */
    public static enum Lang {
        JAVA, // For Java applications
        C, // For C or C++ applications
        PYTHON, // For Python applications
        R, // For R applications
        UNKNOWN // For Services
    }

    /**
     * Python Interpreters.
     */
    public static enum Pythons {
        python, // For systemwide python interpreter
        python3 // For python 3 interpreter
    }


    /*
     * Environment Properties
     */
    public static final String COMPSS_HOME = "COMPSS_HOME";
    public static final String LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
    public static final String COMPSS_WORKING_DIR = "COMPSS_WORKING_DIR";
    public static final String COMPSS_APP_DIR = "COMPSS_APP_DIR";
    public static final String GAT_LOC = "GAT_LOCATION";
    public static final String COMPSS_MPIRUN_TYPE = "COMPSS_MPIRUN_TYPE";
    public static final String COMPSS_SSH_TIMEOUT = "COMPSS_SSH_TIMEOUT";
    public static final String COMPSS_WORKER_INIT_TIMEOUT = "COMPSS_WORKER_INIT_TIMEOUT";
    public static final String COMPSS_HTTP_POOL_SIZE = "COMPSS_HTTP_POOL_SIZE";
    /*
     * Component names
     */
    public static final String TA = "Task Analyser";
    public static final String TS = "Task Scheduler";
    public static final String JM = "Job Manager";
    public static final String DM = "Data Manager";
    public static final String DIP = "Data Information Provider";
    public static final String FTM = "File Transfer Manager";

    /*
     * Dynamic system properties
     */
    public static final String APP_NAME = "compss.appName";
    public static final String SERVICE_NAME = "compss.serviceName";
    public static final String MASTER_NAME = "compss.masterName";
    public static final String MASTER_PORT = "compss.masterPort";
    public static final String DEPLOYMENT_ID = "compss.uuid";
    public static final String EXEC_LABEL = "compss.exec.label";
    public static final String SHUTDOWN_IN_NODE_FAILURE = "compss.shutdown_in_node_failure";

    public static final String LOG_DIR = "compss.log.dir";
    public static final String WORKING_DIR = "compss.master.workingDir";

    public static final String PROJ_FILE = "compss.project.file";
    public static final String PROJ_SCHEMA = "compss.project.schema";
    public static final String RES_FILE = "compss.resources.file";
    public static final String RES_SCHEMA = "compss.resources.schema";

    public static final String LANG = "compss.lang";
    public static final String TASK_SUMMARY = "compss.summary";

    public static final String CHECKPOINT_POLICY = "compss.checkpoint.policy";
    public static final String CHECKPOINT_PARAMS = "compss.checkpoint.params";
    public static final String CHECKPOINT_FOLDER_PATH = "compss.checkpoint.folder";

    public static final String CONSTR_FILE = "compss.constraints.file";
    public static final String SCHEDULER = "compss.scheduler";
    public static final String SCHEDULER_CONFIG_FILE = "compss.scheduler.config";
    public static final String PRESCHED = "compss.presched";
    public static final String GRAPH = "compss.graph";
    public static final String MONITOR = "compss.monitor";
    public static final String INPUT_PROFILE = "compss.profile.input";
    public static final String OUTPUT_PROFILE = "compss.profile.output";
    public static final String EXTERNAL_ADAPTATION = "compss.external.adaptation";

    public static final String DATA_PROVENANCE = "compss.data_provenance";

    public static final String TRACING = "compss.tracing";
    public static final String TRACING_TASK_DEPENDENCIES = "compss.tracing.task.dependencies";
    public static final String EXTRAE_WORKING_DIR = "compss.extrae.working_dir";
    public static final String EXTRAE_CONFIG_FILE = "compss.extrae.file";

    public static final String WORKER_CP = "compss.worker.cp";
    public static final String WORKER_PP = "compss.worker.pythonpath";
    public static final String WORKER_JVM_OPTS = "compss.worker.jvm_opts";
    public static final String WORKER_FPGA_REPROGRAM = "compss.worker.fpga_reprogram";
    public static final String WORKER_REMOVE_WD = "compss.worker.removeWD";
    public static final String WORKER_CPU_AFFINITY = "compss.worker.cpu_affinity";
    public static final String WORKER_GPU_AFFINITY = "compss.worker.gpu_affinity";
    public static final String WORKER_FPGA_AFFINITY = "compss.worker.gpu_affinity";
    public static final String WORKER_IO_EXECUTORS = "compss.worker.io_executors";
    public static final String WORKER_APPDIR = "compss.worker.appdir";
    public static final String WORKER_BINARY_KILL_SIGNAL = "compss.worker.killSignal";
    public static final String WORKER_ENV_SCRIPT = "compss.worker.env_script";

    public static final String COMM_ADAPTOR = "compss.comm";
    public static final String CONN = "compss.conn";

    public static final String SERVICE_ADAPTOR = "es.bsc.compss.ws.master.WSAdaptor";
    public static final String HTTP_ADAPTOR = "es.bsc.compss.http.master.HTTPAdaptor";

    // GAT
    public static final String GAT_ADAPTOR_PATH = "gat.adaptor.path";
    public static final String GAT_DEBUG = "gat.debug";
    public static final String GAT_BROKER_ADAPTOR = "gat.broker.adaptor";
    public static final String GAT_FILE_ADAPTOR = "gat.file.adaptor";

    // LOCAL
    public static final String REUSE_RESOURCES_ON_BLOCK = "compss.execution.reuseOnBlock";
    public static final String ENABLED_NESTED_TASKS_DETECTION = "compss.execution.nested.enabled";

    // Storage properties
    public static final String STORAGE_CONF = "compss.storage.conf";
    public static final String TASK_EXECUTION = "compss.task.execution";

    // Streaming properties
    public static final String STREAMING_BACKEND = "compss.streaming";
    public static final String STREAMING_MASTER_NAME = "compss.streaming.masterName";
    public static final String STREAMING_MASTER_PORT = "compss.streaming.masterPort";

    // Timer properties
    public static final String TIMER_COMPSS_NAME = "compss.timers";

    // Persistent worker c property
    public static final String WORKER_PERSISTENT_C = "compss.worker.persistent.c";

    // Python properties
    public static final String PYTHON_INTERPRETER = "compss.python.interpreter";
    public static final String PYTHON_VERSION = "compss.python.version";
    public static final String PYTHON_VIRTUAL_ENVIRONMENT = "compss.python.virtualenvironment";
    public static final String PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT = "compss.python.propagate_virtualenvironment";
    public static final String PYTHON_MPI_WORKER = "compss.python.mpi_worker";
    public static final String PYTHON_WORKER_CACHE = "compss.python.worker_cache";
    public static final String PYTHON_CACHE_PROFILER = "compss.python.cache_profiler";
    public static final String PYTHON_EXTRAE_CONFIG_FILE = "compss.extrae.file.python";

    // EAR
    public static final String EAR = "compss.ear";

    // System properties for Instrumentation flags
    public static final String COMPSS_TO_FILE = "compss.to.file";
    public static final String COMPSS_IS_WS = "compss.is.ws";
    public static final String COMPSS_IS_MAINCLASS = "compss.is.mainclass";

    // Properties for locating the compss.properties file
    public static final String COMPSS_CONFIG = "compss.properties";
    public static final String COMPSS_CONFIG_LOCATION = "compss.properties.location";
    public static final String COMPSS_CONTEXT = "compss.context";

    // Wall clock limit definition
    public static final String COMPSS_WALL_CLOCK_LIMIT = "compss.wcl";

    // LOG 4J
    public static final String LOG4J = "log4j.configurationFile";

    // Docker execution related variable names

    public static final String DOCKER_APP_DIR_VOLUME = "DOCKER_APP_DIR_VOLUME";
    public static final String DOCKER_APP_DIR_MOUNT = "DOCKER_APP_DIR_MOUNT";
    public static final String DOCKER_PYCOMPSS_VOLUME = "DOCKER_PYCOMPSS_VOLUME";
    public static final String DOCKER_PYCOMPSS_MOUNT = "DOCKER_PYCOMPSS_MOUNT";
    public static final String DOCKER_WORKING_DIR_VOLUME = "DOCKER_WORKING_DIR_VOLUME";
    public static final String DOCKER_WORKING_DIR_MOUNT = "DOCKER_WORKING_DIR_MOUNT";

    public static final String COMPSS_CONTAINER_ENGINE = "COMPSS_CONTAINER_ENGINE";
    public static final String MASTER_CONTAINER_IMAGE = "MASTER_CONTAINER_IMAGE";
    public static final String MASTER_CONTAINER_OPTIONS = "MASTER_CONTAINER_OPTIONS";
    public static final String NESTED_CONTAINER_OPTIONS = "NESTED_CONTAINER_OPTIONS";
    public static final String MPI_RUNNER_SCRIPT = "MPI_RUNNER_SCRIPT";
    public static final String COMPSS_THROTTLE_MAX_TASKS = "COMPSS_THROTTLE_MAX_TASKS";
    public static final String COMPSS_THROTTLE_INTERVAL = "COMPSS_THROTTLE_INTERVAL";


    /**
     * Private constructor to avoid instantiation.
     */
    private COMPSsConstants() {
        throw new NonInstantiableException("COMPSsConstants");
    }

}
