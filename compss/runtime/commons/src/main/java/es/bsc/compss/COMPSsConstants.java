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
package es.bsc.compss;

import es.bsc.compss.types.exceptions.NonInstantiableException;

import java.io.File;
import java.util.UUID;


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
    public static final String SHUTDOWN_IN_NODE_FAILURE = "compss.shutdown_in_node_failure";

    public static final String BASE_LOG_DIR = "compss.baseLogDir";
    public static final String SPECIFIC_LOG_DIR = "compss.specificLogDir";
    public static final String APP_LOG_DIR = "compss.appLogDir";

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

    /*
     * DEFAULT VALUES: According to runcompss script !!!!
     */
    public static final String DEFAULT_CHECKPOINT = "es.bsc.compss.checkpoint.policies.NoCheckpoint";

    public static final String DEFAULT_SCHEDULER = "es.bsc.compss.components.impl.TaskScheduler";

    public static final String SERVICE_ADAPTOR = "es.bsc.compss.ws.master.WSAdaptor";
    public static final String HTTP_ADAPTOR = "es.bsc.compss.http.master.HTTPAdaptor";

    // private static final String DEFAULT_ADAPTOR = "es.bsc.compss.gat.master.GATAdaptor";
    public static final String DEFAULT_ADAPTOR = "es.bsc.compss.nio.master.NIOAdaptor";

    public static final String DEFAULT_CONNECTOR = "es.bsc.compss.connectors.DefaultSSHConnector";

    public static final String DEFAULT_TRACING = "false";
    public static final String DEFAULT_CUSTOM_EXTRAE_FILE = "null";
    public static final String DEFAULT_PYTHON_CUSTOM_EXTRAE_FILE = "null";

    public static final long DEFAULT_MONITOR_INTERVAL = 0;

    public static final String DEFAULT_DEPLOYMENT_ID = UUID.randomUUID().toString();

    public static final String DEFAULT_CONFIG_DIR =
        System.getenv(COMPSS_HOME) + File.separator + "Runtime" + File.separator + "configuration";

    public static final String MPI_CFGS_PATH =
        File.separator + "Runtime" + File.separator + "configuration" + File.separator + "mpi" + File.separator;

    public static final String DEFAULT_RES_SCHEMA = DEFAULT_CONFIG_DIR + File.separator + "xml" + File.separator
        + "resources" + File.separator + "resource_schema.xsd";

    public static final String DEFAULT_PROJECT_SCHEMA = DEFAULT_CONFIG_DIR + File.separator + "xml" + File.separator
        + "projects" + File.separator + "project_schema.xsd";

    public static final String DEFAULT_GAT_ADAPTOR_LOCATION =
        System.getenv(GAT_LOC) + File.separator + "lib" + File.separator + "adaptors";
    public static final String DEFAULT_PERSISTENT_C = "false";

    public static final String DEFAULT_REUSE_RESOURCES_ON_BLOCK = "true";
    public static final String DEFAULT_ENABLED_NESTED_TASKS_DETECTION = "false";

    public static final String DEFAULT_PYTHON_INTERPRETER = "python3";
    public static final String DEFAULT_PYTHON_VERSION = "3";
    public static final String DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT = "null";
    public static final String DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT = "true";
    public static final String DEFAULT_PYTHON_MPI_WORKER = "false";
    public static final String DEFAULT_PYTHON_WORKER_CACHE = "false";
    public static final String DEFAULT_PYTHON_CACHE_PROFILER = "false";


    /**
     * Private constructor to avoid instantiation.
     */
    private COMPSsConstants() {
        throw new NonInstantiableException("COMPSsConstants");
    }

}
