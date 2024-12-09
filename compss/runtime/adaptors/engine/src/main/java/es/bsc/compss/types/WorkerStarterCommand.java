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
package es.bsc.compss.types;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsDefaults;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.ThreadBinder;
import es.bsc.conn.types.StarterCommand;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class WorkerStarterCommand implements StarterCommand {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    // Static Environment variables
    private static final String LIB_SEPARATOR = ":";
    private static final String CLASSPATH_FROM_ENVIRONMENT = (System.getProperty(COMPSsConstants.WORKER_CP) != null
        && !System.getProperty(COMPSsConstants.WORKER_CP).isEmpty()) ? System.getProperty(COMPSsConstants.WORKER_CP)
            : "";

    private static final String PYTHONPATH_FROM_ENVIRONMENT = (System.getProperty(COMPSsConstants.WORKER_PP) != null
        && !System.getProperty(COMPSsConstants.WORKER_PP).isEmpty()) ? System.getProperty(COMPSsConstants.WORKER_PP)
            : "";

    private static final String LIBPATH_FROM_ENVIRONMENT = (System.getenv(COMPSsConstants.LD_LIBRARY_PATH) != null
        && !System.getenv(COMPSsConstants.LD_LIBRARY_PATH).isEmpty()) ? System.getenv(COMPSsConstants.LD_LIBRARY_PATH)
            : "";

    private static final boolean IS_CPU_AFFINITY_DEFINED =
        System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY).isEmpty();
    protected static final String CPU_AFFINITY =
        IS_CPU_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_CPU_AFFINITY)
            : ThreadBinder.BINDER_DISABLED;

    private static final boolean IS_GPU_AFFINITY_DEFINED =
        System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY).isEmpty();
    protected static final String GPU_AFFINITY =
        IS_GPU_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_GPU_AFFINITY)
            : ThreadBinder.BINDER_DISABLED;

    private static final boolean IS_FPGA_AFFINITY_DEFINED =
        System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY) != null
            && !System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY).isEmpty();
    protected static final String FPGA_AFFINITY =
        IS_FPGA_AFFINITY_DEFINED ? System.getProperty(COMPSsConstants.WORKER_FPGA_AFFINITY)
            : ThreadBinder.BINDER_DISABLED;

    private static final boolean IS_IO_EXECUTORS_DEFINED =
        System.getProperty(COMPSsConstants.WORKER_IO_EXECUTORS) != null
            && !System.getProperty(COMPSsConstants.WORKER_IO_EXECUTORS).isEmpty();
    protected static final String IO_EXECUTORS =
        IS_IO_EXECUTORS_DEFINED ? System.getProperty(COMPSsConstants.WORKER_IO_EXECUTORS) : "0";

    private static final String WORKER_APPDIR_FROM_ENVIRONMENT =
        System.getProperty(COMPSsConstants.WORKER_APPDIR) != null
            && !System.getProperty(COMPSsConstants.WORKER_APPDIR).isEmpty()
                ? System.getProperty(COMPSsConstants.WORKER_APPDIR)
                : "";
    private static final String WORKER_ENV_SCRIPT_FROM_ENVIRONMENT =
        System.getProperty(COMPSsConstants.WORKER_ENV_SCRIPT) != null
            && !System.getProperty(COMPSsConstants.WORKER_ENV_SCRIPT).isEmpty()
                ? System.getProperty(COMPSsConstants.WORKER_ENV_SCRIPT)
                : "";
    private static final String WORKER_PYTHON_INTERPRETER_FROM_ENVIRONMENT =
        System.getProperty(COMPSsConstants.PYTHON_INTERPRETER) != null
            && !System.getProperty(COMPSsConstants.PYTHON_INTERPRETER).isEmpty()
            && !System.getProperty(COMPSsConstants.PYTHON_INTERPRETER).equals("null")
                ? System.getProperty(COMPSsConstants.PYTHON_INTERPRETER)
                : "";

    // Deployment ID
    protected static final String DEPLOYMENT_ID = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);

    protected String workerName;
    protected int workerPort;
    protected String masterName;
    protected String workingDir;
    protected String sandboxedWorkingDir;
    protected String installDir;
    protected String appDir = "";
    protected String workerClasspath = "";
    protected String workerPythonpath = "";
    protected String workerLibPath = "";
    protected String workerEnvScriptPath = "";
    protected String[] jvmFlags;
    protected String[] fpgaArgs;
    protected String workerDebug;
    protected String storageConf;
    protected String executionType;
    protected String workerPersistentC;
    protected String pythonInterpreter;
    protected String pythonVersion;
    protected String pythonVirtualEnvironment;
    protected String pythonPropagateVirtualEnvironment;
    protected String pythonExtraeFile;
    protected String pythonMpiWorker;
    protected String pythonWorkerCache;
    protected String pythonCacheProfiler;
    protected String ear;
    protected String dataProvenance;
    protected int totalCPU;
    protected int totalGPU;
    protected int totalFPGA;
    protected int limitOfTasks;
    protected String hostId;
    protected String lang;


    /**
     * Creates the WorkerStarterCommand.
     *
     * @param workerName worker name
     * @param workerPort worker Port number
     * @param masterName master name
     * @param workingDir worker working directory
     * @param installDir worker COMPSs install directory
     * @param appDir worker application install directory
     * @param classpathFromFile worker classpath in projects.xml file
     * @param pythonpathFromFile worker python path in projects.xml file
     * @param libPathFromFile worker library path in project.xml file
     * @param envScriptPathFromFile worker environment script path in project.xml file
     * @param pythonInterpreterFromFile worker python interpreter in project.xml file
     * @param totalCPU total CPU computing units
     * @param totalGPU total GPU
     * @param totalFPGA total FPGA
     * @param limitOfTasks limit of tasks
     * @param hostId tracing worker identifier
     */
    public WorkerStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptPathFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {

        this.workerName = workerName;
        this.workerPort = workerPort;
        this.masterName = masterName;
        this.workingDir = workingDir;
        this.sandboxedWorkingDir = workingDir;
        this.installDir = installDir;

        if (!appDir.isEmpty()) {
            if (!WORKER_APPDIR_FROM_ENVIRONMENT.isEmpty()) {
                LOGGER.warn("Path passed via appdir option and xml AppDir field."
                    + "The path provided by the xml will be used");
            }
            this.appDir = appDir;

        } else {
            if (!WORKER_APPDIR_FROM_ENVIRONMENT.isEmpty()) {
                this.appDir = WORKER_APPDIR_FROM_ENVIRONMENT;
            }
        }
        // Configure environment script
        if (!envScriptPathFromFile.isEmpty()) {
            if (!WORKER_ENV_SCRIPT_FROM_ENVIRONMENT.isEmpty()) {
                LOGGER.warn("Path passed via env_script option and xml EnvironmentPath field."
                    + "The path provided by the xml will be used");
            }
            this.workerEnvScriptPath = envScriptPathFromFile;

        } else {
            if (!WORKER_ENV_SCRIPT_FROM_ENVIRONMENT.isEmpty()) {
                this.workerEnvScriptPath = WORKER_ENV_SCRIPT_FROM_ENVIRONMENT;
            }
        }

        // Configure python interpreter
        if (!pythonInterpreterFromFile.isEmpty()) {
            if (!WORKER_PYTHON_INTERPRETER_FROM_ENVIRONMENT.isEmpty()) {
                LOGGER.warn("Path passed via python_interpreter option and xml PythonInterpreter field."
                    + "The interpreter provided by the xml will be used");
            }
            this.pythonInterpreter = pythonInterpreterFromFile;

        } else {
            if (!WORKER_PYTHON_INTERPRETER_FROM_ENVIRONMENT.isEmpty()) {
                this.pythonInterpreter = WORKER_PYTHON_INTERPRETER_FROM_ENVIRONMENT;
            } else {
                this.pythonInterpreter = COMPSsDefaults.PYTHON_INTERPRETER;
            }
        }

        // Merge command classpath and worker defined classpath
        if (!classpathFromFile.isEmpty()) {
            if (!CLASSPATH_FROM_ENVIRONMENT.isEmpty()) {
                this.workerClasspath = classpathFromFile + LIB_SEPARATOR + CLASSPATH_FROM_ENVIRONMENT;
            } else {
                this.workerClasspath = classpathFromFile;
            }
        } else {
            this.workerClasspath = CLASSPATH_FROM_ENVIRONMENT;
        }
        if (!pythonpathFromFile.isEmpty()) {
            if (!PYTHONPATH_FROM_ENVIRONMENT.isEmpty()) {
                this.workerPythonpath = pythonpathFromFile + LIB_SEPARATOR + PYTHONPATH_FROM_ENVIRONMENT;
            } else {
                this.workerPythonpath = pythonpathFromFile;
            }
        } else {
            this.workerPythonpath = PYTHONPATH_FROM_ENVIRONMENT;
        }

        if (!libPathFromFile.isEmpty()) {
            if (!LIBPATH_FROM_ENVIRONMENT.isEmpty()) {
                this.workerLibPath = libPathFromFile + LIB_SEPARATOR + LIBPATH_FROM_ENVIRONMENT;
            } else {
                this.workerLibPath = libPathFromFile;
            }
        } else {
            this.workerLibPath = LIBPATH_FROM_ENVIRONMENT;
        }

        // Get JVM Flags
        String workerJVMflags = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
        this.jvmFlags = new String[0];
        if (workerJVMflags != null && !workerJVMflags.isEmpty()) {
            this.jvmFlags = workerJVMflags.split(",");
        }

        // Get FPGA reprogram args
        String workerFPGAargs = System.getProperty(COMPSsConstants.WORKER_FPGA_REPROGRAM);
        this.fpgaArgs = new String[0];
        if (workerFPGAargs != null && !workerFPGAargs.isEmpty()) {
            this.fpgaArgs = workerFPGAargs.split(" ");
        }

        // Configure worker debug level
        this.workerDebug = Boolean.toString(LogManager.getLogger(Loggers.WORKER).isDebugEnabled());

        // Configure storage
        this.storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (this.storageConf == null || this.storageConf.equals("") || this.storageConf.equals("null")) {
            this.storageConf = "null";
        }
        this.executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (this.executionType == null || this.executionType.equals("") || this.executionType.equals("null")) {
            this.executionType = COMPSsConstants.TaskExecution.COMPSS.toString();
        }

        // configure persistent_worker_c execution
        this.workerPersistentC = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (this.workerPersistentC == null || this.workerPersistentC.isEmpty()
            || this.workerPersistentC.equals("null")) {
            this.workerPersistentC = COMPSsDefaults.PERSISTENT_C;
        }

        // Configure python version
        this.pythonVersion = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (this.pythonVersion == null || this.pythonVersion.isEmpty() || this.pythonVersion.equals("null")) {
            this.pythonVersion = COMPSsDefaults.PYTHON_VERSION;
        }

        // Configure python virtual environment
        this.pythonVirtualEnvironment = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (this.pythonVirtualEnvironment == null || this.pythonVirtualEnvironment.isEmpty()
            || this.pythonVirtualEnvironment.equals("null")) {
            this.pythonVirtualEnvironment = COMPSsDefaults.PYTHON_VIRTUAL_ENVIRONMENT;
        }
        this.pythonPropagateVirtualEnvironment =
            System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (this.pythonPropagateVirtualEnvironment == null || this.pythonPropagateVirtualEnvironment.isEmpty()
            || this.pythonPropagateVirtualEnvironment.equals("null")) {
            this.pythonPropagateVirtualEnvironment = COMPSsDefaults.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
        }

        // Configure python extrae config file
        this.pythonExtraeFile = System.getProperty(COMPSsConstants.PYTHON_EXTRAE_CONFIG_FILE);
        if (this.pythonExtraeFile == null || this.pythonExtraeFile.isEmpty() || this.pythonExtraeFile.equals("null")) {
            this.pythonExtraeFile = COMPSsDefaults.PYTHON_CUSTOM_EXTRAE_FILE;
        }

        // Configure mpi worker
        this.pythonMpiWorker = System.getProperty(COMPSsConstants.PYTHON_MPI_WORKER);
        if (this.pythonMpiWorker == null || this.pythonMpiWorker.isEmpty() || this.pythonMpiWorker.equals("null")) {
            this.pythonMpiWorker = COMPSsDefaults.PYTHON_MPI_WORKER;
        }

        // Configure worker cache
        this.pythonWorkerCache = System.getProperty(COMPSsConstants.PYTHON_WORKER_CACHE);
        if (this.pythonWorkerCache == null || this.pythonWorkerCache.isEmpty()
            || this.pythonWorkerCache.equals("null")) {
            this.pythonWorkerCache = COMPSsDefaults.PYTHON_WORKER_CACHE;
        }

        // Configure profiler cache
        this.pythonCacheProfiler = System.getProperty(COMPSsConstants.PYTHON_CACHE_PROFILER);
        if (this.pythonCacheProfiler == null || this.pythonCacheProfiler.isEmpty()
            || this.pythonCacheProfiler.equals("null")) {
            this.pythonCacheProfiler = COMPSsDefaults.PYTHON_CACHE_PROFILER;
        }

        // Configure EAR
        this.ear = System.getProperty(COMPSsConstants.EAR);
        if (this.ear == null || this.ear.isEmpty() || this.ear.equals("null")) {
            this.ear = COMPSsDefaults.EAR;
        }

        // Configure provenance
        this.dataProvenance = System.getProperty(COMPSsConstants.DATA_PROVENANCE);
        if (this.dataProvenance == null || this.dataProvenance.isEmpty() || this.dataProvenance.equals("null")) {
            this.dataProvenance = COMPSsDefaults.DP_ENABLED;
        }

        this.lang = System.getProperty(COMPSsConstants.LANG);

        this.totalCPU = totalCPU;
        this.totalGPU = totalGPU;
        this.totalFPGA = totalFPGA;
        this.limitOfTasks = limitOfTasks;
        this.hostId = hostId;
    }

    @Override
    public String getBaseWorkingDir() {
        return this.workingDir;
    }

    @Override
    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    @Override
    public void setNodeId(String nodeId) {
        this.hostId = nodeId;
    }

    @Override
    public void setSandboxedWorkingDir(String sandboxedWorkingDir) {
        this.sandboxedWorkingDir = sandboxedWorkingDir;
    }

}
