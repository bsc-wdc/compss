/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

    // Deployment ID
    protected static final String DEPLOYMENT_ID = System.getProperty(COMPSsConstants.DEPLOYMENT_ID);

    protected String workerName;
    protected int workerPort;
    protected String masterName;
    protected String workingDir;
    protected String installDir;
    protected String appDir = "";
    protected String workerClasspath = "";
    protected String workerPythonpath = "";
    protected String workerLibPath = "";
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
    protected String pythonMpiWorker;
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
     * @param libPathFromFile worker library path path in project.xml file
     * @param totalCPU total CPU computing units
     * @param totalGPU total GPU
     * @param totalFPGA total FPGA
     * @param limitOfTasks limit of tasks
     * @param hostId tracing worker identifier
     */
    public WorkerStarterCommand(String workerName, int workerPort, String masterName, String workingDir,
        String installDir, String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        int totalCPU, int totalGPU, int totalFPGA, int limitOfTasks, String hostId) {

        this.workerName = workerName;
        this.workerPort = workerPort;
        this.masterName = masterName;
        this.workingDir = workingDir;
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

        // Merge command classpath and worker defined classpath
        if (!classpathFromFile.isEmpty()) {
            if (!CLASSPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerClasspath = classpathFromFile + LIB_SEPARATOR + CLASSPATH_FROM_ENVIRONMENT;
            } else {
                workerClasspath = classpathFromFile;
            }
        } else {
            workerClasspath = CLASSPATH_FROM_ENVIRONMENT;
        }
        if (!pythonpathFromFile.isEmpty()) {
            if (!PYTHONPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerPythonpath = pythonpathFromFile + LIB_SEPARATOR + PYTHONPATH_FROM_ENVIRONMENT;
            } else {
                workerPythonpath = pythonpathFromFile;
            }
        } else {
            workerPythonpath = PYTHONPATH_FROM_ENVIRONMENT;
        }

        if (!libPathFromFile.isEmpty()) {
            if (!LIBPATH_FROM_ENVIRONMENT.isEmpty()) {
                workerLibPath = libPathFromFile + LIB_SEPARATOR + LIBPATH_FROM_ENVIRONMENT;
            } else {
                workerLibPath = libPathFromFile;
            }
        } else {
            workerLibPath = LIBPATH_FROM_ENVIRONMENT;
        }

        // Get JVM Flags
        String workerJVMflags = System.getProperty(COMPSsConstants.WORKER_JVM_OPTS);
        jvmFlags = new String[0];
        if (workerJVMflags != null && !workerJVMflags.isEmpty()) {
            jvmFlags = workerJVMflags.split(",");
        }

        // Get FPGA reprogram args
        String workerFPGAargs = System.getProperty(COMPSsConstants.WORKER_FPGA_REPROGRAM);
        fpgaArgs = new String[0];
        if (workerFPGAargs != null && !workerFPGAargs.isEmpty()) {
            fpgaArgs = workerFPGAargs.split(" ");
        }

        // Configure worker debug level
        workerDebug = Boolean.toString(LogManager.getLogger(Loggers.WORKER).isDebugEnabled());

        // Configure storage
        storageConf = System.getProperty(COMPSsConstants.STORAGE_CONF);
        if (storageConf == null || storageConf.equals("") || storageConf.equals("null")) {
            storageConf = "null";
        }
        executionType = System.getProperty(COMPSsConstants.TASK_EXECUTION);
        if (executionType == null || executionType.equals("") || executionType.equals("null")) {
            executionType = COMPSsConstants.TaskExecution.COMPSS.toString();
        }

        // configure persistent_worker_c execution
        workerPersistentC = System.getProperty(COMPSsConstants.WORKER_PERSISTENT_C);
        if (workerPersistentC == null || workerPersistentC.isEmpty() || workerPersistentC.equals("null")) {
            workerPersistentC = COMPSsConstants.DEFAULT_PERSISTENT_C;
        }

        // Configure python interpreter
        pythonInterpreter = System.getProperty(COMPSsConstants.PYTHON_INTERPRETER);
        if (pythonInterpreter == null || pythonInterpreter.isEmpty() || pythonInterpreter.equals("null")) {
            pythonInterpreter = COMPSsConstants.DEFAULT_PYTHON_INTERPRETER;
        }

        // Configure python version
        pythonVersion = System.getProperty(COMPSsConstants.PYTHON_VERSION);
        if (pythonVersion == null || pythonVersion.isEmpty() || pythonVersion.equals("null")) {
            pythonVersion = COMPSsConstants.DEFAULT_PYTHON_VERSION;
        }

        // Configure python virtual environment
        pythonVirtualEnvironment = System.getProperty(COMPSsConstants.PYTHON_VIRTUAL_ENVIRONMENT);
        if (pythonVirtualEnvironment == null || pythonVirtualEnvironment.isEmpty()
            || pythonVirtualEnvironment.equals("null")) {
            pythonVirtualEnvironment = COMPSsConstants.DEFAULT_PYTHON_VIRTUAL_ENVIRONMENT;
        }
        pythonPropagateVirtualEnvironment = System.getProperty(COMPSsConstants.PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT);
        if (pythonPropagateVirtualEnvironment == null || pythonPropagateVirtualEnvironment.isEmpty()
            || pythonPropagateVirtualEnvironment.equals("null")) {
            pythonPropagateVirtualEnvironment = COMPSsConstants.DEFAULT_PYTHON_PROPAGATE_VIRTUAL_ENVIRONMENT;
        }

        pythonMpiWorker = System.getProperty(COMPSsConstants.PYTHON_MPI_WORKER);
        if (pythonMpiWorker == null || pythonMpiWorker.isEmpty() || pythonMpiWorker.equals("null")) {
            pythonMpiWorker = COMPSsConstants.DEFAULT_PYTHON_MPI_WORKER;
        }
        this.lang = System.getProperty(COMPSsConstants.LANG);

        this.totalCPU = totalCPU;
        this.totalGPU = totalGPU;
        this.totalFPGA = totalFPGA;
        this.limitOfTasks = limitOfTasks;
        this.hostId = hostId;
    }

    /**
     * Generate the command to start the worker.
     * 
     * @return Command as string array
     * @throws Exception Error when generating the starter command
     */
    public abstract String[] getStartCommand() throws Exception;

    public abstract void setScriptName(String scriptName);

    @Override
    public void setWorkerName(String workerName) {
        this.workerName = workerName;
    }

    @Override
    public void setNodeId(String nodeId) {
        this.hostId = nodeId;

    }

}
