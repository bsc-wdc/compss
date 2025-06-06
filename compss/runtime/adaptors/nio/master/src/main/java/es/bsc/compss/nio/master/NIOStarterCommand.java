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
package es.bsc.compss.nio.master;

import es.bsc.compss.comm.Comm;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTracer;
import es.bsc.compss.types.WorkerStarterCommand;

import java.io.File;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class NIOStarterCommand extends WorkerStarterCommand {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.COMM);

    // NIO Script path
    private static final String SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
        + File.separator + "adaptors" + File.separator + "nio" + File.separator;
    private static final String STARTER_SCRIPT_NAME = "persistent_worker.sh";

    // Script name
    private String scriptName;


    /**
     * Creates the WorkerStarterCommand for NIO.
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
    public NIOStarterCommand(String workerName, int workerPort, String masterName, String workingDir, String installDir,
        String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile,
        String envScriptPathFromFile, String pythonInterpreterFromFile, int totalCPU, int totalGPU, int totalFPGA,
        int limitOfTasks, String hostId) {

        super(workerName, workerPort, masterName, workingDir, installDir, appDir, classpathFromFile, pythonpathFromFile,
            libPathFromFile, envScriptPathFromFile, pythonInterpreterFromFile, totalCPU, totalGPU, totalFPGA,
            limitOfTasks, hostId);

        this.scriptName = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + SCRIPT_PATH
            + STARTER_SCRIPT_NAME;
    }

    @Override
    public String[] getStartCommand() throws Exception {

        /*
         * ************************************************************************************************************
         * BUILD COMMAND
         * ************************************************************************************************************
         */
        String[] cmd = new String[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + NIOAdaptor.NUM_PARAMS_NIO_WORKER
            + this.jvmFlags.length + 1 + this.fpgaArgs.length];

        /* SCRIPT ************************************************ */
        cmd[0] = this.scriptName;

        /* Values ONLY for persistent_worker.sh ****************** */
        cmd[1] = this.workerEnvScriptPath.isEmpty() ? "null" : this.workerEnvScriptPath;

        cmd[2] = this.workerLibPath.isEmpty() ? "null" : this.workerLibPath;

        if (appDir.isEmpty()) {
            LOGGER.warn("No path passed via appdir option neither xml AppDir field");
            cmd[3] = "null";
        } else {
            cmd[3] = appDir;
        }

        cmd[4] = this.workerClasspath.isEmpty() ? "null" : this.workerClasspath;

        cmd[5] = Comm.getStreamingBackend().name();

        cmd[6] = String.valueOf(this.jvmFlags.length);
        for (int i = 0; i < this.jvmFlags.length; ++i) {
            cmd[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + i] = this.jvmFlags[i];
        }

        int nextPosition = NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + this.jvmFlags.length;
        cmd[nextPosition++] = String.valueOf(this.fpgaArgs.length);
        for (String fpgaArg : this.fpgaArgs) {
            cmd[nextPosition++] = fpgaArg;
        }

        /* Values for NIOWorker ********************************** */
        cmd[nextPosition++] = this.workerDebug;

        // Internal parameters
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER);
        cmd[nextPosition++] = this.workerName;
        cmd[nextPosition++] = String.valueOf(this.workerPort);
        cmd[nextPosition++] = this.masterName;
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MASTER_PORT);
        cmd[nextPosition++] = String.valueOf(Comm.getStreamingPort());

        // Worker parameters
        cmd[nextPosition++] = String.valueOf(this.totalCPU);
        cmd[nextPosition++] = String.valueOf(this.totalGPU);
        cmd[nextPosition++] = String.valueOf(this.totalFPGA);

        // affinity
        cmd[nextPosition++] = String.valueOf(CPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(GPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(FPGA_AFFINITY);
        cmd[nextPosition++] = String.valueOf(IO_EXECUTORS);
        cmd[nextPosition++] = String.valueOf(this.limitOfTasks);

        // Application parameters
        cmd[nextPosition++] = DEPLOYMENT_ID;
        cmd[nextPosition++] = this.lang;
        cmd[nextPosition++] = this.sandboxedWorkingDir;
        cmd[nextPosition++] = this.installDir;

        cmd[nextPosition++] = cmd[3];
        cmd[nextPosition++] = this.workerLibPath.isEmpty() ? "null" : this.workerLibPath;
        cmd[nextPosition++] = this.workerClasspath.isEmpty() ? "null" : this.workerClasspath;
        cmd[nextPosition++] = this.workerPythonpath.isEmpty() ? "null" : this.workerPythonpath;

        // Tracing parameters
        cmd[nextPosition++] = String.valueOf(NIOTracer.isActivated());
        cmd[nextPosition++] = NIOTracer.getExtraeFile();
        cmd[nextPosition++] = this.hostId;
        cmd[nextPosition++] = String.valueOf(NIOTracer.isTracingTaskDependencies());

        // Storage parameters
        cmd[nextPosition++] = this.storageConf;
        cmd[nextPosition++] = this.executionType;

        // persistent_c parameter
        cmd[nextPosition++] = this.workerPersistentC;

        // Python interpreter parameter
        cmd[nextPosition++] = this.pythonInterpreter;
        // Python interpreter version
        cmd[nextPosition++] = this.pythonVersion;
        // Python virtual environment parameter
        cmd[nextPosition++] = this.pythonVirtualEnvironment;
        // Python propagate virtual environment parameter
        cmd[nextPosition++] = this.pythonPropagateVirtualEnvironment;
        // Python extrae config file
        cmd[nextPosition++] = this.pythonExtraeFile;
        // Python use MPI worker parameter
        cmd[nextPosition++] = this.pythonMpiWorker;
        // Python use worker cache
        cmd[nextPosition++] = this.pythonWorkerCache;
        // Python use cache profiler
        cmd[nextPosition++] = this.pythonCacheProfiler;

        // Ear
        cmd[nextPosition++] = this.ear;

        // Provenance
        cmd[nextPosition++] = this.dataProvenance;

        if (cmd.length != nextPosition) {
            throw new Exception(
                "ERROR: Incorrect number of parameters. Expected: " + cmd.length + ". Got: " + nextPosition);
        }

        return cmd;
    }

    @Override
    public void setScriptName(String scriptName) {
        this.scriptName = scriptName;
    }

}
