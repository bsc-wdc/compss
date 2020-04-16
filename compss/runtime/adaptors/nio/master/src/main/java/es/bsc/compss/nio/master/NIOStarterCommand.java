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

    private static final String SCRIPT_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
        + File.separator + "adaptors" + File.separator + "nio" + File.separator;

    private static final String STARTER_SCRIPT_NAME = "persistent_worker.sh";

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
     * @param libPathFromFile worker library path path in project.xml file
     * @param totalCPU total CPU computing units
     * @param totalGPU total GPU
     * @param totalFPGA total FPGA
     * @param limitOfTasks limit of tasks
     * @param hostId tracing worker identifier
     */
    public NIOStarterCommand(String workerName, int workerPort, String masterName, String workingDir, String installDir,
        String appDir, String classpathFromFile, String pythonpathFromFile, String libPathFromFile, int totalCPU,
        int totalGPU, int totalFPGA, int limitOfTasks, String hostId) {

        super(workerName, workerPort, masterName, workingDir, installDir, appDir, classpathFromFile, pythonpathFromFile,
            libPathFromFile, totalCPU, totalGPU, totalFPGA, limitOfTasks, hostId);
        scriptName = installDir + (installDir.endsWith(File.separator) ? "" : File.separator) + SCRIPT_PATH
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
            + jvmFlags.length + 1 + fpgaArgs.length];

        /* SCRIPT ************************************************ */
        cmd[0] = scriptName;

        /* Values ONLY for persistent_worker.sh ****************** */
        cmd[1] = workerLibPath.isEmpty() ? "null" : workerLibPath;

        if (appDir.isEmpty()) {
            LOGGER.warn("No path passed via appdir option neither xml AppDir field");
            cmd[2] = "null";
        } else {
            cmd[2] = appDir;
        }

        cmd[3] = workerClasspath.isEmpty() ? "null" : workerClasspath;

        cmd[4] = Comm.getStreamingBackend().name();

        cmd[5] = String.valueOf(jvmFlags.length);
        for (int i = 0; i < jvmFlags.length; ++i) {
            cmd[NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + i] = jvmFlags[i];
        }

        int nextPosition = NIOAdaptor.NUM_PARAMS_PER_WORKER_SH + jvmFlags.length;
        cmd[nextPosition++] = String.valueOf(fpgaArgs.length);
        for (String fpgaArg : fpgaArgs) {
            cmd[nextPosition++] = fpgaArg;
        }

        /* Values for NIOWorker ********************************** */
        cmd[nextPosition++] = workerDebug;

        // Internal parameters
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_SEND_WORKER);
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MAX_RECEIVE_WORKER);
        cmd[nextPosition++] = workerName;
        cmd[nextPosition++] = String.valueOf(workerPort);
        cmd[nextPosition++] = masterName;
        cmd[nextPosition++] = String.valueOf(NIOAdaptor.MASTER_PORT);
        cmd[nextPosition++] = String.valueOf(Comm.getStreamingPort());

        // Worker parameters
        cmd[nextPosition++] = String.valueOf(totalCPU);
        cmd[nextPosition++] = String.valueOf(totalGPU);
        cmd[nextPosition++] = String.valueOf(totalFPGA);

        // affinity
        cmd[nextPosition++] = String.valueOf(CPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(GPU_AFFINITY);
        cmd[nextPosition++] = String.valueOf(FPGA_AFFINITY);
        cmd[nextPosition++] = String.valueOf(IO_EXECUTORS);
        cmd[nextPosition++] = String.valueOf(limitOfTasks);

        // Application parameters
        cmd[nextPosition++] = DEPLOYMENT_ID;
        cmd[nextPosition++] = lang;
        cmd[nextPosition++] = workingDir;
        cmd[nextPosition++] = installDir;

        cmd[nextPosition++] = cmd[2];
        cmd[nextPosition++] = workerLibPath.isEmpty() ? "null" : workerLibPath;
        cmd[nextPosition++] = workerClasspath.isEmpty() ? "null" : workerClasspath;
        cmd[nextPosition++] = workerPythonpath.isEmpty() ? "null" : workerPythonpath;

        // Tracing parameters
        cmd[nextPosition++] = String.valueOf(NIOTracer.getLevel());
        cmd[nextPosition++] = NIOTracer.getExtraeFile();
        cmd[nextPosition++] = hostId;

        // Storage parameters
        cmd[nextPosition++] = storageConf;
        cmd[nextPosition++] = executionType;

        // persistent_c parameter
        cmd[nextPosition++] = workerPersistentC;

        // Python interpreter parameter
        cmd[nextPosition++] = pythonInterpreter;
        // Python interpreter version
        cmd[nextPosition++] = pythonVersion;
        // Python virtual environment parameter
        cmd[nextPosition++] = pythonVirtualEnvironment;
        // Python propagate virtual environment parameter
        cmd[nextPosition++] = pythonPropagateVirtualEnvironment;
        // Python use MPI worker parameter
        cmd[nextPosition++] = pythonMpiWorker;

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
