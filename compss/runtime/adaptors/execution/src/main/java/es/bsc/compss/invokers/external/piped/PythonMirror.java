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
package es.bsc.compss.invokers.external.piped;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.executor.external.piped.ControlPipePair;
import es.bsc.compss.executor.external.piped.PipedMirror;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.net.InetAddress;
import java.net.UnknownHostException;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class PythonMirror extends PipedMirror {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_INVOKER);

    // Worker paths
    private static final String BINDINGS_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "bindings-common" + File.separator + "lib";
    public static final String PYCOMPSS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "python";
    private static final String WORKER_PY_RELATIVE_PATH = File.separator + "pycompss" + File.separator + "worker"
        + File.separator + "piper" + File.separator + "piper_worker.py";
    private static final String MPI_WORKER_PY_RELATIVE_PATH = File.separator + "pycompss" + File.separator + "worker"
        + File.separator + "piper" + File.separator + "mpi_piper_worker.py";

    // Environment variable names
    private static final String ENV_LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
    private static final String ENV_PYTHONPATH = "PYTHONPATH";

    // Attributes
    private final PythonParams pyParams;
    private final String pyCOMPSsHome;


    /**
     * Creates a new PythonMirror instance with the given context and size.
     * 
     * @param context Context.
     * @param size Mirror thread size.
     */
    public PythonMirror(InvocationContext context, int size) {
        super(context, size);
        this.pyParams = (PythonParams) context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        String installDir = context.getInstallDir();
        this.pyCOMPSsHome = installDir + PYCOMPSS_RELATIVE_PATH + File.separator + pyParams.getPythonVersion();
        init(context);
    }

    @Override
    public String getMirrorName() {
        return "python";
    }

    @Override
    public String getPipeBuilderContext() {
        StringBuilder cmd = new StringBuilder();

        // Binding
        cmd.append(COMPSsConstants.Lang.PYTHON).append(TOKEN_SEP);

        // Specific parameters
        cmd.append(this.pyParams.getPythonVirtualEnvironment()).append(TOKEN_SEP);
        cmd.append(this.pyParams.getPythonPropagateVirtualEnvironment()).append(TOKEN_SEP);
        cmd.append(this.pyParams.usePythonMpiWorker()).append(TOKEN_SEP);
        cmd.append(this.size + 1).append(TOKEN_SEP); // Number of MPI threads if using MPI worker.
        cmd.append(this.pyParams.getPythonInterpreter()).append(TOKEN_SEP);
        cmd.append(this.pyParams.getPythonExtraeFile()).append(TOKEN_SEP);

        return cmd.toString();
    }

    @Override
    public String getLaunchWorkerCommand(InvocationContext context, ControlPipePair pipe) {
        // Specific launch command is of the form: binding bindingExecutor bindingArgs
        // The bindingArgs are of the form python -u piper_worker.py debug tracing storageConf #threads <cmdPipes>
        // <resultPipes> controlPipeCMD controlPipeRESULT

        StringBuilder cmd = new StringBuilder();

        // Add specific MPI worker parameters (uses LATER_ variables defined in bindings_piper.sh)
        if (this.pyParams.usePythonMpiWorker()) {
            // Rank 0 acts as the Piper Worker. Other processes act as piped executors
            cmd.append("mpirun").append(TOKEN_SEP);
            cmd.append("-np").append(TOKEN_SEP).append(this.size + 1).append(TOKEN_SEP);
            cmd.append("-x").append(TOKEN_SEP).append("LD_PRELOAD=$LATER_MPI_LD_PRELOAD").append(TOKEN_SEP);
            cmd.append("-x").append(TOKEN_SEP).append("EXTRAE_CONFIG_FILE=$LATER_MPI_EXTRAE_CONFIG_FILE")
                .append(TOKEN_SEP);
            cmd.append("-x").append(TOKEN_SEP).append("EXTRAE_USE_POSIX_CLOCK=0").append(TOKEN_SEP);
            cmd.append("-x").append(TOKEN_SEP).append("PYTHONPATH=$LATER_MPI_PYTHONPATH:$PYTHONPATH").append(TOKEN_SEP);
        }

        // Add Python interpreter call
        cmd.append(this.pyParams.getPythonInterpreter()).append(TOKEN_SEP).append("-u").append(TOKEN_SEP);
        if (!LOGGER.isDebugEnabled()) {
            // If not logging, then use the optimized flag
            cmd.append("-O").append(TOKEN_SEP);
        }

        String profilePath = System.getenv("COMPSS_WORKER_PROFILE_PATH");
        if (profilePath != null) {
            // Add profiling and its output path
            cmd.append("-m").append(TOKEN_SEP).append("mprof").append(TOKEN_SEP).append("run").append(TOKEN_SEP);
            cmd.append("--multiprocess").append(TOKEN_SEP).append("--include-children").append(TOKEN_SEP);
            String profileFile = profilePath + "/";
            try {
                profileFile = profileFile + InetAddress.getLocalHost().getHostName() + ".dat";
            } catch (UnknownHostException e) {
                // TODO Auto-generated catch block
                e.printStackTrace();
            }
            cmd.append("--output").append(TOKEN_SEP).append(profileFile).append(TOKEN_SEP);
        }

        // Add PyCOMPSs worker main class
        if (this.pyParams.usePythonMpiWorker()) {
            cmd.append(this.pyCOMPSsHome).append(MPI_WORKER_PY_RELATIVE_PATH).append(TOKEN_SEP);
        } else {
            cmd.append(this.pyCOMPSsHome).append(WORKER_PY_RELATIVE_PATH).append(TOKEN_SEP);
        }

        // Add Python worker arguments
        cmd.append(context.getWorkingDir()).append(TOKEN_SEP);
        cmd.append(context.getLogDir()).append(TOKEN_SEP);
        cmd.append(context.getAnalysisDir()).append(TOKEN_SEP);
        cmd.append(context.getRuntimeAPI() != null).append(TOKEN_SEP);
        cmd.append(LOGGER.isDebugEnabled()).append(TOKEN_SEP);
        cmd.append(Tracer.isActivated()).append(TOKEN_SEP);
        cmd.append(context.getStorageConf()).append(TOKEN_SEP);
        cmd.append(context.getStreamingBackend().name()).append(TOKEN_SEP);
        cmd.append(context.getStreamingMasterName()).append(TOKEN_SEP);
        cmd.append(context.getStreamingMasterPort()).append(TOKEN_SEP);
        cmd.append(this.pyParams.getPythonWorkerCache()).append(TOKEN_SEP);
        cmd.append(this.pyParams.getPythonCacheProfiler()).append(TOKEN_SEP);

        // Ear integration
        cmd.append(context.getEar()).append(TOKEN_SEP);

        // Provenance integration
        cmd.append(context.getDataProvenance()).append(TOKEN_SEP);

        cmd.append(this.size).append(TOKEN_SEP);
        for (int i = 0; i < this.size; ++i) {
            cmd.append(i).append(TOKEN_SEP);
        }
        String executorPipes = this.basePipePath + "executor";
        for (int i = 0; i < this.size; ++i) {
            cmd.append(executorPipes).append(i).append(".outbound").append(TOKEN_SEP);
        }
        for (int i = 0; i < this.size; ++i) {
            cmd.append(executorPipes).append(i).append(".inbound").append(TOKEN_SEP);
        }
        cmd.append(pipe.getOutboundPipe()).append(TOKEN_SEP);
        cmd.append(pipe.getInboundPipe());

        return cmd.toString();
    }

    @Override
    public Map<String, String> getEnvironment(InvocationContext context) {
        // PyCOMPSs HOME
        Map<String, String> env = new HashMap<>();
        env.put("PYCOMPSS_HOME", this.pyCOMPSsHome);

        // PYTHONPATH
        String pythonPath = System.getenv(ENV_PYTHONPATH);
        if (pythonPath == null) {
            pythonPath = this.pyCOMPSsHome + ":" + this.pyParams.getPythonPath() + ":" + context.getAppDir();
        } else {
            pythonPath =
                this.pyCOMPSsHome + ":" + this.pyParams.getPythonPath() + ":" + context.getAppDir() + pythonPath;
        }
        env.put(ENV_PYTHONPATH, pythonPath);

        // LD_LIBRARY_PATH
        String ldLibraryPath = System.getenv(ENV_LD_LIBRARY_PATH);
        String bindingsHome = context.getInstallDir() + BINDINGS_RELATIVE_PATH;
        if (ldLibraryPath != null) {
            ldLibraryPath = ldLibraryPath.concat(":" + bindingsHome);
        } else {
            ldLibraryPath = bindingsHome;
        }
        env.put(ENV_LD_LIBRARY_PATH, ldLibraryPath);

        return env;
    }

    @Override
    protected String getPBWorkingDir(InvocationContext context) {
        String workingDir = super.getPBWorkingDir(context);
        if (Tracer.isActivated()) {
            workingDir += "python";
            File wdpath = new File(workingDir);
            if (!wdpath.exists()) {
                if (!wdpath.mkdirs()) {
                    ErrorManager.error("Could not create working dir for python tracefiles, path: " + workingDir);
                }
            } else {
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug(
                        "Working directorty for python trace files: " + workingDir + " already exists!.. Skipping!");
                }
            }
        }
        return workingDir;
    }
}
