/*         
 *  Copyright 2002-2018 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.executor.ExecutorContext;
import es.bsc.compss.executor.utils.PipedMirror;
import es.bsc.compss.executor.utils.PipePair;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.external.commands.ExecuteTaskExternalCommand;
import es.bsc.compss.invokers.external.piped.commands.ExecuteTaskPipeCommand;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.invokers.types.PythonParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.Tracer;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


public class PythonInvoker extends PipedInvoker {

    public PythonInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir,
            InvocationResources assignedResources, PipePair pipes) throws JobExecutionException {

        super(context, invocation, taskSandboxWorkingDir, assignedResources, pipes);
    }

    @Override
    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation, String sandBox,
            InvocationResources assignedResources) {

        ExecuteTaskPipeCommand taskExecution = new ExecuteTaskPipeCommand(invocation.getJobId());
        return taskExecution;
    }

    public static PipedMirror getMirror(InvocationContext context, ExecutorContext platform) {
        int threads = platform.getSize();
        return new PythonMirror(context, threads);
    }


    private static class PythonMirror extends PipedMirror {

        protected static final String BINDINGS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "bindings-common"
                + File.separator + "lib";
        public static final String PYCOMPSS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "python";
        private static final String WORKER_PY_RELATIVE_PATH = File.separator + "pycompss" + File.separator + "worker" + File.separator
                + "piper_worker.py";

        private static final String ENV_LD_LIBRARY_PATH = "LD_LIBRARY_PATH";
        private static final String ENV_PYTHONPATH = "PYTHONPATH";

        private final PythonParams pyParams;
        private final String pyCOMPSsHome;


        public PythonMirror(InvocationContext context, int size) {
            super(context, size);
            this.pyParams = (PythonParams) context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
            String installDir = context.getInstallDir();
            this.pyCOMPSsHome = installDir + PYCOMPSS_RELATIVE_PATH + File.separator + pyParams.getPythonVersion();
            init(context);
        }

        @Override
        public String getLaunchCommand(InvocationContext context) {
            // Specific launch command is of the form: binding bindingExecutor bindingArgs
            // The bindingArgs are of the form python -u piper_worker.py debug tracing storageConf #threads cmdPipes
            // resultPipes
            StringBuilder cmd = new StringBuilder();

            cmd.append(COMPSsConstants.Lang.PYTHON).append(TOKEN_SEP);
            cmd.append(this.pyParams.getPythonVirtualEnvironment()).append(TOKEN_SEP);
            cmd.append(this.pyParams.getPythonPropagateVirtualEnvironment()).append(TOKEN_SEP);

            cmd.append(Tracer.isActivated()).append(TOKEN_SEP);

            cmd.append(this.pyParams.getPythonInterpreter()).append(TOKEN_SEP).append("-u").append(TOKEN_SEP);
            cmd.append(this.pyCOMPSsHome).append(WORKER_PY_RELATIVE_PATH).append(TOKEN_SEP);

            cmd.append(LOGGER.isDebugEnabled()).append(TOKEN_SEP);
            cmd.append(Tracer.isActivated()).append(TOKEN_SEP);
            cmd.append(context.getStorageConf()).append(TOKEN_SEP);
            cmd.append(this.size).append(TOKEN_SEP);
            String computePipes = this.basePipePath + "compute";

            for (int i = 0; i < this.size; ++i) {
                cmd.append(computePipes).append(i).append(".outbound").append(TOKEN_SEP);
            }

            for (int i = 0; i < this.size; ++i) {
                cmd.append(computePipes).append(i).append(".inbound").append(TOKEN_SEP);
            }

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
                pythonPath = this.pyCOMPSsHome + ":" + this.pyParams.getPythonPath() + ":" + context.getAppDir() + pythonPath;
            }

            env.put(ENV_PYTHONPATH, pythonPath);

            // LD_LIBRARY_PATH
            String ldLibraryPath = System.getenv(ENV_LD_LIBRARY_PATH);
            CParams cParams = (CParams) context.getLanguageParams(Lang.C);
            if (ldLibraryPath == null) {
                ldLibraryPath = cParams.getLibraryPath();
            } else {
                ldLibraryPath = ldLibraryPath.concat(":" + cParams.getLibraryPath());
            }
            String bindingsHome = context.getInstallDir() + BINDINGS_RELATIVE_PATH;
            ldLibraryPath = ldLibraryPath.concat(":" + bindingsHome);
            env.put(ENV_LD_LIBRARY_PATH, ldLibraryPath);

            return env;
        }

        @Override
        protected String getPBWorkingDir(InvocationContext context) {
            String workingDir = super.getPBWorkingDir(context);
            if (Tracer.isActivated()) {
                workingDir += "python";
                if (!new File(workingDir).mkdirs()) {
                    ErrorManager.error("Could not create working dir for python tracefiles, path: " + workingDir);
                }
            }
            return workingDir;
        }
    }
}
