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
import es.bsc.compss.executor.Executor.ExecutorsContext;
import es.bsc.compss.executor.utils.ExecutionPlatformMirror;
import es.bsc.compss.executor.utils.PipedMirror;
import es.bsc.compss.executor.utils.PipedMirror.PipePair;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.external.ExternalCommand.ExecuteTaskExternalCommand;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import java.io.File;
import java.util.HashMap;
import java.util.Map;


/**
 *
 * @author flordan
 */
public class CInvoker extends PipedInvoker {

    public CInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources, PipePair pipes)
            throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources, pipes);
    }

    @Override
    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation, String sandBox, InvocationResources assignedResources) {
        throw new UnsupportedOperationException("Not supported yet."); //To change body of generated methods, choose Tools | Templates.
    }

    public static ExecutionPlatformMirror getMirror(InvocationContext context, ExecutorsContext platform) {
        int numThreads = platform.getSize();
        return new CMirror(context, numThreads);
    }


    private static class CMirror extends PipedMirror {
        // C worker relative path

        private static final String C_PIPER = "c_piper.sh";
        private static final String LIBRARY_PATH_ENV = "LD_LIBRARY_PATH";
        private static final String C_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "c" + File.separator + "lib";
        private static final String WORKER_C_RELATIVE_PATH = File.separator + "worker" + File.separator + "worker_c";
        protected static final String BINDINGS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "bindings-common" + File.separator
                + "lib";

        public CMirror(InvocationContext context, int size) {
            super(context, size);
        }

        @Override
        public String getLaunchCommand(InvocationContext context) {
            // Specific launch command is of the form: binding bindingExecutor bindingArgs
            StringBuilder cmd = new StringBuilder();

            String installDir = context.getInstallDir();

            cmd.append(COMPSsConstants.Lang.C).append(TOKEN_SEP);

            cmd.append(installDir).append(PIPER_SCRIPT_RELATIVE_PATH).append(C_PIPER).append(TOKEN_SEP);

            String computePipes = basePipePath + "compute";
            cmd.append(size).append(TOKEN_SEP);
            for (int i = 0; i < size; ++i) {
                cmd.append(computePipes).append(i).append(".outbound").append(TOKEN_SEP);
            }

            cmd.append(size).append(TOKEN_SEP);
            for (int i = 0; i < size; ++i) {
                cmd.append(computePipes).append(i).append(".inbound").append(TOKEN_SEP);
            }

            return cmd.toString();
        }

        @Override
        public Map<String, String> getEnvironment(InvocationContext context) {
            Map<String, String> env = new HashMap<>();
            String ldLibraryPath = System.getenv(LIBRARY_PATH_ENV);
            if (ldLibraryPath == null) {
                ldLibraryPath = context.getLibPath();
            } else {
                ldLibraryPath = ldLibraryPath.concat(":" + context.getLibPath());
            }

            // Add C and commons libs
            ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + C_LIB_RELATIVE_PATH);
            ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + BINDINGS_RELATIVE_PATH);

            env.put(LIBRARY_PATH_ENV, ldLibraryPath);
            return env;
        }
    }
}
