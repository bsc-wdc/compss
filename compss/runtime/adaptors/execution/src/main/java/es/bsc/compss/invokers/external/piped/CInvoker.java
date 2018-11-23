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
import es.bsc.compss.executor.Executor.ExecutorsContext;
import es.bsc.compss.executor.utils.ExecutionPlatformMirror;
import es.bsc.compss.executor.utils.PipedMirror;
import es.bsc.compss.executor.utils.PipedMirror.PipePair;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.commands.external.ExecuteTaskExternalCommand;
import es.bsc.compss.invokers.commands.piped.ExecuteTaskPipeCommand;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;

import java.io.File;
import java.util.HashMap;
import java.util.Map;


public class CInvoker extends PipedInvoker {

    private static final String WORKER_C_RELATIVE_PATH = File.separator + "worker" + File.separator + "worker_c";
    private static final String QUOTES = "\"";


    public CInvoker(InvocationContext context, Invocation invocation, File taskSandboxWorkingDir, InvocationResources assignedResources,
            PipePair pipes) throws JobExecutionException {
        super(context, invocation, taskSandboxWorkingDir, assignedResources, pipes);

    }

    @Override
    protected ExecuteTaskExternalCommand getTaskExecutionCommand(InvocationContext context, Invocation invocation, String sandBox,
            InvocationResources assignedResources) {

        ExecuteTaskPipeCommand taskExecution = new ExecuteTaskPipeCommand(invocation.getJobId());

        // NX_ARGS string built from the Resource Description
        StringBuilder reqs = new StringBuilder();
        MethodResourceDescription requirements = (MethodResourceDescription) invocation.getRequirements();
        int numCUs = requirements.getTotalCPUComputingUnits();
        reqs.append("NX_ARGS='--smp-cpus=").append(numCUs);
        String compss_nx_args;
        if ((compss_nx_args = System.getenv("COMPSS_NX_ARGS")) != null) {
            reqs.append(" " + compss_nx_args);
        }
        // Debug mode on
        if (invocation.isDebugEnabled()) {
            reqs.append("#--summary#--verbose");
        }
        int[] assignedCoreUnits = assignedResources.getAssignedCPUs();
        int[] assignedGPUs = assignedResources.getAssignedGPUs();
        int[] assignedFPGAs = assignedResources.getAssignedFPGAs();

        StringBuilder cuda_visible = new StringBuilder();
        StringBuilder opencl_visible = new StringBuilder();
        cuda_visible.append("CUDA_VISIBLE_DEVICES=").append(QUOTES);
        opencl_visible.append("GPU_DEVICE_ORDINAL=").append(QUOTES);
        if (assignedGPUs.length > 0) {
            reqs.append("#--gpu-warmup=no");
            for (int i = 0; i < (assignedGPUs.length - 1); i++) {
                cuda_visible.append(assignedGPUs[i]).append(",");
                opencl_visible.append(assignedGPUs[i]).append(",");
            }
            cuda_visible.append(assignedGPUs[assignedGPUs.length - 1]);
            opencl_visible.append(assignedGPUs[assignedGPUs.length - 1]);

            for (int j = 0; j < requirements.getProcessors().size(); j++) {
                Processor p = requirements.getProcessors().get(j);
                if (p.getType().equals("GPU") && p.getInternalMemory() > 0.00001) {
                    float bytes = p.getInternalMemory() * 1048576; // MB to byte conversion
                    int b_int = Math.round(bytes);
                    reqs.append("#--gpu-max-memory=").append(b_int);
                }
            }
        } else if (assignedFPGAs.length > 0) {
            reqs.append("#--disable-cuda=yes");
        } else {
            reqs.append("#--disable-cuda=yes");
            reqs.append("#--disable-opencl=yes");
        }
        cuda_visible.append(QUOTES);
        opencl_visible.append(QUOTES);
        reqs.append("'");

        // Taskset string to bind the job
        StringBuilder taskset = new StringBuilder();
        if (assignedCoreUnits != null && assignedCoreUnits.length > 0) {
            taskset.append("taskset -c ");
            for (int i = 0; i < (numCUs - 1); i++) {
                taskset.append(assignedCoreUnits[i]).append(",");
            }
            taskset.append(assignedCoreUnits[numCUs - 1]).append(" ");
        }
        taskExecution.appendArgument(cuda_visible.toString() + ";" + opencl_visible.toString() + ";" + reqs.toString());
        if (taskset.length() > 0) {
            taskExecution.appendArgument(taskset.toString());
        }
        taskExecution.appendArgument(context.getAppDir() + WORKER_C_RELATIVE_PATH);

        return taskExecution;
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
        protected static final String BINDINGS_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "bindings-common"
                + File.separator + "lib";


        public CMirror(InvocationContext context, int size) {
            super(context, size);
            init(context);
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
            CParams cParams = (CParams) context.getLanguageParams(Lang.C);
            if (ldLibraryPath == null) {
                ldLibraryPath = cParams.getLibraryPath();
            } else {
                ldLibraryPath = ldLibraryPath.concat(":" + cParams.getLibraryPath());
            }

            // Add C and commons libs
            ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + C_LIB_RELATIVE_PATH);
            ldLibraryPath = ldLibraryPath.concat(":" + context.getInstallDir() + BINDINGS_RELATIVE_PATH);

            env.put(LIBRARY_PATH_ENV, ldLibraryPath);
            return env;
        }
    }
}
