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
package es.bsc.compss.invokers.util;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.invokers.types.CParams;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.components.Processor;
import es.bsc.compss.types.resources.components.Processor.ProcessorType;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;


public class CExecutionCommandGenerator {

    private static final String BINDINGS_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "bindings-common" + File.separator + "lib";
    private static final String C_LIB_RELATIVE_PATH =
        File.separator + "Bindings" + File.separator + "c" + File.separator + "lib";
    private static final String WORKER_C_RELATIVE_PATH = File.separator + "worker" + File.separator + "worker_c";
    private static final String LIBRARY_PATH_ENV = "LD_LIBRARY_PATH";
    private static final String QUOTES = "\"";


    /**
     * Generate task execution command.
     * 
     * @param context Task execution context
     * @param invocation Task execution description
     * @param sandBox Execution sandbox
     * @param assignedResources Assigned resources
     * @return execution command as list of strings
     */
    public static ArrayList<String> getTaskExecutionCommand(InvocationContext context, Invocation invocation,
        String sandBox, InvocationResources assignedResources) {

        // NX_ARGS string built from the Resource Description
        StringBuilder reqs = new StringBuilder();
        MethodResourceDescription requirements = (MethodResourceDescription) invocation.getRequirements();
        int numCUs = requirements.getTotalCPUComputingUnits();
        reqs.append("NX_ARGS='--smp-cpus=").append(numCUs);
        String compssNXArgs;
        if ((compssNXArgs = System.getenv("COMPSS_NX_ARGS")) != null) {
            reqs.append(" " + compssNXArgs);
        }
        // Debug mode on
        if (invocation.isDebugEnabled()) {
            reqs.append("#--summary#--verbose-copies#--verbose");
        }

        StringBuilder cudaVisible = new StringBuilder();
        StringBuilder openCLVisible = new StringBuilder();
        cudaVisible.append("CUDA_VISIBLE_DEVICES=").append(QUOTES);
        openCLVisible.append("GPU_DEVICE_ORDINAL=").append(QUOTES);
        int[] assignedGPUs = assignedResources.getAssignedGPUs();
        int[] assignedFPGAs = assignedResources.getAssignedFPGAs();
        if (assignedGPUs.length > 0) {
            reqs.append("#--gpu-warmup=no");
            for (int i = 0; i < (assignedGPUs.length - 1); i++) {
                cudaVisible.append(assignedGPUs[i]).append(",");
                openCLVisible.append(assignedGPUs[i]).append(",");
            }
            cudaVisible.append(assignedGPUs[assignedGPUs.length - 1]);
            openCLVisible.append(assignedGPUs[assignedGPUs.length - 1]);

            for (int j = 0; j < requirements.getProcessors().size(); j++) {
                Processor p = requirements.getProcessors().get(j);
                if (p.getType().equals(ProcessorType.GPU) && p.getInternalMemory() > 0.00001) {
                    float bytes = p.getInternalMemory() * 1048576; // MB to byte conversion
                    int bInt = Math.round(bytes);
                    reqs.append("#--gpu-max-memory=").append(bInt);
                }
            }
        } else if (assignedFPGAs.length > 0) {
            reqs.append("#--disable-cuda=yes");
        } else {
            reqs.append("#--disable-cuda=yes");
            reqs.append("#--disable-opencl=yes");
            reqs.append("#--disable-fpga=yes");
        }
        cudaVisible.append(QUOTES);
        openCLVisible.append(QUOTES);
        reqs.append("'");

        // Taskset string to bind the job
        StringBuilder taskset = new StringBuilder();
        int[] assignedCoreUnits = assignedResources.getAssignedCPUs();
        if (assignedCoreUnits != null && assignedCoreUnits.length > 0) {
            taskset.append("taskset -c ");
            for (int i = 0; i < (numCUs - 1); i++) {
                taskset.append(assignedCoreUnits[i]).append(",");
            }
            taskset.append(assignedCoreUnits[numCUs - 1]).append(" ");
        }

        ArrayList<String> lArgs = new ArrayList<>();
        lArgs.add(cudaVisible.toString() + ";" + openCLVisible.toString() + ";" + reqs.toString() + " "
            + taskset.toString() + context.getAppDir() + WORKER_C_RELATIVE_PATH);
        return lArgs;
    }

    /**
     * Get execution environment.
     * 
     * @param context Task execution context
     * @return Environment as key-value map
     */
    public static Map<String, String> getEnvironment(InvocationContext context) {

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

        Map<String, String> env = new HashMap<>();
        env.put(LIBRARY_PATH_ENV, ldLibraryPath);
        return env;
    }

}
