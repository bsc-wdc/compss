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
package es.bsc.compss.nio.worker.executors;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.types.resources.components.Processor;

import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class CExecutionCommandGenerator {

    private static final String C_LIB_RELATIVE_PATH = File.separator + "Bindings" + File.separator + "c" + File.separator + "lib";
    private static final String WORKER_C_RELATIVE_PATH = File.separator + "worker" + File.separator + "worker_c";
    private static final String LIBRARY_PATH_ENV = "LD_LIBRARY_PATH";
    private static final String QUOTES="\"";
    
    protected static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    protected static final boolean WORKER_DEBUG = LOGGER.isDebugEnabled();


    public static ArrayList<String> getTaskExecutionCommand(NIOWorker nw, NIOTask nt, String sandBox, int[] assignedCoreUnits,
            int[] assignedGPUs, int[] assignedFPGAs) {
        ArrayList<String> lArgs = new ArrayList<>();

        // NX_ARGS string built from the Resource Description
        StringBuilder reqs = new StringBuilder();
        int numCUs = nt.getResourceDescription().getTotalCPUComputingUnits();
        reqs.append("NX_ARGS='--smp-cpus=").append(numCUs);
        String compss_nx_args;
        if ((compss_nx_args = System.getenv("COMPSS_NX_ARGS")) != null) {
            reqs.append(" " + compss_nx_args);
        }
        // Debug mode on
        if (WORKER_DEBUG) {
<<<<<<< HEAD
            reqs.append("#--summary#--verbose");
=======
            reqs.append(" --summary --verbose-copies --verbose");
>>>>>>> Disable fpga added when is no used, cross-compile flags to make them compatible with ompss flags added, and the LDFLAGS to link added too
        }

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

            for (int j = 0; j < nt.getResourceDescription().getProcessors().size(); j++) {
                Processor p = nt.getResourceDescription().getProcessors().get(j);
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
            reqs.append("#--disable-fpga=yes");
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

        lArgs.add(cuda_visible.toString() +";"+opencl_visible.toString() +";"+ reqs.toString() + " "+ taskset.toString() + nw.getAppDir()
                + WORKER_C_RELATIVE_PATH);
        return lArgs;
    }

    public static Map<String, String> getEnvironment(NIOWorker nw) {
        Map<String, String> env = new HashMap<>();
        String ldLibraryPath = System.getenv(LIBRARY_PATH_ENV);
        if (ldLibraryPath == null) {
            ldLibraryPath = nw.getLibPath();
        } else {
            ldLibraryPath = ldLibraryPath.concat(":" + nw.getLibPath());
        }

        // Add C and commons libs
        ldLibraryPath = ldLibraryPath.concat(":" + nw.getInstallDir() + C_LIB_RELATIVE_PATH);
        ldLibraryPath = ldLibraryPath.concat(":" + nw.getInstallDir() + ExternalExecutor.BINDINGS_RELATIVE_PATH);

        env.put(LIBRARY_PATH_ENV, ldLibraryPath);
        return env;
    }

}
