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

package es.bsc.compss.utils.execution;

import es.bsc.compss.execution.ExecutionPlatform;
import es.bsc.compss.execution.ExecutionPlatformConfiguration;
import es.bsc.compss.execution.utils.ResourceManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.ExecutorRequest;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationExecutionRequest;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.util.ErrorManager;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExecutionManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    private final ExecutionPlatform cpuExecutors;
    private final ExecutionPlatform ioExecutors;


    /**
     * Instantiates a new ExecutorRequest Manager.
     *
     * @param context Invocation context
     * @param computingUnitsCPU Number of CPU Computing Units
     * @param cpuMap CPU Mapping
     * @param reuseCPUsOnBlock if {@literal true} resources are released and the platform accepts new invocations when
     *            an already-running invocation stalls; otherwise, the running invocation keeps the resources.
     * @param computingUnitsGPU Number of GPU Computing Units
     * @param gpuMap GPU Mapping
     * @param computingUnitsFPGA Number of FPGA Computing Units
     * @param fpgaMap FPGA Mapping
     * @param ioExecNum Number of IO Executors
     * @param limitOfTasks Limit of number of simultaneous tasks
     */
    public ExecutionManager(InvocationContext context, int computingUnitsCPU, String cpuMap, boolean reuseCPUsOnBlock,
        int computingUnitsGPU, String gpuMap, int computingUnitsFPGA, String fpgaMap, int ioExecNum, int limitOfTasks) {

        ResourceManager rm = null;
        try {
            rm = new ResourceManager(computingUnitsCPU, cpuMap, computingUnitsGPU, gpuMap, computingUnitsFPGA, fpgaMap);
        } catch (InvalidMapException ime) {
            ErrorManager.fatal(ime);
        }
        ExecutionPlatformConfiguration cpuConf =
            new ExecutionPlatformConfiguration(computingUnitsCPU, reuseCPUsOnBlock);
        this.cpuExecutors = new ExecutionPlatform("CPUThreadPool", context, cpuConf, rm);
        ExecutionPlatformConfiguration ioConf = new ExecutionPlatformConfiguration(ioExecNum, false);
        this.ioExecutors = new ExecutionPlatform("IOThreadPool", context, ioConf, rm);
    }

    /**
     * Initializes the pool of threads that execute tasks.
     *
     * @throws InitializationException Error initializing execution Manager
     */
    public void init() throws InitializationException {
        LOGGER.info("Init Execution Manager");
        this.cpuExecutors.start();
        if (this.ioExecutors.getSize() > 0) {
            this.ioExecutors.start();
        }
    }

    /**
     * Starts the Python mirror.
     */
    public void startMirror() {
        LOGGER.info("Init Python Mirror");
        this.cpuExecutors.startMirror();
    }

    /**
     * Enqueues a new task.
     *
     * @param exec Task execution description
     */
    public void enqueue(InvocationExecutionRequest exec) {
        if (exec.isIOExecution()) {
            if (this.ioExecutors.getSize() == 0) {
                ErrorManager.error("No available IO executors to execute: " + exec.getInvocationSignature());
            } else {
                this.ioExecutors.execute(exec);
            }
        } else {
            this.cpuExecutors.execute(exec);
        }
    }

    /**
     * Stops the ExecutorRequest Manager and its pool of threads.
     */
    public void stop() {
        LOGGER.info("Stopping Threads...");
        // Stop the job threads
        this.cpuExecutors.stop();
        if (this.ioExecutors.getSize() > 0) {
            this.ioExecutors.stop();
        }
    }

    /**
     * Increase execution manager capabilities.
     * 
     * @param cpuCount Number of CPU Computing Units
     * @param gpuCount Number of GPU Computing Units
     * @param fpgaCount Number of FPGA Computing Units
     * @param otherCount Number of Other type of Computing Units
     */
    public void increaseCapabilities(int cpuCount, int gpuCount, int fpgaCount, int otherCount) {
        this.cpuExecutors.addWorkerThreads(cpuCount);
    }

    /**
     * Reduce execution manager capabilities.
     * 
     * @param cpuCount Number of CPU Computing Units
     * @param gpuCount Number of GPU Computing Units
     * @param fpgaCount Number of FPGA Computing Units
     * @param otherCount Number of Other type of Computing Units
     */
    public void reduceCapabilities(int cpuCount, int gpuCount, int fpgaCount, int otherCount) {
        this.cpuExecutors.removeWorkerThreads(cpuCount);
    }

    /**
     * Cancel a running job.
     */
    public void cancelJob(int jobId) {
        this.cpuExecutors.cancelJob(jobId);
    }

}
