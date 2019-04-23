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

package es.bsc.compss.executor;

import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.utils.ExecutionPlatform;
import es.bsc.compss.executor.utils.ResourceManager;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.InitializationException;
import es.bsc.compss.types.execution.exceptions.InvalidMapException;
import es.bsc.compss.util.ErrorManager;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class ExecutionManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    private final ExecutionPlatform cpuExecutors;


    /**
     * Instantiates a new Execution Manager. 
     *
     * @param context Invocation context
     * @param computingUnitsCPU Number of CPU Computing Units
     * @param cpuMap CPU Mapping
     * @param computingUnitsGPU Number of GPU Computing Units
     * @param gpuMap GPU Mapping
     * @param computingUnitsFPGA Number of FPGA Computing Units
     * @param fpgaMap FPGA Mapping
     * @param limitOfTasks Limit of number of simultaneous tasks
     */
    public ExecutionManager(InvocationContext context, int computingUnitsCPU, String cpuMap, int computingUnitsGPU,
            String gpuMap, int computingUnitsFPGA, String fpgaMap, int limitOfTasks) {

        ResourceManager rm = null;
        try {
            rm = new ResourceManager(computingUnitsCPU, cpuMap, computingUnitsGPU, gpuMap, computingUnitsFPGA, fpgaMap);
        } catch (InvalidMapException ime) {
            ErrorManager.fatal(ime);
        }
        this.cpuExecutors = new ExecutionPlatform("CPUThreadPool", context, computingUnitsCPU, rm);
    }

    /**
     * Initializes the pool of threads that execute tasks.
     *
     * @throws InitializationException Error initializing execution Manager
     */
    public void init() throws InitializationException {
        LOGGER.info("Init Execution Manager");
        this.cpuExecutors.start();
    }

    /**
     * Enqueues a new task.
     *
     * @param exec Task execution description
     */
    public void enqueue(Execution exec) {
        this.cpuExecutors.execute(exec);
    }

    /**
     * Stops the Execution Manager and its pool of threads.
     */
    public void stop() {
        LOGGER.info("Stopping Threads...");
        // Stop the job threads
        this.cpuExecutors.stop();
    }

    /**
     * Increase execution manager capabilities.
     * @param cpuCount Number of CPU Computing Units
     * @param gpuCount Number of GPU Computing Units
     * @param fpgaCount Number of FPGA Computing Units
     * @param otherCount Number of Other type of Computing Units
     */
    public void increaseCapabilities(int cpuCount, int gpuCount, int fpgaCount, int otherCount) {
        /*
         * if (tracing_level == Tracer.BASIC_MODE) { Tracer.enablePThreads(); }
         */
        this.cpuExecutors.addWorkerThreads(cpuCount);
        /*
         * if (tracing_level == Tracer.BASIC_MODE) { Tracer.disablePThreads(); }
         */
    }

    /**
     * Reduce execution manager capabilities.
     * @param cpuCount Number of CPU Computing Units
     * @param gpuCount Number of GPU Computing Units
     * @param fpgaCount Number of FPGA Computing Units
     * @param otherCount Number of Other type of Computing Units
     */
    public void reduceCapabilities(int cpuCount, int gpuCount, int fpgaCount, int otherCount) {
        /*
         * if (tracing_level == Tracer.BASIC_MODE) { Tracer.enablePThreads(); }
         */
        this.cpuExecutors.removeWorkerThreads(cpuCount);
        /*
         * if (tracing_level == Tracer.BASIC_MODE) { Tracer.disablePThreads(); }
         */
    }
}
