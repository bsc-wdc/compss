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


/**
 *
 * @author flordan
 */
public class ExecutionManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    public final int limitOfTasks;
    public int runningTasks;
    public final ExecutionPlatform cpuExecutors;

    public ExecutionManager(InvocationContext context,
            int computingUnitsCPU, String cpuMap,
            int computingUnitsGPU, String gpuMap,
            int computingUnitsFPGA, String fpgaMap,
            int limitOfTasks) {
        this.limitOfTasks = limitOfTasks;
        ResourceManager rm = null;
        try {
            rm = new ResourceManager(computingUnitsCPU, cpuMap, computingUnitsGPU, gpuMap, computingUnitsFPGA, fpgaMap);
        } catch (InvalidMapException ime) {
            ErrorManager.fatal(ime);
        }
        cpuExecutors = new ExecutionPlatform("CPUThreadPool", context, computingUnitsCPU, rm);
    }

    /**
     * Initializes the pool of threads that execute tasks
     *
     * @throws InitializationException
     */
    public void init() throws InitializationException {
        LOGGER.info("Init Execution Manager");
        this.cpuExecutors.start();
    }

    /**
     * Enqueues a new task
     *
     * @param exec
     */
    public void enqueue(Execution exec) {
        this.cpuExecutors.execute(exec);
    }

    /**
     * Stops the Execution Manager and its pool of threads
     *
     */
    public void stop() {
        LOGGER.info("Stopping Threads...");
        // Stop the job threads
        this.cpuExecutors.stop();
    }
}
