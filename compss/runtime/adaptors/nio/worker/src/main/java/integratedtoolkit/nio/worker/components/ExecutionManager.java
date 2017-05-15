package integratedtoolkit.nio.worker.components;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOAgent;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.binders.ThreadBinder;
import integratedtoolkit.nio.worker.binders.Unbinded;
import integratedtoolkit.nio.worker.binders.BindToMap;
import integratedtoolkit.nio.worker.binders.BindToResource;
import integratedtoolkit.nio.worker.exceptions.InvalidMapException;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableCoresException;
import integratedtoolkit.nio.worker.util.CThreadPool;
import integratedtoolkit.nio.worker.util.JavaThreadPool;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.PythonThreadPool;


public class ExecutionManager {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    private final JobsThreadPool pool;

    // Bound Resources
    private final ThreadBinder binderCPUs;
    private final ThreadBinder binderGPUs;


    public static enum BinderType {
        CPU, // CPU
        GPU, // GPU
    }


    /**
     * Instantiates a new task Execution Manager
     * 
     * @param nw
     * @param computingUnitsCPU
     * @param computingUnitsGPU
     * @param cpuMap
     * @param gpuMap
     * @param limitOfTasks
     * @throws InvalidMapException
     */
    public ExecutionManager(NIOWorker nw, int computingUnitsCPU, int computingUnitsGPU, String cpuMap, String gpuMap, int limitOfTasks)
            throws InvalidMapException {

        LOGGER.info("Instantiate Execution Manager");

        // Compute the real number of threads needed on the worker
        int numThreads;
        if (limitOfTasks > 0) {
            numThreads = Math.min(limitOfTasks, computingUnitsCPU);
        } else {
            numThreads = computingUnitsCPU;
        }

        // Instantiate the language dependent thread pool
        String lang = nw.getLang();
        switch (Lang.valueOf(lang.toUpperCase())) {
            case JAVA:
                this.pool = new JavaThreadPool(nw, numThreads);
                break;
            case PYTHON:
                this.pool = new PythonThreadPool(nw, numThreads);
                break;
            case C:
                this.pool = new CThreadPool(nw, numThreads);
                break;
            default:
                this.pool = null;
                // Print to the job.err file
                LOGGER.error("Incorrect language " + lang + " in worker " + nw.getHostName());
                break;
        }

        // Instantiate CPU binders
        LOGGER.debug("Instantiate CPU Binder");
        switch (cpuMap) {
            case NIOAgent.BINDER_DISABLED:
                this.binderCPUs = new Unbinded();
                break;
            case NIOAgent.BINDER_AUTOMATIC:
                String resourceMap = BindToMap.getResourceCPUDescription();
                this.binderCPUs = new BindToMap(computingUnitsCPU, resourceMap);
                break;
            default:
                // Custom user map
                this.binderCPUs = new BindToMap(computingUnitsCPU, cpuMap);
                break;
        }

        // Instantiate GPU Binders
        LOGGER.debug("Instantiate GPU Binder");
        switch (gpuMap) {
            case NIOAgent.BINDER_DISABLED:
                this.binderGPUs = new Unbinded();
                break;
            case NIOAgent.BINDER_AUTOMATIC:
                this.binderGPUs = new BindToResource(computingUnitsGPU);
                break;
            default:
                // Custom user map
                this.binderGPUs = new BindToMap(computingUnitsGPU, gpuMap);
                break;
        }
    }

    /**
     * Initializes the pool of threads that execute tasks
     * 
     * @throws InitializationException
     */
    public void init() throws InitializationException {
        LOGGER.info("Init Execution Manager");
        this.pool.startThreads();
    }

    /**
     * Enqueues a new task
     * 
     * @param nt
     */
    public void enqueue(NIOTask nt) {
        this.pool.enqueue(nt);
    }

    /**
     * Stops the Execution Manager and its pool of threads
     * 
     */
    public void stop() {
        LOGGER.info("Stop Execution Manager");
        // Stop the job threads
        this.pool.stopThreads();
    }

    /**
     * Bind numCUs core units to the job
     * 
     * @param jobId
     * @param numCUs
     * @return
     * @throws UnsufficientAvailableCoresException
     */
    public int[] bind(int jobId, int numCUs, BinderType type) throws UnsufficientAvailableComputingUnitsException {
        switch (type) {
            case CPU:
                return this.binderCPUs.bindComputingUnits(jobId, numCUs);
            case GPU:
                return this.binderGPUs.bindComputingUnits(jobId, numCUs);
        }
        return null;
    }

    /**
     * Release core units occupied by the job
     * 
     * @param jobId
     */
    public void release(int jobId, BinderType type) {
        switch (type) {
            case CPU:
                this.binderCPUs.releaseComputingUnits(jobId);
            case GPU:
                this.binderGPUs.releaseComputingUnits(jobId);
        }
    }

}
