package es.bsc.compss.nio.worker.components;

import java.io.File;
import java.io.IOException;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOAgent;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.binders.ThreadBinder;
import es.bsc.compss.nio.worker.binders.Unbinded;
import es.bsc.compss.nio.worker.binders.BindToMap;
import es.bsc.compss.nio.worker.binders.BindToResource;
import es.bsc.compss.nio.worker.exceptions.InvalidMapException;
import es.bsc.compss.nio.worker.exceptions.InitializationException;
import es.bsc.compss.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.nio.worker.exceptions.UnsufficientAvailableCoresException;
import es.bsc.compss.nio.worker.util.CThreadPool;
import es.bsc.compss.nio.worker.util.ExternalThreadPool;
import es.bsc.compss.nio.worker.util.JavaThreadPool;
import es.bsc.compss.nio.worker.util.JobsThreadPool;
import es.bsc.compss.nio.worker.util.PythonThreadPool;


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
     * @throws IOException 
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
        LOGGER.debug("Instantiate CPU Binder with " + computingUnitsCPU +" CUs");
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
        LOGGER.debug("Instantiate GPU Binder with " + computingUnitsGPU +" CUs");
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

    public void removeExternalData(String absFileName) {
        if (this.pool instanceof ExternalThreadPool) {
            // Get dataId from file name
            String filename = new File(absFileName).getName();
            // Check if filename follow the object id pattern.
            if (filename.startsWith("d") && filename.endsWith(".IT")) {
                int index = filename.indexOf('_');
                if (index > 0) {
                	LOGGER.debug("calling remove");
                    ((ExternalThreadPool) this.pool).removeExternalData(filename);
                }

            }

        }

    }

    public boolean serializeExternalData(String name, String path) {
        if (this.pool instanceof ExternalThreadPool) {
            return ((ExternalThreadPool) this.pool).serializeExternalData(name, path);
        }
        return false;
    }

}
