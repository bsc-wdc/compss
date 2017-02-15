package integratedtoolkit.nio.worker.components;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableComputingUnitsException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableCoresException;
import integratedtoolkit.nio.worker.exceptions.UnsufficientAvailableGPUsException;
import integratedtoolkit.nio.worker.util.ThreadBinderUnaware;
import integratedtoolkit.nio.worker.util.CThreadPool;
import integratedtoolkit.nio.worker.util.JavaThreadPool;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.PythonThreadPool;
import integratedtoolkit.nio.worker.util.ThreadBinder;
import integratedtoolkit.nio.worker.util.ThreadBinderCPU;


public class ExecutionManager {

    private static final Logger logger = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    private final JobsThreadPool pool;

    // Bound Resources
    private final ThreadBinder binderCPUs;
    private final ThreadBinder binderGPUs;

    public static enum BINDER_TYPE {
        CPU, // CPU
        GPU, // GPU
    }

    public ExecutionManager(NIOWorker nw, int numThreads, int numGPUs) {
        logger.info("Instantiate Execution Manager");

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
                logger.error("Incorrect language " + lang + " in worker " + nw.getHostName());
                break;
        }

        // Instantiate CPU binders
        ThreadBinder tb;
        try {
            tb = new ThreadBinderCPU(numThreads);
        } catch (InitializationException e) {
            tb = new ThreadBinderUnaware(numThreads);
        }
        this.binderCPUs = tb;

        // Instantiate GPU Binders
        this.binderGPUs = new ThreadBinderUnaware(numGPUs);
    }

    public void init() throws InitializationException {
        logger.info("Init Execution Manager");
        this.pool.startThreads();
    }

    public void enqueue(NIOTask nt) {
        this.pool.enqueue(nt);
    }

    public void stop() {
        logger.info("Stop Execution Manager");
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
    public int[] bind(int jobId, int numCUs, BINDER_TYPE type) throws UnsufficientAvailableComputingUnitsException {
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
    public void release(int jobId, BINDER_TYPE type) {
        switch (type) {
            case CPU:
                this.binderCPUs.releaseComputingUnits(jobId);
            case GPU:
                this.binderGPUs.releaseComputingUnits(jobId);
        }
    }

}
