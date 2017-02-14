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

    private JobsThreadPool pool;

    // Bound Resources
    private ThreadBinder binderCPUs;
    private ThreadBinder binderGPUs;


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
                // Print to the job.err file
                logger.error("Incorrect language " + lang + " in worker " + nw.getHostName());
                break;
        }

        // Set every resource assigned job to -1 (no job assigned to that CU)
        try{
            this.binderCPUs = new ThreadBinderCPU(numThreads);
        }
        catch (InitializationException e){
            this.binderCPUs = new ThreadBinderUnaware(numThreads);
        }
        
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
    public int[] bindCPUs(int jobId, int numCUs) throws UnsufficientAvailableCoresException {
        int assignedCoreUnits[] = new int[numCUs];

        try {
            assignedCoreUnits = this.binderCPUs.bindComputingUnits(jobId, numCUs);
        } catch (UnsufficientAvailableComputingUnitsException e) {
            throw new UnsufficientAvailableCoresException("Not enough available cores for task execution");
        }
        return assignedCoreUnits;
    }

    /**
     * Release core units occupied by the job
     * 
     * @param jobId
     */
    public void releaseCPUs(int jobId) {
        this.binderCPUs.releaseComputingUnits(jobId);
    }

    /**
     * Bind numGPUs core units to the job
     * 
     * @param jobId
     * @param numGPUs
     * @return
     * @throws UnsufficientAvailableGPUsException
     */
    public int[] bindGPUs(int jobId, int numGPUs) throws UnsufficientAvailableGPUsException {
        int assignedGPUs[] = new int[numGPUs];

        try {
            assignedGPUs = this.binderGPUs.bindComputingUnits(jobId, numGPUs);
        } catch (UnsufficientAvailableComputingUnitsException e) {
            throw new UnsufficientAvailableGPUsException("Not enough available GPUs for task execution");
        }

        return assignedGPUs;
    }

    /**
     * Release GPUs occupied by the job
     * 
     * @param jobId
     */
    public void releaseGPUs(int jobId) {
        this.binderGPUs.releaseComputingUnits(jobId);
    }

}
