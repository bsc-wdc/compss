package integratedtoolkit.nio.worker.components;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.ITConstants.Lang;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.util.CThreadPool;
import integratedtoolkit.nio.worker.util.JavaThreadPool;
import integratedtoolkit.nio.worker.util.JobsThreadPool;
import integratedtoolkit.nio.worker.util.PythonThreadPool;


public class ExecutionManager {

    private static final Logger logger = LogManager.getLogger(Loggers.WORKER_EXEC_MANAGER);

    private JobsThreadPool pool;


    public ExecutionManager(NIOWorker nw, int numThreads) {
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

}
