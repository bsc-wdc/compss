package integratedtoolkit.nio.worker.util;

import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.ITConstants;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.executors.CExecutor;
import integratedtoolkit.nio.worker.executors.ExternalExecutor;
import integratedtoolkit.util.ErrorManager;


/**
 * Representation of a C Thread Pool
 * 
 */
public class CThreadPool extends ExternalThreadPool {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    // C worker relative path
    private static final String C_PIPER = "c_piper.sh";
    private static final String PERSISTENT_WORKER_C = "/worker/persistent_worker_c";


    /**
     * Creates a new C Thread Pool associated to the given worker and with fixed size
     * 
     * @param nw
     * @param size
     */
    public CThreadPool(NIOWorker nw, int size) {
        super(nw, size);
    }

    /**
     * Starts the threads of the pool
     * 
     */
    @Override
    public void startThreads() throws InitializationException {
        LOGGER.info("Start threads of ThreadPool");
        int i = 0;
        for (Thread t : workerThreads) {
            CExecutor executor = new CExecutor(nw, this, queue, writePipeFiles[i], taskResultReader[i]);
            t = new Thread(executor);
            t.setName(JOB_THREADS_POOL_NAME + " pool thread # " + i);
            t.start();
            i = i + 1;
        }
        sem.acquireUninterruptibly(this.size);
        LOGGER.debug("Finished C ThreadPool");
    }

    @Override
    public String getLaunchCommand() {
        // Specific launch command is of the form: binding bindingExecutor bindingArgs
        StringBuilder cmd = new StringBuilder();

        cmd.append(ITConstants.Lang.C).append(ExternalExecutor.TOKEN_SEP);
        if (!NIOWorker.isPersistentCEnabled()) {
            // No persistent version
            cmd.append(installDir).append(ExternalThreadPool.PIPER_SCRIPT_RELATIVE_PATH).append(C_PIPER).append(ExternalExecutor.TOKEN_SEP);
        } else {
            // Persistent version

            if (nw.getAppDir() != null && !nw.getAppDir().isEmpty()) {
                cmd.append("NX_ARGS='--enable-block' ").append(nw.getAppDir()).append(PERSISTENT_WORKER_C)
                        .append(ExternalExecutor.TOKEN_SEP);
                // Adding Data pipes in the case of persistent worker
                cmd.append(writeDataPipeFile).append(ExternalExecutor.TOKEN_SEP).append(readDataPipeFile)
                        .append(ExternalExecutor.TOKEN_SEP);
            } else {
                ErrorManager.warn("Appdir is not defined. It is mandatory for c/c++ binding");
                return null;
            }
        }

        cmd.append(writePipeFiles.length).append(ExternalExecutor.TOKEN_SEP);
        for (int i = 0; i < writePipeFiles.length; ++i) {
            cmd.append(writePipeFiles[i]).append(ExternalExecutor.TOKEN_SEP);
        }

        cmd.append(readPipeFiles.length).append(ExternalExecutor.TOKEN_SEP);
        for (int i = 0; i < readPipeFiles.length; ++i) {
            cmd.append(readPipeFiles[i]).append(ExternalExecutor.TOKEN_SEP);
        }

        return cmd.toString();
    }

    @Override
    public Map<String, String> getEnvironment(NIOWorker nw) {
        return CExecutor.getEnvironment(nw);
    }

    @Override
    public void removeExternalData(String dataID) {
        /*
         * TODO ADD MANAGEMENT OF EXTERNAL DATA REMOVE Send external Data removements through data pipes Send data id to
         * remove to persistent worker If in cache, delete (persistent.cc) In persistent, keep reading data pipe, delete
         * if necessary
         */
    }

    @Override
    public boolean serializeExternalData(String dataId, String path) {
        /*
         * TODO ADD MANAGEMENT OF EXTERNAL DATA REMOVE Send external Data serialization through data pipes Check if the
         * data is expected, then serialize Return true if serialization was done, false otherwise
         */
        return false;
    }

}