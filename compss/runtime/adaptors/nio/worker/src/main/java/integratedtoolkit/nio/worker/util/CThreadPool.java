package integratedtoolkit.nio.worker.util;

import java.util.Map;

import integratedtoolkit.ITConstants;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.executors.CExecutor;
import integratedtoolkit.nio.worker.executors.ExternalExecutor;
import integratedtoolkit.util.ErrorManager;


public class CThreadPool extends ExternalThreadPool {

    private static final String C_PIPER = "c_piper.sh";
    private static final String PERSISTENT_WORKER_C = "/worker/persistent_worker_c";


    public CThreadPool(NIOWorker nw, int size) {
        super(nw, size);
    }

    /**
     * Starts the threads of the pool
     * 
     */
    public void startThreads() throws InitializationException {
        logger.info("Start threads of ThreadPool");
        int i = 0;
        for (Thread t : workerThreads) {
            CExecutor executor = new CExecutor(nw, this, queue, writePipeFiles[i], taskResultReader[i]);
            t = new Thread(executor);
            t.setName(JOB_THREADS_POOL_NAME + " pool thread # " + i);
            t.start();
            i = i + 1;
        }

        sem.acquireUninterruptibly(this.size);
    }

    @Override
    public String getLaunchCommand() {
        // Specific launch command is of the form: binding bindingExecutor bindingArgs
        StringBuilder cmd = new StringBuilder();
        
        cmd.append(ITConstants.Lang.C).append(ExternalExecutor.TOKEN_SEP);
        //No persistent version
        cmd.append(installDir).append(ExternalThreadPool.PIPER_SCRIPT_RELATIVE_PATH).append(C_PIPER).append(ExternalExecutor.TOKEN_SEP);
        
        //Persistent version (uncomment for testing)
        /*if (nw.getAppDir()!=null && !nw.getAppDir().isEmpty()){
        	cmd.append(nw.getAppDir()).append(PERSISTENT_WORKER_C).append(ExternalExecutor.TOKEN_SEP);
        }else{
        	ErrorManager.warn("Appdir is not defined. It is mandatory for c/c++ binding");
        	return null;
        }*/
        	
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

}