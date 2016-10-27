package integratedtoolkit.nio.worker.util;

import java.io.File;
import java.util.Map;

import integratedtoolkit.ITConstants;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.executors.ExternalExecutor;
import integratedtoolkit.nio.worker.executors.PythonExecutor;


public class PythonThreadPool extends ExternalThreadPool {

    private static final String WORKER_PY_RELATIVE_PATH = File.separator + "pycompss" + File.separator + "worker" + File.separator
            + "piper_worker.py";
    private static final String PYTHON_PIPER = "python_piper.sh";


    public PythonThreadPool(NIOWorker nw, int size) {
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
            PythonExecutor executor = new PythonExecutor(nw, this, queue, writePipeFiles[i], taskResultReader[i]);
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
        // In the PYTHON case binding executor is python -u piper_worker.py debug tracing #threads cmdPipes resultPipes

        StringBuilder cmd = new StringBuilder();

        cmd.append(ITConstants.Lang.PYTHON).append(ExternalExecutor.TOKEN_SEP);

        if (PythonExecutor.pythonPersistentWorker) {
            cmd.append("python").append(ExternalExecutor.TOKEN_SEP).append("-u").append(ExternalExecutor.TOKEN_SEP);
            cmd.append(installDir).append(PythonExecutor.PYCOMPSS_RELATIVE_PATH).append(WORKER_PY_RELATIVE_PATH)
                    .append(ExternalExecutor.TOKEN_SEP);
        } else {
            cmd.append(installDir).append(ExternalThreadPool.PIPER_SCRIPT_RELATIVE_PATH).append(PYTHON_PIPER)
                    .append(ExternalExecutor.TOKEN_SEP);
        }

        cmd.append(NIOWorker.isWorkerDebugEnabled()).append(ExternalExecutor.TOKEN_SEP);
        cmd.append(NIOWorker.isTracingEnabled()).append(ExternalExecutor.TOKEN_SEP);
        cmd.append(size).append(ExternalExecutor.TOKEN_SEP);

        for (int i = 0; i < writePipeFiles.length; ++i) {
            cmd.append(writePipeFiles[i]).append(ExternalExecutor.TOKEN_SEP);
        }

        for (int i = 0; i < readPipeFiles.length; ++i) {
            cmd.append(readPipeFiles[i]).append(ExternalExecutor.TOKEN_SEP);
        }

        return cmd.toString();
    }

    @Override
    public Map<String, String> getEnvironment(NIOWorker nw) {
        return PythonExecutor.getEnvironment(nw);
    }

}