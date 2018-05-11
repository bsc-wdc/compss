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
package es.bsc.compss.nio.worker.util;

import java.io.File;
import java.io.IOException;
import java.util.Map;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.exceptions.InitializationException;
import es.bsc.compss.nio.worker.executors.ExternalExecutor;
import es.bsc.compss.nio.worker.executors.PythonExecutor;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.nio.NIOTracer;


/**
 * Representation of a Python Thread Pool
 *
 */
public class PythonThreadPool extends ExternalThreadPool {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    // Python interpreter (default: system python - usually python)
    private static String pythonInterpreter = "python";
    // Worker subfolder (default: 2)
    private static String workerSubFolder = "2";

    // Python worker relative path
    private static final String WORKER_PY_RELATIVE_PATH = File.separator + "pycompss" + File.separator + "worker" + File.separator + "piper_worker.py";

    /**
     * Creates a new Python Thread Pool associated to the given worker and with fixed size
     *
     * @param nw
     * @param size
     * @throws IOException
     */
    public PythonThreadPool(NIOWorker nw, int size) {
        super(nw, size);
        // FIXME: Any parameter that conditions getLaunchCommand can not be set here since super calls ExternalThreadPool init which calls this.getLaunchCommand.
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
        // The bindingArgs are of the form python -u piper_worker.py debug tracing storageConf #threads cmdPipes
        // resultPipes

        // FIXME: This should be in the constructor
        PythonThreadPool.pythonInterpreter = NIOWorker.getPythonInterpreter();
        // Define the appropriate worker subfolder
        PythonThreadPool.workerSubFolder = NIOWorker.getPythonVersion();

        StringBuilder cmd = new StringBuilder();

        cmd.append(COMPSsConstants.Lang.PYTHON).append(ExternalExecutor.TOKEN_SEP);
        cmd.append(NIOWorker.getPythonVirtualEnvironment()).append(ExternalExecutor.TOKEN_SEP);
        cmd.append(NIOWorker.isTracingEnabled()).append(ExternalExecutor.TOKEN_SEP);

        cmd.append(PythonThreadPool.pythonInterpreter).append(ExternalExecutor.TOKEN_SEP).append("-u").append(ExternalExecutor.TOKEN_SEP);
        cmd.append(installDir).append(PythonExecutor.PYCOMPSS_RELATIVE_PATH)
                              .append(File.separator).append(PythonThreadPool.workerSubFolder)
                              .append(WORKER_PY_RELATIVE_PATH)
                              .append(ExternalExecutor.TOKEN_SEP);

        cmd.append(NIOWorker.isWorkerDebugEnabled()).append(ExternalExecutor.TOKEN_SEP);
        cmd.append(NIOWorker.isTracingEnabled()).append(ExternalExecutor.TOKEN_SEP);
        cmd.append(NIOWorker.getStorageConf()).append(ExternalExecutor.TOKEN_SEP);
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

    public static String getWorkerSubFolder() {
        return workerSubFolder;
    }

    @Override
    protected String getPBWorkingDir() {
        String workingDir = nw.getWorkingDir();
        if (NIOTracer.isActivated()) {
            workingDir += "python";
            if (!new File(workingDir).mkdirs()) {
                ErrorManager.error("Could not create working dir for python tracefiles, path: " + workingDir);
            }
        }
        return workingDir;
    }

    @Override
    public void removeExternalData(String dataID) {
        // Nothing to do in current python worker version (It could be useful when cache available)
    }

    @Override
    public boolean serializeExternalData(String name, String path) {
        // Nothing to do with current python worker version (It could be useful when cache available)
        return false;
    }
}
