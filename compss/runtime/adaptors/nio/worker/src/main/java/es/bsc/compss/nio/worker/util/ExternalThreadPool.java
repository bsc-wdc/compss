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
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.executors.ExternalExecutor;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.compss.nio.NIOTracer;


/**
 * Handles the bash piper script and its Gobblers The processes opened by each Thread inside the pool are managed by
 * their finish() method
 *
 */
public abstract class ExternalThreadPool extends JobsThreadPool {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    // Logger messages
    private static final String ERROR_PB = "Error starting ProcessBuilder";
    private static final String ERROR_GC = "Error generating worker external launch command";

    // Piper paths
    protected static final String PIPER_SCRIPT_RELATIVE_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
            + File.separator + "adaptors" + File.separator + "nio" + File.separator + "pipers" + File.separator;
    private static final String PIPE_SCRIPT_NAME = "bindings_piper.sh";
    private static final String PIPE_FILE_BASENAME = "pipe_";
    private static final int PIPE_CREATION_TIME = 50; // ms

    // Piper process handlers
    protected final String installDir; // InstallDir
    private final String piperScript; // Piper bash script
    protected final String[] writePipeFiles; // Pipe for sending executions
    protected final String[] readPipeFiles; // Pipe to read results
    protected TaskResultReader[] taskResultReader;
    // Added to send data commands to binding
    protected final String writeDataPipeFile; // Pipe for sending data commands
    protected final String readDataPipeFile; // Pipe to read data commands results
    protected static FileOutputStream writeDataStream;

    private Process piper;
    private StreamGobbler outputGobbler;
    private StreamGobbler errorGobbler;


    /**
     * Instantiates a generic external thread pool associated to the given worker and with fixed size
     *
     * @param nw
     * @param size
     * @throws IOException
     */
    public ExternalThreadPool(NIOWorker nw, int size) {
        super(nw, size);

        // Prepare bash piper for bindings
        installDir = nw.getInstallDir();
        String workingDir = nw.getWorkingDir();
        piperScript = installDir + PIPER_SCRIPT_RELATIVE_PATH + PIPE_SCRIPT_NAME;

        // Prepare pipes (one per thread)
        writePipeFiles = new String[size];
        readPipeFiles = new String[size];
        for (int i = 0; i < size; ++i) {
            writePipeFiles[i] = workingDir + PIPE_FILE_BASENAME + UUID.randomUUID().hashCode();
            readPipeFiles[i] = workingDir + PIPE_FILE_BASENAME + UUID.randomUUID().hashCode();
        }

        // Prepare data pipes
        writeDataPipeFile = workingDir + PIPE_FILE_BASENAME + UUID.randomUUID().hashCode();
        readDataPipeFile = workingDir + PIPE_FILE_BASENAME + UUID.randomUUID().hashCode();

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("PIPE Script: " + piperScript);

            StringBuilder writes = new StringBuilder();
            writes.append("WRITE PIPE Files: ");
            for (int i = 0; i < writePipeFiles.length; ++i) {
                writes.append(writePipeFiles[i]).append(" ");
            }
            writes.append("\n");
            LOGGER.debug(writes.toString());

            StringBuilder reads = new StringBuilder();
            reads.append("READ PIPE Files: ");
            for (int i = 0; i < readPipeFiles.length; ++i) {
                reads.append(readPipeFiles[i]).append(" ");
            }
            reads.append("\n");
            LOGGER.debug(reads.toString());

            // Data pipes
            LOGGER.debug("WRITE DATA PIPE: " + writeDataPipeFile);
            LOGGER.debug("READ DATA PIPE: " + readDataPipeFile);
        }

        // Init main ProcessBuilder
        init();

        // Init TaskResultReader to retrieve task results (one per thread)
        taskResultReader = new TaskResultReader[size];
        for (int i = 0; i < size; ++i) {
            taskResultReader[i] = new TaskResultReader(readPipeFiles[i]);
        }

        // Add Shutdown Hook to ensure all sub-processes are closed
        LOGGER.debug("Add ExternalExecutor shutdown hook");
        Runtime.getRuntime().addShutdownHook(new Ender(this));
    }

    private String constructGeneralArgs() {
        // General Args are of the form: NUM_THREADS dataPipeW dataPipeR 2 pipeW1 pipeW2 2 pipeR1 pipeR2
        StringBuilder cmd = new StringBuilder();

        cmd.append(size).append(ExternalExecutor.TOKEN_SEP);

        cmd.append(writeDataPipeFile).append(ExternalExecutor.TOKEN_SEP);
        cmd.append(readDataPipeFile).append(ExternalExecutor.TOKEN_SEP);

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

    private void init() {
        // Init PB to launch commands to bindings
        // Command of the form: bindings_piper.sh NUM_THREADS 2 pipeW1 pipeW2 2 pipeR1 pipeR2 binding args
        LOGGER.info("Init piper ProcessBuilder");
        String generalArgs = constructGeneralArgs();
        String specificArgs = getLaunchCommand();
        if (specificArgs == null) {
            ErrorManager.error(ERROR_GC);
            return;
        }
        ProcessBuilder pb = new ProcessBuilder(piperScript, generalArgs, specificArgs);
        try {
            // Set NW environment
            Map<String, String> env = getEnvironment(nw);

            addEnvironment(env, nw);

            pb.directory(new File(getPBWorkingDir()));
            pb.environment().putAll(env);
            pb.environment().remove(NIOTracer.LD_PRELOAD);
            pb.environment().remove(NIOTracer.EXTRAE_CONFIG_FILE);

            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(Long.parseLong(NIOTracer.getHostID()), NIOTracer.getSyncType());
            }

            piper = pb.start();

            LOGGER.debug("Starting stdout/stderr gobblers ...");
            try {
                piper.getOutputStream().close();
            } catch (IOException e) {
                // Stream closed
            }
            PrintStream out = ((ThreadPrintStream) System.out).getStream();
            PrintStream err = ((ThreadPrintStream) System.err).getStream();
            outputGobbler = new StreamGobbler(piper.getInputStream(), out, LOGGER);
            errorGobbler = new StreamGobbler(piper.getErrorStream(), err, LOGGER);
            outputGobbler.start();
            errorGobbler.start();
        } catch (IOException e) {
            ErrorManager.error(ERROR_PB, e);
        }

        // The ProcessBuilder is non-blocking but we block the thread for a short period of time to allow the
        // bash script to create the needed environment (pipes)
        try {
            Thread.sleep(PIPE_CREATION_TIME * size);
        } catch (InterruptedException e) {
            // No need to catch such exceptions
        }
    }

    protected String getPBWorkingDir() {
        return nw.getWorkingDir();
    }

    private void addEnvironment(Map<String, String> env, NIOWorker nw) {
        env.put(COMPSsConstants.COMPSS_WORKING_DIR, nw.getWorkingDir());
        env.put(COMPSsConstants.COMPSS_APP_DIR, nw.getAppDir());
        if (LOGGER.isDebugEnabled()){
            env.put("COMPSS_BINDINGS_DEBUG", "1");
        }
    }

    /**
     * Stops specific language components. It is executed after all the threads in the pool have been stopped
     *
     */
    @Override
    protected void specificStop() {
        // Wait for piper process builder to end
        // Check out end status and close gobblers
        try {
            LOGGER.info("Waiting for finishing piper process");
            int exitCode = piper.waitFor();
            if (NIOTracer.isActivated()) {
                NIOTracer.emitEvent(NIOTracer.EVENT_END, NIOTracer.getSyncType());
            }
            outputGobbler.join();
            errorGobbler.join();
            if (exitCode != 0) {
                ErrorManager.error("ExternalExecutor piper ended with " + exitCode + " status");
            }
        } catch (InterruptedException e) {
            // No need to handle such exception
        } finally {
            if (piper != null) {
                if (piper.getInputStream() != null) {
                    try {
                        piper.getInputStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
                if (piper.getErrorStream() != null) {
                    try {
                        piper.getErrorStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
            }
        }

        // ---------------------------------------------------------------------------
        LOGGER.info("ExternalThreadPool finished");
    }

    /**
     * Ensures that the bash process and its pipes are killed
     *
     * @param etp
     */
    public static void ender(ExternalThreadPool etp) {
        LOGGER.info("Starting ExternalThreadPool ender");
        // Destroys the bash process
        etp.piper.destroy();

        // Pipes are destroyed by bash TRAP on script
    }

    /**
     * Returns the launch command for every binding
     *
     * @return
     */
    public abstract String getLaunchCommand();

    /**
     * Returns the specific environment variables of each binding
     *
     * @param nw
     * @return
     */
    public abstract Map<String, String> getEnvironment(NIOWorker nw);

    /**
     * Request to delete a data in the external binding
     *
     * @param data
     *            identifier
     * @return True if success, false if not removed
     */
    public abstract void removeExternalData(String dataID);

    /**
     * Requests to serialize an external data to the given path
     *
     * @param name
     * @param path
     * @return
     */
    public abstract boolean serializeExternalData(String name, String path);

}
