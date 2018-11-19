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
package es.bsc.compss.executor.utils;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.invokers.commands.piped.EndTaskPipeCommand;
import es.bsc.compss.invokers.commands.piped.ErrorTaskPipeCommand;
import es.bsc.compss.invokers.commands.piped.QuitPipeCommand;
import es.bsc.compss.invokers.external.ExternalCommand.CommandType;
import es.bsc.compss.invokers.external.piped.PipeCommand;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.compss.util.Tracer;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.util.Map;
import java.util.UUID;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class PipedMirror implements ExecutionPlatformMirror {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    // Logger messages
    private static final String ERROR_PIPE_CLOSE = "Error on closing pipe ";
    private static final String ERROR_PIPE_QUIT = "Error finishing readPipeFile ";
    private static final String ERROR_PIPE_NOT_FOUND = "Pipe cannot be found";
    private static final String ERROR_PIPE_NOT_READ = "Pipe cannot be read";
    private static final String ERROR_PB = "Error starting ProcessBuilder";
    private static final String ERROR_GC = "Error generating worker external launch command";

    protected static final String TOKEN_NEW_LINE = "\n";
    protected static final String TOKEN_SEP = " ";

    protected static final String PIPER_SCRIPT_RELATIVE_PATH = "Runtime" + File.separator + "scripts" + File.separator + "system"
            + File.separator + "adaptors" + File.separator + "nio" + File.separator + "pipers" + File.separator;
    private static final String PIPE_SCRIPT_NAME = "bindings_piper.sh";
    private static final String PIPE_FILE_BASENAME = "pipe_";
    private static final int PIPE_CREATION_TIME = 50; // ms
    private static final int MAX_WRITE_PIPE_RETRIES = 3;

    protected final String mirrorId;
    protected final int size;
    protected final String basePipePath;

    private Process piper;
    private StreamGobbler outputGobbler;
    private StreamGobbler errorGobbler;


    public PipedMirror(InvocationContext context, int size) {
        mirrorId = String.valueOf(UUID.randomUUID().hashCode());
        String workingDir = context.getWorkingDir();
        basePipePath = workingDir + PIPE_FILE_BASENAME + mirrorId + "_";
        this.size = size;
    }

    protected void init(InvocationContext context) {
        String installDir = context.getInstallDir();
        String piperScript = installDir + PIPER_SCRIPT_RELATIVE_PATH + PIPE_SCRIPT_NAME;
        LOGGER.debug("PIPE Script: " + piperScript);

        // Init PB to launch commands to bindings
        // Command of the form: bindings_piper.sh NUM_THREADS 2 pipeW1 pipeW2 2 pipeR1 pipeR2 binding args
        String generalArgs = constructGeneralArgs(context);
        String specificArgs = getLaunchCommand(context);
        if (specificArgs == null) {
            ErrorManager.error(ERROR_GC);
            return;
        }
        LOGGER.info("Init piper ProcessBuilder");
        ProcessBuilder pb = new ProcessBuilder(piperScript, generalArgs, specificArgs);
        try {
            // Set NW environment
            Map<String, String> env = getEnvironment(context);

            env.put(COMPSsConstants.COMPSS_WORKING_DIR, context.getWorkingDir());
            env.put(COMPSsConstants.COMPSS_APP_DIR, context.getAppDir());

            pb.directory(new File(getPBWorkingDir(context)));
            pb.environment().putAll(env);
            pb.environment().remove(Tracer.LD_PRELOAD);
            pb.environment().remove(Tracer.EXTRAE_CONFIG_FILE);

            if (Tracer.isActivated()) {
                long tracingHostId = context.getTracingHostID();
                Tracer.emitEvent(tracingHostId, Tracer.getSyncType());
            }

            piper = pb.start();

            LOGGER.debug("Starting stdout/stderr gobblers ...");
            try {
                piper.getOutputStream().close();
            } catch (IOException e) {
                // Stream closed
            }

            outputGobbler = new StreamGobbler(piper.getInputStream(), null, LOGGER);
            errorGobbler = new StreamGobbler(piper.getErrorStream(), null, LOGGER);
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

    private String constructGeneralArgs(InvocationContext context) {
        StringBuilder cmd = new StringBuilder();

        String computePipes = basePipePath + "compute";
        String dataPipe = basePipePath + "data";

        // NUM THREADS
        cmd.append(size).append(TOKEN_SEP);

        // Data pipes
        cmd.append(dataPipe).append(".outbound").append(TOKEN_SEP);
        cmd.append(dataPipe).append(".inbound").append(TOKEN_SEP);

        // Write Pipes
        cmd.append(size).append(TOKEN_SEP);
        StringBuilder writePipes = new StringBuilder();
        if (size > 0) {
            writePipes.append(computePipes).append("0.outbound");
        }
        for (int i = 1; i < size; ++i) {
            writePipes.append(TOKEN_SEP).append(computePipes).append(i).append(".outbound");
        }
        cmd.append(writePipes.toString()).append(TOKEN_SEP);

        // Read Pipes
        cmd.append(size).append(TOKEN_SEP);
        StringBuilder readPipes = new StringBuilder();
        if (size > 0) {
            readPipes.append(computePipes).append("0.inbound");
        }
        for (int i = 1; i < size; ++i) {
            readPipes.append(TOKEN_SEP).append(computePipes).append(i).append(".inbound");
        }
        cmd.append(readPipes.toString()).append(TOKEN_SEP);

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("WRITE PIPE Files: " + writePipes.toString() + "\n");
            LOGGER.debug("READ PIPE Files: " + readPipes.toString() + "\n");

            // Data pipes
            LOGGER.debug("WRITE DATA PIPE: " + dataPipe + ".outbound");
            LOGGER.debug("READ DATA PIPE: " + dataPipe + ".inbound");
        }

        // General Args are of the form: NUM_THREADS dataPipeW dataPipeR 2 pipeW1 pipeW2 2 pipeR1 pipeR2
        return cmd.toString();
    }

    /**
     * Returns the launch command for every binding
     *
     * @param context
     * @return
     */
    public abstract String getLaunchCommand(InvocationContext context);

    /**
     * Returns the specific environment variables of each binding
     *
     * @param context
     * @return
     */
    public abstract Map<String, String> getEnvironment(InvocationContext context);

    protected String getPBWorkingDir(InvocationContext context) {
        return context.getWorkingDir();
    }

    @Override
    public void stop() {
        stopPipes();
        stopPiper();
    }

    private void stopPipes() {
        LOGGER.info("Stopping compute pipes for mirror " + mirrorId);
        for (int i = 0; i < size; ++i) {
            PipePair pipes = new PipePair("compute" + i);
            pipes.close();
        }
        /*
         * LOGGER.info("Stopping data pipes for mirror " + mirrorId); PipePair pipes = new PipePair("data");
         * pipes.close(); LOGGER.info("All pipes for mirror " + mirrorId + " are stopped");
         */
    }

    private void stopPiper() {
        try {
            LOGGER.info("Waiting for finishing piper process");
            int exitCode = piper.waitFor();
            if (Tracer.isActivated()) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.getSyncType());
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

    public PipePair getPipes(String executorId) {
        return new PipePair(executorId);
    }


    public class PipePair {

        private final String pipePath;


        private PipePair(String id) {
            pipePath = PipedMirror.this.basePipePath + id;
        }

        public boolean send(PipeCommand command) {
            boolean done = false;
            int retries = 0;
            String taskCMD = command.getAsString();
            String writePipe = pipePath + ".outbound";
            if (LOGGER.isDebugEnabled()) {
                LOGGER.debug("EXECUTOR COMMAND: " + taskCMD + " @ " + writePipe);
            }
            taskCMD = taskCMD + TOKEN_NEW_LINE;
            while (!done && retries < MAX_WRITE_PIPE_RETRIES) {
                // Send to pipe : task tID command(jobOut jobErr externalCMD) \n
                OutputStream output = null;
                try {
                    output = new FileOutputStream(writePipe, true);
                    output.write(taskCMD.getBytes());
                    output.flush();
                    done = true;
                } catch (Exception e) {
                    LOGGER.debug("Error on pipe write. Retry");
                    ++retries;
                } finally {
                    if (output != null) {
                        try {
                            output.close();
                        } catch (Exception e) {
                            ErrorManager.error(ERROR_PIPE_CLOSE + writePipe, e);
                        }
                    }
                }
            }
            return done;
        }

        @Override
        public String toString() {
            return "READ pipe: " + pipePath + ".inbound  WRITE pipe:" + pipePath + ".outbound";
        }

        public PipeCommand read() {
            PipeCommand readCommand = null;
            try {
                FileInputStream input = new FileInputStream(pipePath + ".inbound");// WARN: This call is blocking for
                                                                                   // NamedPipes

                String line = null;
                try (BufferedReader reader = new BufferedReader(new InputStreamReader(input))) {
                    line = reader.readLine();
                }
                if (line != null) {
                    String[] result = line.split(" ");

                    // Skip if line is not well formed
                    if (result.length < 1) {
                        LOGGER.warn("Skipping line: " + line);
                        return null;
                    }
                    // Process the received tag
                    CommandType commandType = CommandType.valueOf(result[0].toUpperCase());
                    switch (commandType) {
                        case QUIT:
                            // This quit is received from our proper shutdown, not from bindings. We just end
                            LOGGER.debug("Received quit message");
                            break;
                        case END_TASK:
                            LOGGER.debug("Received endTask message: " + line);
                            if (result.length < 3) {
                                LOGGER.warn("WARN: Skipping endTask line because is malformed");
                                break;
                            }
                            // Line of the form: "endTask" ID STATUS D paramType1 paramValue1 ... paramTypeD paramValueD
                            readCommand = new EndTaskPipeCommand(result);
                            break;
                        case ERROR_TASK:
                            LOGGER.debug("Received errorTask message: " + line);
                            // We have received a fatal error from bindings, we notify error to every waiter and end
                            readCommand = new ErrorTaskPipeCommand(result);
                            break;
                        case EXECUTE_TASK:
                        case REMOVE:
                        case SERIALIZE:
                            // Should not receive any of these tags
                        default:
                            LOGGER.warn("Unrecognised tag on PipedMirror: " + result[0] + ". Skipping message");
                            break;
                    }
                }
            } catch (FileNotFoundException fnfe) {
                // This exception is only handled at the beginning of the execution
                // when the pipe is not created yet. Only display on debug
                LOGGER.debug(ERROR_PIPE_NOT_FOUND + " " + pipePath + ".inbound");
            } catch (IOException ioe) {
                LOGGER.error(ERROR_PIPE_NOT_READ);
            }
            return readCommand;
        }

        public void close() {
            // Send quit tag to pipe
            String writePipe = pipePath + ".outbound";
            if (new File(writePipe).exists()) {
                boolean done = this.send(new QuitPipeCommand());
                if (!done) {
                    ErrorManager.error(ERROR_PIPE_QUIT + writePipe);
                }
            }
        }
    }
}
