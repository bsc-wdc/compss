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
package es.bsc.compss.executor.external.piped;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.executor.external.ExternalExecutorException;
import es.bsc.compss.executor.external.commands.ExternalCommand.CommandType;
import es.bsc.compss.executor.external.piped.commands.AddExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.AddedExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.ChannelCreatedPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CreateChannelPipeCommand;
import es.bsc.compss.executor.external.piped.commands.ExecutorPIDQueryPipeCommand;
import es.bsc.compss.executor.external.piped.commands.ExecutorPIDReplyPipeCommand;
import es.bsc.compss.executor.external.piped.commands.PingPipeCommand;
import es.bsc.compss.executor.external.piped.commands.PipeCommand;
import es.bsc.compss.executor.external.piped.commands.PongPipeCommand;
import es.bsc.compss.executor.external.piped.commands.QuitPipeCommand;
import es.bsc.compss.executor.external.piped.commands.RemoveExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.RemovedExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.StartWorkerPipeCommand;
import es.bsc.compss.executor.external.piped.commands.WorkerStartedPipeCommand;
import es.bsc.compss.executor.external.piped.exceptions.ClosedPipeException;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.util.ErrorManager;
import es.bsc.compss.util.StreamGobbler;
import es.bsc.compss.util.Tracer;
import java.io.File;
import java.io.IOException;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.UUID;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public abstract class PipedMirror implements ExecutionPlatformMirror<PipePair> {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    // Logger messages
    private static final String ERROR_PB_START = "Error starting ProcessBuilder";
    private static final String ERROR_W_START = "Error starting Worker";
    private static final String ERROR_W_PIPE = "Error on Worker pipe";

    protected static final String TOKEN_NEW_LINE = "\n";
    protected static final String TOKEN_SEP = " ";

    protected static final String PIPER_SCRIPT_RELATIVE_PATH = "Runtime" + File.separator + "scripts" + File.separator
            + "system" + File.separator + "adaptors" + File.separator + "nio" + File.separator + "pipers"
            + File.separator;
    private static final String PIPE_SCRIPT_NAME = "bindings_piper.sh";
    private static final String PIPE_FILE_BASENAME = "pipe_";

    protected final String mirrorId;
    protected final int size;
    private final HashMap<String, PipePair> pipePool;
    protected final String basePipePath;

    private final MirrorMonitor monitor;

    private Process pipeBuilderProcess;
    private ControlPipePair pipeBuilderPipe;
    private StreamGobbler pipeBuildeOutGobbler;
    private StreamGobbler pipeBuildeErrGobbler;

    private ControlPipePair pipeWorkerPipe;


    public PipedMirror(InvocationContext context, int size) {
        mirrorId = String.valueOf(UUID.randomUUID().hashCode());
        String workingDir = context.getWorkingDir();
        if (!workingDir.endsWith(File.separator)) {
            workingDir += File.separator;
        }
        basePipePath = workingDir + PIPE_FILE_BASENAME + mirrorId + "_";
        this.size = size;
        this.pipePool = new HashMap<>();
        this.monitor = new MirrorMonitor();
    }

    public String getMirrorId() {
        return this.mirrorId;
    }

    protected final void init(InvocationContext context) {
        monitor.start();
        startPipeBuilder(context);
        startWorker(context);
    }

    private void startPipeBuilder(InvocationContext context) {
        String installDir = context.getInstallDir();
        String piperScript = installDir + PIPER_SCRIPT_RELATIVE_PATH + PIPE_SCRIPT_NAME;
        LOGGER.debug("PIPE Script: " + piperScript);
        String args = constructPipeBuilderArgs(context);
        LOGGER.info("Init piper PipeBuilder");
        ProcessBuilder pb = new ProcessBuilder(piperScript, args);
        try {
            // Set NW environment
            Map<String, String> env = getEnvironment(context);

            env.put(COMPSsConstants.COMPSS_WORKING_DIR, context.getWorkingDir());
            env.put(COMPSsConstants.COMPSS_APP_DIR, context.getAppDir());

            pb.directory(new File(getPBWorkingDir(context)));
            pb.environment().putAll(env);
            pb.environment().remove(Tracer.LD_PRELOAD);
            pb.environment().remove(Tracer.EXTRAE_CONFIG_FILE);

            if (Tracer.extraeEnabled()) {
                long tracingHostId = context.getTracingHostID();
                Tracer.emitEvent(tracingHostId, Tracer.getSyncType());
            }

            pipeBuilderProcess = pb.start();
            LOGGER.debug("Starting stdout/stderr gobblers ...");
            try {
                pipeBuilderProcess.getOutputStream().close();
            } catch (IOException e) {
                // Stream no Longer Exists
            }

            // Active wait until the process is created and the pipes are ready
            while (pipeBuilderProcess.isAlive() && !new File(pipeBuilderPipe.getOutboundPipe()).exists()) {
                // TODO: SHOULD WE ADD A TIMEOUT AT THIS POINT??
            }

            if (!pipeBuilderProcess.isAlive()) {
                ErrorManager.fatal(ERROR_PB_START);
            }

            pipeBuildeOutGobbler = new StreamGobbler(pipeBuilderProcess.getInputStream(), null, LOGGER);
            pipeBuildeErrGobbler = new StreamGobbler(pipeBuilderProcess.getErrorStream(), null, LOGGER);
            pipeBuildeOutGobbler.start();
            pipeBuildeErrGobbler.start();

            monitor.mainProcess(pipeBuilderProcess, pipeBuilderPipe);

            if (pipeBuilderPipe.sendCommand(new PingPipeCommand())) {
                try {
                    pipeBuilderPipe.waitForCommand(new PongPipeCommand());
                } catch (ClosedPipeException ie) {
                    ErrorManager.fatal(ERROR_PB_START);
                }
            } else {
                ErrorManager.fatal(ERROR_PB_START);
            }
        } catch (IOException e) {
            ErrorManager.error(ERROR_PB_START, e);
        }
    }

    private String constructPipeBuilderArgs(InvocationContext context) {
        StringBuilder cmd = new StringBuilder();

        // Control pipes
        pipeBuilderPipe = new ControlPipePair(basePipePath, "control");
        cmd.append(pipeBuilderPipe.getOutboundPipe()).append(TOKEN_SEP);
        cmd.append(pipeBuilderPipe.getInboundPipe()).append(TOKEN_SEP);

        // Executor Pipes
        StringBuilder writePipes = new StringBuilder();
        StringBuilder readPipes = new StringBuilder();
        for (int i = 0; i < size; ++i) {
            String pipeName = "compute" + i;
            PipePair computePipe = new PipePair(basePipePath, pipeName);
            this.pipePool.put(pipeName, computePipe);
            writePipes.append(computePipe.getOutboundPipe()).append(TOKEN_SEP);
            readPipes.append(computePipe.getInboundPipe()).append(TOKEN_SEP);
        }

        // Write Pipes
        cmd.append(size).append(TOKEN_SEP);
        cmd.append(writePipes.toString());

        // Read Pipes
        cmd.append(size).append(TOKEN_SEP);
        cmd.append(readPipes.toString());

        if (LOGGER.isDebugEnabled()) {
            LOGGER.debug("WRITE PIPE Files: " + writePipes.toString() + "\n");
            LOGGER.debug("READ PIPE Files: " + readPipes.toString() + "\n");

            // Data pipes
            LOGGER.debug("WRITE DATA PIPE: " + pipeBuilderPipe + ".outbound");
            LOGGER.debug("READ DATA PIPE: " + pipeBuilderPipe + ".inbound");
        }

        cmd.append(Tracer.getLevel()).append(TOKEN_SEP);

        cmd.append(getPipeBuilderContext());

        // General Args are of the form: controlPipeW controlPipeR workerPipeW workerPipeR 2 pipeW1 pipeW2 2 pipeR1
        // pipeR2
        return cmd.toString();
    }

    public abstract String getPipeBuilderContext();

    private void startWorker(InvocationContext context) {
        pipeWorkerPipe = new ControlPipePair(basePipePath, "control_worker");
        String cmd = getLaunchWorkerCommand(context, pipeWorkerPipe);
        if (pipeBuilderPipe.sendCommand(new StartWorkerPipeCommand(cmd, pipeWorkerPipe))) {
            WorkerStartedPipeCommand startedCMD = new WorkerStartedPipeCommand();
            try {
                pipeBuilderPipe.waitForCommand(startedCMD);
            } catch (ClosedPipeException ie) {
                ErrorManager.fatal(ERROR_W_START);
            }
            int workerPID = startedCMD.getPid();
            monitor.registerWorker(mirrorId, workerPID, pipeWorkerPipe);
        } else {
            ErrorManager.fatal(ERROR_W_START);
        }
    }

    /**
     * Returns the launch command for every binding
     *
     * @param context
     * @param pipe
     * @return
     */
    public abstract String getLaunchWorkerCommand(InvocationContext context, ControlPipePair pipe);

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
        stopWorker();
        stopPiper();
        monitor.stop();
    }

    private void stopExecutors() {
        LOGGER.info("Stopping compute pipes for mirror " + mirrorId);
        for (String executorId : new LinkedList<>(pipePool.keySet())) {
            unregisterExecutor(executorId);
        }
    }

    private void stopWorker() {
        stopExecutors();
        LOGGER.info("Stopping mirror " + mirrorId);
        if (pipeWorkerPipe.sendCommand(new QuitPipeCommand())) {
            try {
                pipeWorkerPipe.waitForCommand(new QuitPipeCommand());
            } catch (ClosedPipeException cpe) {
                // Worker is already closed
            }
        } else {
            // Worker is already closed
        }
        monitor.unregisterWorker(mirrorId);
        pipeWorkerPipe.delete();
    }

    private void stopPiper() {
        LOGGER.info("Stopping piper process");
        if (pipeBuilderPipe.sendCommand(new QuitPipeCommand())) {
            try {
                pipeBuilderPipe.waitForCommand(new QuitPipeCommand());
            } catch (ClosedPipeException cpe) {
                ErrorManager.fatal(ERROR_W_PIPE);
            }
        } else {
            ErrorManager.fatal(ERROR_W_PIPE);
        }

        try {
            LOGGER.info("Waiting for finishing piper process");
            int exitCode = pipeBuilderProcess.waitFor();
            if (Tracer.extraeEnabled()) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.getSyncType());
            }
            pipeBuildeOutGobbler.join();
            pipeBuildeErrGobbler.join();
            if (exitCode != 0) {
                ErrorManager.error("ExternalExecutor piper ended with " + exitCode + " status");
            }
        } catch (InterruptedException e) {
            // No need to handle such exception
        } finally {
            if (pipeBuilderProcess != null) {
                if (pipeBuilderProcess.getInputStream() != null) {
                    try {
                        pipeBuilderProcess.getInputStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
                if (pipeBuilderProcess.getErrorStream() != null) {
                    try {
                        pipeBuilderProcess.getErrorStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
            }
        }
        pipeBuilderPipe.delete();
        // ---------------------------------------------------------------------------
        LOGGER.info("ExternalThreadPool finished");
    }

    @Override
    public PipePair registerExecutor(String executorId) {
        boolean createExecutor = false;
        PipePair pp;
        synchronized (this.pipePool) {
            pp = this.pipePool.get(executorId);
            if (pp == null) {
                pp = new PipePair(this.basePipePath, executorId);
                createExecutor = true;
                this.pipePool.put(executorId, pp);
            }
        }
        int executorPID = -1;
        if (createExecutor) {
            // Create pipe
            if (pipeBuilderPipe.sendCommand(new CreateChannelPipeCommand(pp))) {
                ChannelCreatedPipeCommand createdPipe = new ChannelCreatedPipeCommand(pp);
                try {
                    pipeBuilderPipe.waitForCommand(createdPipe);
                } catch (ClosedPipeException ie) {
                    throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
                }
            } else {
                throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
            }

            // Launch executor
            if (pipeWorkerPipe.sendCommand(new AddExecutorPipeCommand(pp))) {
                try {
                    AddedExecutorPipeCommand reply = new AddedExecutorPipeCommand(pp);
                    pipeWorkerPipe.waitForCommand(reply);
                    executorPID = reply.getPid();

                } catch (ClosedPipeException ie) {
                    throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
                }
            } else {
                throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
            }
        } else {
            // Query executor PID
            if (pipeWorkerPipe.sendCommand(new ExecutorPIDQueryPipeCommand(pp))) {
                try {
                    ExecutorPIDReplyPipeCommand reply = new ExecutorPIDReplyPipeCommand(pp);
                    pipeWorkerPipe.waitForCommand(reply);
                    executorPID = reply.getPids().get(0);
                } catch (ClosedPipeException ie) {
                    throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
                }
            } else {
                throw new UnsupportedOperationException("Not yet implemented. Specific exception should be raised");
            }
        }
        monitor.registerExecutor(this, executorId, executorPID, pp);

        return pp;
    }

    @Override
    public void unregisterExecutor(String executorId) {
        PipePair pp;
        synchronized (this.pipePool) {
            pp = this.pipePool.remove(executorId);
        }
        if (pp == null) {
            // Executor not alive on the mirror
            return;
        }

        // Shutting down executor
        if (pp.sendCommand(new QuitPipeCommand())) {
            try {
                PipeCommand command = null;
                while (command == null) {
                    command = pp.readCommand();
                    if (command.getType() != CommandType.QUIT) {
                        break;
                    }
                }
            } catch (ExternalExecutorException cpe) {
                // Executor is already dead -> Do nothing
            }
        }
        // Executor is down, no need to keep monitoring it
        monitor.unregisterExecutor(this, executorId);

        // Making sure that it is off through the Worker
        if (pipeWorkerPipe.sendCommand(new RemoveExecutorPipeCommand(pp))) {
            try {
                pipeWorkerPipe.waitForCommand(new RemovedExecutorPipeCommand(pp));
            } catch (ClosedPipeException cpe) {
                ErrorManager.fatal(ERROR_W_PIPE);
            }
        } // else : Worker is dead and executor probably too since it is not responding. Ignore

        pp.delete();
        LOGGER.debug("EXECUTOR " + executorId + " shut down!");
    }
}
