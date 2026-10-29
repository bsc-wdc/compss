/*
 *  Copyright 2002-2026 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.external.CommandChannelException;
import es.bsc.compss.executor.external.ExecutorContextMirror;
import es.bsc.compss.executor.external.commands.ExternalCommand;
import es.bsc.compss.executor.external.piped.commands.AddExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.AddedExecutorPipeCommand;
import es.bsc.compss.executor.external.piped.commands.CancelJobCommand;
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
import es.bsc.wdc.tracing.Tracer;
import java.io.File;
import java.io.IOException;
import java.sql.Timestamp;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.Semaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public abstract class PipedMirror implements ExecutorContextMirror<PipedExecutor> {

    // Logger messages
    private static final String ERROR_NOT_IMPL_EXC = "Not yet implemented. Specific exception should be raised";
    private static final String ERROR_PB_START = "Error starting ProcessBuilder";
    private static final String ERROR_W_START = "Error starting Worker";
    private static final String ERROR_W_PIPE = "Error on Worker pipe";

    protected static final String TOKEN_NEW_LINE = "\n";
    protected static final String TOKEN_SEP = " ";

    // Piper paths
    protected static final String PIPER_SCRIPT_RELATIVE_PATH =
        File.separator + "Runtime" + File.separator + "scripts" + File.separator + "system" + File.separator
            + "adaptors" + File.separator + "nio" + File.separator + "pipers" + File.separator;
    private static final String PIPE_SCRIPT_NAME = "bindings_piper.sh";
    private static final String PIPE_FILE_BASENAME = "pipe_";

    private static final Logger logger = LogManager.getLogger(Loggers.WORKER_EXECUTOR);

    protected final String mirrorId;
    protected final ExecutorContext mirroredContext;
    // Authoritative registry of the mirror's executors: it owns the executor pipes (pre-populated at startup by
    // constructPipeBuilderArgs with one entry per pre-created pipe), and it is the structure that unregisterExecutor
    // and the stop path enumerate to shut the pipes down. All access is synchronized on it
    private final HashMap<String, PipedExecutor> mirroredExecutors;
    // Cache of the current thread's executor, so the hot path does not pay a pipe-command round-trip per task. It is
    // advisory: entries are validated through the canRun check and re-registered on the mirror when stale, and stop()
    // only removes the stopping thread's entry (other threads' entries become inert garbage reclaimed lazily)
    private final ThreadLocal<PipedExecutor> threadExecutor;
    protected final String basePipePath;

    private final MirrorMonitor monitor;

    private Process pipeBuilderProcess;
    private StreamGobbler pipeBuildeOutGobbler;
    private StreamGobbler pipeBuildeErrGobbler;

    private ControlPipePair pipeBuilderPipe;
    private ControlPipePair pipeWorkerPipe;

    private final Semaphore workerEndSem = new Semaphore(0);


    /**
     * Piped Mirror constructor.
     *
     * @param platform Mirrored platform
     */
    public PipedMirror(ExecutorContext platform) {
        this.mirrorId = String.valueOf(UUID.randomUUID().hashCode());
        InvocationContext invContext = platform.getContext();
        String workingDir = invContext.getWorkingDir();
        if (!workingDir.endsWith(File.separator)) {
            workingDir += File.separator;
        }
        this.basePipePath = workingDir + PIPE_FILE_BASENAME + this.mirrorId + "_";
        this.mirroredContext = platform;
        this.mirroredExecutors = new HashMap<>();
        this.threadExecutor = new ThreadLocal<>();
        this.monitor = new MirrorMonitor();
    }

    protected final void init(InvocationContext context) {
        // Start monitor
        this.monitor.start();

        // Configure PipeBuilder
        startPipeBuilder(context);

        // Start worker
        startWorker(context);
    }

    private void startPipeBuilder(InvocationContext context) {
        String installDir = context.getInstallDir();
        String piperScript = installDir + PIPER_SCRIPT_RELATIVE_PATH + PIPE_SCRIPT_NAME;
        logger.debug("PIPE Script: {}", piperScript);

        int initialExecutorCount = mirroredContext.getSize();
        String args = constructPipeBuilderArgs(context, initialExecutorCount);
        logger.debug("Init piper PipeBuilder. Args: {}", args);
        ProcessBuilder pb = new ProcessBuilder(piperScript, args);
        try {
            // Set NW environment
            Map<String, String> env = getEnvironment(context);

            env.put(COMPSsConstants.COMPSS_WORKING_DIR, context.getWorkingDir());
            env.put(COMPSsConstants.COMPSS_APP_DIR, context.getAppDir());

            pb.directory(new File(getPBWorkingDir(context)));
            pb.environment().putAll(env);

            // Setup process environment -- Tracing entries
            Tracer.prepareSubProcessEnvironment(pb.environment(), true);

            // Emit event for worker initialisation
            if (Tracer.isActivated()) {
                int size = this.mirroredContext.getSize();
                Tracer.startSynchronization(size);
            }

            // Launch process builder
            this.pipeBuilderProcess = pb.start();
            logger.debug("Starting stdout/stderr gobblers ...");
            try {
                this.pipeBuilderProcess.getOutputStream().close();
            } catch (IOException e) {
                // Stream no Longer Exists
            }

            // Active wait until the process is created and the pipes are ready
            while (this.pipeBuilderProcess.isAlive() && !new File(this.pipeBuilderPipe.getOutboundPipe()).exists()) {
                // TODO: SHOULD WE ADD A TIMEOUT AT THIS POINT??
            }

            // Check that the process is still alive
            if (!this.pipeBuilderProcess.isAlive()) {
                ErrorManager.fatal(ERROR_PB_START);
            }

            // Enable pipes, monitor and commands
            this.pipeBuildeOutGobbler =
                new StreamGobbler(this.pipeBuilderProcess.getInputStream(), null, logger, false);
            this.pipeBuildeErrGobbler = new StreamGobbler(this.pipeBuilderProcess.getErrorStream(), null, logger, true);
            this.pipeBuildeOutGobbler.start();
            this.pipeBuildeErrGobbler.start();

            this.monitor.mainProcess(this.pipeBuilderProcess, this.pipeBuilderPipe);

            if (this.pipeBuilderPipe.sendCommand(new PingPipeCommand())) {
                try {
                    this.pipeBuilderPipe.waitForCommand(new PongPipeCommand());
                } catch (ClosedPipeException ie) {
                    ErrorManager.fatal("Exception creating process builder. Message: " + ie.getMessage(), ie);
                }
            } else {
                ErrorManager.fatal(ERROR_PB_START + ": Error sending first Ping command. ");
            }
        } catch (IOException e) {
            ErrorManager
                .error(ERROR_PB_START + ": Exception during process builder creation. Message: " + e.getMessage(), e);
        }
    }

    private String constructPipeBuilderArgs(InvocationContext context, int initialExecutorCount) {
        StringBuilder cmd = new StringBuilder();

        // Control pipes
        this.pipeBuilderPipe = new ControlPipePair(basePipePath, "control", this);
        cmd.append(this.pipeBuilderPipe.getOutboundPipe()).append(TOKEN_SEP);
        cmd.append(this.pipeBuilderPipe.getInboundPipe()).append(TOKEN_SEP);
        cmd.append(context.getWorkingDir()).append(TOKEN_SEP);
        cmd.append(context.getLogDir()).append(TOKEN_SEP);

        // Executor Pipes
        StringBuilder writePipes = new StringBuilder();
        StringBuilder readPipes = new StringBuilder();
        for (int i = 0; i < initialExecutorCount; ++i) {
            String pipeName = "executor" + i;
            PipePair executorPipe = new PipePair(basePipePath, pipeName, this);
            PipedExecutor pExecutor = new PipedExecutor(this, executorPipe);
            this.mirroredExecutors.put(pipeName, pExecutor);
            writePipes.append(executorPipe.getOutboundPipe()).append(TOKEN_SEP);
            readPipes.append(executorPipe.getInboundPipe()).append(TOKEN_SEP);
        }

        // Write Pipes
        cmd.append(initialExecutorCount).append(TOKEN_SEP);
        cmd.append(writePipes);

        // Read Pipes
        cmd.append(initialExecutorCount).append(TOKEN_SEP);
        cmd.append(readPipes);

        if (logger.isDebugEnabled()) {
            logger.debug("WRITE PIPE Files: {}\n", writePipes);
            logger.debug("READ PIPE Files: {}\n", readPipes);

            // Data pipes
            logger.debug("WRITE DATA PIPE: {}.outbound", pipeBuilderPipe);
            logger.debug("READ DATA PIPE: {}.inbound", pipeBuilderPipe);
        }

        cmd.append(Tracer.isExtraeActivated()).append(TOKEN_SEP);
        cmd.append(Tracer.getTracerOutputDir()).append(getMirrorName()).append(File.separator).append(TOKEN_SEP);
        cmd.append(getPipeBuilderContext());

        // General Args are of the form: controlPipeW controlPipeR workerPipeW workerPipeR 2 pipeW1 pipeW2 2 pipeR1
        // pipeR2
        return cmd.toString();
    }

    public abstract String getMirrorName();

    public abstract String getPipeBuilderContext();

    private void startWorker(InvocationContext context) {
        this.pipeWorkerPipe = new ControlPipePair(this.basePipePath, "control_worker", this);

        String cmd = getLaunchWorkerCommand(context, this.pipeWorkerPipe);
        StartWorkerPipeCommand swpc = new StartWorkerPipeCommand(cmd, this.pipeWorkerPipe, context.getLogDir());
        if (this.pipeBuilderPipe.sendCommand(swpc)) {
            WorkerStartedPipeCommand startedCMD = new WorkerStartedPipeCommand();
            try {
                this.pipeBuilderPipe.waitForCommand(startedCMD);
            } catch (ClosedPipeException ie) {
                ErrorManager.fatal(ERROR_W_START);
            }
            int workerPID = startedCMD.getPid();

            logger.info("Monitor Worker {}", workerPID);
            this.monitor.monitor(workerPID, new MirrorMonitor.MonitorElementListener() {

                @Override
                public void onDead() {
                    logger.info("Received death notification for mirror {}'s worker process.", mirrorId);
                    pipeWorkerPipe.noLongerExists();
                    pipeWorkerPipe.delete();
                    workerEndSem.release();
                }
            });
        } else {
            ErrorManager.fatal(ERROR_W_START);
        }
    }

    /**
     * Returns the launch command for every binding.
     *
     * @param context Invocation context
     * @param pipe Control pipes
     * @return
     */
    public abstract String getLaunchWorkerCommand(InvocationContext context, ControlPipePair pipe);

    /**
     * Returns the specific environment variables of each binding.
     *
     * @param context Invocation Context
     * @return Environment variables key, value map
     */
    public abstract Map<String, String> getEnvironment(InvocationContext context);

    protected String getPBWorkingDir(InvocationContext context) {
        return context.getWorkingDir();
    }

    @Override
    public void stop() {
        stopWorker();
        stopPiper();

        this.monitor.stop();
        // The thread handle cache goes away with the mirror: this thread's entry is removed, and the entries of
        // other threads refer to handles shut down above, so they are re-created on their next lazy lookup
        this.threadExecutor.remove();
    }

    private void stopExecutors() {
        logger.info("Stopping executor pipes for mirror {}", this.mirrorId);

        // Emit one event when the executors are going to be stopped
        if (Tracer.isActivated()) {
            Tracer.endSynchronization();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Tracer.startSynchronization(timestamp.getTime());
            Tracer.endSynchronization();
        }

        for (Map.Entry<String, PipedExecutor> entry : mirroredExecutors.entrySet()) {
            PipedExecutor pExec = entry.getValue();
            if (pExec.canRun()) {
                shutdownExecutor(pExec);
                logger.debug("EXECUTOR {} shut down!", entry.getKey());
            }
        }
    }

    private void stopWorker() {
        stopExecutors();

        logger.info("Stopping mirror {}", this.mirrorId);
        if (this.pipeWorkerPipe.sendCommand(new QuitPipeCommand())) {
            try {
                this.pipeWorkerPipe.waitForCommand(new QuitPipeCommand());
            } catch (ClosedPipeException cpe) {
                // Worker is already closed
            }
        } else {
            // Worker is already closed
        }

        // Emit event for end worker
        if (Tracer.isActivated()) {
            Tracer.endSynchronization();
            Timestamp timestamp = new Timestamp(System.currentTimeMillis());
            Tracer.startSynchronization(timestamp.getTime());
            Tracer.endSynchronization();
        }
        waitForWorkerEnd();
    }

    private void waitForWorkerEnd() {
        logger.info("Waiting for mirror {}'s worker process end", this.mirrorId);
        try {
            workerEndSem.acquire();
        } catch (InterruptedException ie) {
            // No need to do anything
        }
        logger.info("Mirror {}'s worker process has finished. Continuing mirror shutdown", this.mirrorId);
    }

    private void stopPiper() {
        logger.info("Stopping piper process");

        boolean errorOnPipe = false;
        if (this.pipeBuilderPipe.sendCommand(new QuitPipeCommand())) {
            try {
                this.pipeBuilderPipe.waitForCommand(new QuitPipeCommand());
            } catch (ClosedPipeException cpe) {
                logger.error(ERROR_W_PIPE, cpe);
                errorOnPipe = true;
            }
        } else {
            logger.error(ERROR_W_PIPE);
            errorOnPipe = true;
        }

        try {
            if (errorOnPipe) {
                if (this.pipeBuilderProcess.isAlive()) {
                    logger.info("Error passing QUIT message. Killing piper process");
                    this.pipeBuilderProcess.destroy();
                }
            } else {
                logger.info("Waiting for finishing piper process");
                int exitCode = pipeBuilderProcess.waitFor();

                logger.info("piper process finished");
                if (exitCode != 0) {
                    ErrorManager.error("CommandChannel piper ended with " + exitCode + " status");
                }
            }
            this.pipeBuildeOutGobbler.join();
            this.pipeBuildeErrGobbler.join();
        } catch (InterruptedException e) {
            // No need to handle such exception
        } finally {
            if (this.pipeBuilderProcess != null) {
                if (this.pipeBuilderProcess.getInputStream() != null) {
                    try {
                        this.pipeBuilderProcess.getInputStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
                if (this.pipeBuilderProcess.getErrorStream() != null) {
                    try {
                        this.pipeBuilderProcess.getErrorStream().close();
                    } catch (IOException e) {
                        // No need to handle such exception
                    }
                }
            }
        }
        this.pipeBuilderPipe.delete();
        logger.info("ExternalThreadPool finished");
    }

    @Override
    public PipedExecutor registerExecutor(Executor executor) {
        boolean createExecutor = false;
        PipedExecutor pExecutor;
        int executorId = executor.getId();
        String executorName = "executor" + executorId;
        synchronized (this.mirroredExecutors) {
            pExecutor = this.mirroredExecutors.get(executorName);
            if (pExecutor == null || !pExecutor.canRun()) {
                PipePair pp = new PipePair(this.basePipePath, executorName, this);
                pExecutor = new PipedExecutor(this, pp);
                createExecutor = true;
                this.mirroredExecutors.put(executorName, pExecutor);
            }
        }
        int executorPID;
        if (createExecutor) {
            executorPID = startPipedExecutor(executorId, pExecutor);
        } else {
            executorPID = getExecutorPID(pExecutor);
        }
        registerReadyExecutor(pExecutor, executorPID);

        return pExecutor;
    }

    @Override
    public PipedExecutor getMirroredExecutor(Executor executor) {
        PipedExecutor pExecutor = this.threadExecutor.get();
        if (pExecutor == null || !pExecutor.canRun()) {
            pExecutor = registerExecutor(executor);
            this.threadExecutor.set(pExecutor);
        }
        return pExecutor;
    }

    private int startPipedExecutor(int executorId, PipedExecutor pExec) {
        PipePair pp = pExec.getPipePair();
        // Create pipe
        if (this.pipeBuilderPipe.sendCommand(new CreateChannelPipeCommand(pp))) {
            ChannelCreatedPipeCommand createdPipe = new ChannelCreatedPipeCommand(pp);
            try {
                this.pipeBuilderPipe.waitForCommand(createdPipe);
            } catch (ClosedPipeException ie) {
                throw new UnsupportedOperationException(ERROR_NOT_IMPL_EXC);
            }
        } else {
            throw new UnsupportedOperationException(ERROR_NOT_IMPL_EXC);
        }

        // Launch executor
        if (this.pipeWorkerPipe.sendCommand(new AddExecutorPipeCommand(executorId, pp))) {
            try {
                AddedExecutorPipeCommand reply = new AddedExecutorPipeCommand(pp);
                this.pipeWorkerPipe.waitForCommand(reply);
                if (reply.isSuccess()) {
                    return reply.getPid();
                } else {
                    throw new UnsupportedOperationException(ERROR_NOT_IMPL_EXC);
                }
            } catch (ClosedPipeException ie) {
                throw new UnsupportedOperationException(ERROR_NOT_IMPL_EXC);
            }
        } else {
            throw new UnsupportedOperationException(ERROR_NOT_IMPL_EXC);
        }
    }

    private int getExecutorPID(PipedExecutor pExec) {
        PipePair pp = pExec.getPipePair();
        // Query executor PID
        if (this.pipeWorkerPipe.sendCommand(new ExecutorPIDQueryPipeCommand(pp))) {
            try {
                ExecutorPIDReplyPipeCommand reply = new ExecutorPIDReplyPipeCommand(pp);
                this.pipeWorkerPipe.waitForCommand(reply);
                return reply.getPids().get(0);
            } catch (ClosedPipeException ie) {
                logger.error("Trying to send a command through a closed pipe");
                throw new UnsupportedOperationException(ERROR_NOT_IMPL_EXC);
            }
        } else {
            throw new UnsupportedOperationException(ERROR_NOT_IMPL_EXC);
        }
    }

    private void registerReadyExecutor(PipedExecutor pExec, int pid) {
        logger.info("Monitor Executor {}", pid);
        pExec.deployed();
        this.monitor.monitor(pid, new MirrorMonitor.MonitorElementListener() {

            @Override
            public void onDead() {
                if (pExec.canRun()) {
                    pExec.terminated();
                    destroyedPipedExecutor(pExec);
                }
                // Otherwise, it was already stopped
            }
        });
    }

    @Override
    public void unregisterExecutor(Executor executor) {
        String executorId = "executor" + executor.getId();
        PipedExecutor pExec;
        synchronized (this.mirroredExecutors) {
            pExec = this.mirroredExecutors.remove(executorId);
        }
        if (pExec != null) {
            // Executor alive on the mirror.
            shutdownExecutor(pExec);
            logger.debug("EXECUTOR {} shut down!", executorId);
        }
    }

    private void stopPipedExecutor(PipedExecutor pExec) {
        PipePair execPipe = pExec.getPipePair();
        if (execPipe.sendCommand(new QuitPipeCommand())) {
            try {
                PipeCommand command = null;
                while (command == null) {
                    command = execPipe.readCommand();
                    if (command.getType() != ExternalCommand.CommandType.QUIT) {
                        break;
                    }
                }
            } catch (CommandChannelException cpe) {
                // Executor is already dead -> Do nothing
            }
        }

        // Making sure that it is off through the Worker
        if (this.pipeWorkerPipe.sendCommand(new RemoveExecutorPipeCommand(execPipe))) {
            try {
                this.pipeWorkerPipe.waitForCommand(new RemovedExecutorPipeCommand(execPipe));
            } catch (ClosedPipeException cpe) {
                logger.error(ERROR_W_PIPE, cpe);
            }
        } // else : Worker is dead and executor probably too since it is not responding. Ignore

    }

    private void destroyedPipedExecutor(PipedExecutor pExecutor) {
        PipePair executorPipe = pExecutor.getPipePair();
        // Ensure pipes no longer exists
        executorPipe.noLongerExists();
        executorPipe.delete();
    }

    private void shutdownExecutor(PipedExecutor pExec) {
        pExec.stopping();
        stopPipedExecutor(pExec);
        destroyedPipedExecutor(pExec);
    }

    @Override
    public void cancelJob(PipedExecutor handle) {
        // Making sure that it is off through the Worker
        this.pipeWorkerPipe.sendCommand(new CancelJobCommand(handle.getPipePair()));
    }
}
