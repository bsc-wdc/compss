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
package es.bsc.compss.execution.invokers.binary;

import static es.bsc.compss.types.implementations.definition.ContainerDescription.ContainerEngine.SINGULARITY;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.api.Workflow;
import es.bsc.compss.exceptions.InvokeExecutionException;
import es.bsc.compss.exceptions.StreamCloseException;
import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.InvocationParam;
import es.bsc.compss.execution.LanguageParams;
import es.bsc.compss.execution.exceptions.JobExecutionException;
import es.bsc.compss.execution.executor.ExecutionSandbox;
import es.bsc.compss.execution.invokers.Invoker;
import es.bsc.compss.execution.invokers.types.PythonParams;
import es.bsc.compss.execution.invokers.types.StdIOStream;
import es.bsc.compss.execution.invokers.util.BinaryRunner;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.semantics.Constants;
import es.bsc.compss.semantics.data.DataType;
import es.bsc.compss.types.implementations.definition.ContainerDescription;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.PrintStream;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

public class MPIInvoker extends Invoker<Workflow> {

    private static final int NUM_BASE_MPI_ARGS = 6;

    private static final String MANA_LAUNCHER_CHECK_CMD = "mana_status";

    private static final String MANA_COORDINATOR_CMD = "mana_coordinator";

    private static final String MANA_LAUNCH_CMD = "mana_launch";
    private static final String MANA_INTERVAL_FLAG = "--interval";
    private static final String MANA_CKPTDIR_FLAG = "--ckptdir";

    private static final String MANA_RESTART_CMD = "mana_restart";
    private static final String MANA_RESTARTDIR_FLAG = "--restartdir";
    private static final String MANA_CHDIR_FLAG = "-C";

    private static final String MANA_CKPT_SUBDIR_PREFIX = "mana_ckpt_";
    // Watchdog of the MANA ranks (see ManaWatchdog). COMPSS_MANA_WATCHDOG_GRACE=<seconds> sets the grace period;
    // 0 turns the watchdog off.
    private static final String MANA_WATCHDOG_GRACE_ENV = "COMPSS_MANA_WATCHDOG_GRACE";
    private static final int DEFAULT_MANA_WATCHDOG_GRACE = 20;
    // COMPSS_MANA_WATCHDOG_STARTUP=<seconds>: time the ranks have to connect at launch or restore; 0 turns it off.
    private static final String MANA_WATCHDOG_STARTUP_ENV = "COMPSS_MANA_WATCHDOG_STARTUP";
    private static final int DEFAULT_MANA_WATCHDOG_STARTUP = 120;
    private static final long MANA_WATCHDOG_PERIOD_MS = 5000L;
    private static final long MANA_KILL_WAIT_MS = 10000L;
    private static final String DMTCP_COMMAND_CMD = "dmtcp_command";
    // A client of mana_status: "3, lower-half[42000:2740318]@gs04r3b43, <unique pid>, WorkerState::RUNNING, ..."
    private static final java.util.regex.Pattern MANA_CLIENT_LINE =
        java.util.regex.Pattern.compile("^\\s*\\d+,\\s*[^,\\[]*\\[\\d+:(\\d+)\\]@([^,\\s]+),");
    private static final String MANA_ARGS_FILE = "launch_args.txt";
    private static final String MANA_SANDBOX_FILE = "launch_sandbox.txt";

    private static final String MANA_HOME_ENV = "MANA_HOME";
    private static final String MANA_BIN_ENV = "MANA_BIN";

    private static final String HYDRA_NO_AUTO_CLEANUP_FLAG = "-disable-auto-cleanup";
    private static final String HYDRA_NO_AUTO_CLEANUP_ENV = "COMPSS_MANA_HYDRA_NO_AUTO_CLEANUP";

    private static final String OMPI_PML_ENV = "OMPI_MCA_pml";
    private static final String OMPI_PML_OB1 = "ob1";
    private static final String OMPI_BTL_ENV = "OMPI_MCA_btl";
    private static final String OMPI_BTL_TCP = "tcp,self,vader";
    private static final String OMPI_OSC_ENV = "OMPI_MCA_osc";
    private static final String OMPI_OSC_NO_UCX = "^ucx";
    private static final String OMPI_TCP_IF_INCLUDE_ENV = "OMPI_MCA_btl_tcp_if_include";
    private static final String OMPI_TCP_IF_EXCLUDE_ENV = "OMPI_MCA_btl_tcp_if_exclude";
    private static final String MANA_TCP_IF_ENV = "COMPSS_MANA_TCP_IF";
    private static final String DEFAULT_IPOIB_IF = "ib0";

    private static final String SCR_CKPT_SUBDIR_PREFIX = "scr_ckpt_";
    private static final String SCR_CACHE_SUBDIR_PREFIX = "scr_cache_";
    private static final String SCR_CNTL_SUBDIR_PREFIX = "scr_cntl_";
    private static final String SCR_PREFIX_ENV = "SCR_PREFIX";
    private static final String SCR_CACHE_BASE_ENV = "SCR_CACHE_BASE";
    private static final String SCR_CNTL_BASE_ENV = "SCR_CNTL_BASE";
    private static final String DEFAULT_SCR_CACHE_ROOT = "/dev/shm";
    private static final String SCR_CHECKPOINT_SECONDS_ENV = "SCR_CHECKPOINT_SECONDS";
    private static final String SCR_CHECKPOINT_INTERVAL_ENV = "SCR_CHECKPOINT_INTERVAL";
    private static final String SCR_CACHE_BYPASS_ENV = "SCR_CACHE_BYPASS";
    private static final String SCR_CACHE_BYPASS_DISABLED = "0";
    private static final String SCR_FLUSH_ENV = "SCR_FLUSH";
    private static final String SCR_FLUSH_EVERY_CHECKPOINT = "1";

    // System properties set by runcompss --checkpointMPI / --ckptMPI_dir / --ckptMPI_interval
    private static final String PROP_MPI_CKPT_ENABLED = "compss.mpi.checkpoint.enabled";
    private static final String PROP_MPI_CKPT_DIR = "compss.mpi.checkpoint.dir";
    private static final String PROP_MPI_CKPT_INTERVAL = "compss.mpi.checkpoint.interval";
    private static final int DEFAULT_CKPT_INTERVAL = 300;

    private static final String ERROR_TARGET_PARAM = "ERROR: MPI Execution doesn't support target parameters";


    // Launcher families MANA has to be configured differently for.
    private enum MpiFamily {
        OPENMPI, HYDRA, SLURM, UNKNOWN
    }


    private static final Map<String, MpiFamily> MPI_FAMILY_CACHE = new ConcurrentHashMap<>();

    MPIDefinition mpiDef;

    private BinaryRunner br;

    private final List<Path> manaPathLinks = new ArrayList<>();


    /**
     * MPI Invoker constructor.
     * 
     * @param context Task execution context.
     * @param invocation Task execution description.
     * @param sandBox Task execution sandbox directory.
     * @param assignedResources Assigned resources.
     * @throws JobExecutionException Error creating the MPI invoker.
     */
    public MPIInvoker(InvocationContext context, Invocation invocation, ExecutionSandbox sandBox,
        InvocationResources assignedResources) throws JobExecutionException {

        super(context, invocation, sandBox, assignedResources);

        // Get method definition properties
        try {
            this.mpiDef = (MPIDefinition) this.invocation.getMethodImplementation().getDefinition();
            this.mpiDef.setRunnerProperties(context.getInstallDir());
        } catch (Exception e) {
            throw new JobExecutionException(
                ERROR_METHOD_DEFINITION + this.invocation.getMethodImplementation().getMethodType(), e);
        }

        // Internal binary runner
        this.br = null;
    }

    private void checkArguments() throws JobExecutionException {
        try {
            mpiDef.checkArguments();
        } catch (IllegalArgumentException e) {
            throw new JobExecutionException(e);
        }
        if (this.invocation.getTarget() != null && this.invocation.getTarget().getValue() != null) {
            throw new JobExecutionException(ERROR_TARGET_PARAM);
        }
    }

    @Override
    public void invokeMethod() throws JobExecutionException {
        checkArguments();

        logger.info("Invoked MPI {} in {}", this.mpiDef.getBinary(), this.context.getHostName());

        // Execute binary
        Object retValue;
        try {
            retValue = runInvocation();
        } catch (InvokeExecutionException iee) {
            throw new JobExecutionException(iee);
        }

        // Close out streams if any
        try {
            if (this.br != null) {
                // Python interpreter for direct access on stream property calls
                String pythonInterpreter = null;
                LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
                if (lp instanceof PythonParams) {
                    PythonParams pp = (PythonParams) lp;
                    pythonInterpreter = pp.getPythonInterpreter();
                }
                this.br.closeStreams(this.invocation.getParams(), pythonInterpreter);
            }
        } catch (StreamCloseException se) {
            logger.error("Exception closing binary streams", se);
            throw new JobExecutionException(se);
        }

        // Update binary results
        for (InvocationParam np : this.invocation.getResults()) {
            if (np.getType() == DataType.FILE_T) {
                serializeBinaryExitValue(np, retValue);
            } else {
                np.setValue(retValue);
                np.setValueClass(retValue.getClass());
            }
        }
    }

    private Object runInvocation() throws InvokeExecutionException {
        // Command similar to
        // mpirun -H COMPSsWorker01,COMPSsWorker02 -n 2 (--bind-to core) exec args

        // --checkpointMPI is the master switch; then @mpi(checkpoint=True) selects MANA and False selects SCR.
        final boolean globalEnabled = Boolean.parseBoolean(System.getProperty(PROP_MPI_CKPT_ENABLED, "false"));
        final boolean useMana = globalEnabled && this.mpiDef.isCheckpoint();
        final boolean useScr = globalEnabled && !this.mpiDef.isCheckpoint();

        final int taskInterval = this.mpiDef.getInterval();
        final String checkpointInterval;
        if (taskInterval > 0) {
            checkpointInterval = String.valueOf(taskInterval);
        } else {
            String globalInterval = System.getProperty(PROP_MPI_CKPT_INTERVAL);
            checkpointInterval = (globalInterval != null && !globalInterval.isEmpty()) ? globalInterval
                : String.valueOf(DEFAULT_CKPT_INTERVAL);
        }

        final int ckptIter = this.mpiDef.getCkptIter();

        final String globalCkptBase = System.getProperty(PROP_MPI_CKPT_DIR);

        // Python interpreter for direct access on stream property calls
        String pythonInterpreter = null;
        LanguageParams lp = this.context.getLanguageParams(COMPSsConstants.Lang.PYTHON);
        if (lp instanceof PythonParams) {
            PythonParams pp = (PythonParams) lp;
            pythonInterpreter = pp.checkCoverageAndGetPythonInterpreter();

        }

        String[] appParams = new String[0];
        if (this.mpiDef.hasParamsString()) {
            appParams =
                BinaryRunner.buildAppParams(this.invocation.getParams(), this.mpiDef.getParams(), pythonInterpreter);
        }

        // Calculate binary-streams redirection
        StdIOStream streamValues = new StdIOStream();
        ArrayList<String> binaryParams = BinaryRunner.createCMDParametersFromValues(this.invocation.getParams(),
            this.invocation.getTarget(), streamValues, pythonInterpreter);

        // MPI Flags
        String mpiFlags = mpiDef.getMpiFlags();
        int numMPIFlags = 0;
        String[] mpiflagsArray = null;
        if (mpiFlags != null && !mpiFlags.isEmpty() && !mpiFlags.equals("[unassigned]")) {
            mpiflagsArray = BinaryRunner.buildAppParams(this.invocation.getParams(), mpiFlags, pythonInterpreter);
            numMPIFlags = mpiflagsArray.length;
        }

        // Prepare command
        final String sandboxPath = this.sandBox.getFolder().getAbsolutePath();
        final long taskId = this.invocation.getTaskId();
        final String manaHome = Paths.get(sandboxPath, "mana_home_" + taskId + "_" + System.nanoTime()).toString();
        final String ckptBase = (globalCkptBase != null && !globalCkptBase.isEmpty()) ? globalCkptBase : sandboxPath;
        final String ckptSubdirPrefix = this.mpiDef.isCheckpoint() ? MANA_CKPT_SUBDIR_PREFIX : SCR_CKPT_SUBDIR_PREFIX;
        final String ckptDir = Paths.get(ckptBase, ckptSubdirPrefix + taskId).toString();
        final String scrRunKey = taskId + "_" + Integer.toHexString(ckptDir.hashCode());
        final String scrCacheRoot = System.getenv(SCR_CACHE_BASE_ENV);
        final String scrCacheBase =
            (scrCacheRoot != null && !scrCacheRoot.isEmpty()) ? scrCacheRoot : DEFAULT_SCR_CACHE_ROOT;
        final String scrCacheDir = Paths.get(scrCacheBase, SCR_CACHE_SUBDIR_PREFIX + scrRunKey).toString();
        final String scrCntlRoot = System.getenv(SCR_CNTL_BASE_ENV);
        final String scrCntlBase =
            (scrCntlRoot != null && !scrCntlRoot.isEmpty()) ? scrCntlRoot : DEFAULT_SCR_CACHE_ROOT;
        final String scrCntlDir = Paths.get(scrCntlBase, SCR_CNTL_SUBDIR_PREFIX + scrRunKey).toString();

        if (useMana) {
            logger.info(
                "[MPI INVOKER] MANA checkpointing active - dir=" + ckptDir + " interval=" + checkpointInterval + "s");
        } else if (useScr) {
            logger.info(
                "[MPI INVOKER] SCR checkpointing active - dir=" + ckptDir + " interval=" + checkpointInterval + "s");
        } else if (this.mpiDef.isCheckpoint() && !globalEnabled) {
            logger.info("[MPI INVOKER] Task has checkpoint=True but --checkpointMPI is not set; skipping MANA.");
        }

        final MpiFamily mpiFamily = useMana ? resolveMpiFamily() : null;

        if (useMana) {
            logger.info("[MPI INVOKER] MANA launcher family: " + mpiFamily);
            try {
                Files.createDirectories(Paths.get(manaHome));
                logger.info("[MPI INVOKER] Ensuring MANA coordinator is ready...");
                ensureManaCoordinator(manaHome);
                logger.info("[MPI INVOKER] MANA coordinator ready for use");
            } catch (IOException e) {
                throw new InvokeExecutionException("ERROR: Could not create MANA home directory", e);
            } catch (InvokeExecutionException e) {
                logger.error("[MPI INVOKER] Failed to setup MANA coordinator: " + e.getMessage(), e);
                throw e;
            }
        }

        boolean isRestart = false;
        if (useMana && Files.exists(Paths.get(ckptDir, "ckpt_rank_0"))) {
            final int expectedRanks =
                parseRanks(this.mpiDef.generateNumberOfProcesses(this.numWorkers, this.computingUnits));
            final String problem = manaCheckpointProblem(Paths.get(ckptDir), expectedRanks);
            if (problem == null) {
                isRestart = true;
                logger.info("[MPI INVOKER] Checkpoint detected at " + ckptDir + ". Configuring RESTART mode.");
            } else {
                logger.warn("[MANA] The checkpoint at " + ckptDir + " is not usable (" + problem
                    + "); starting the task from scratch.");
            }
        }

        if (useMana && !isRestart) {
            try {
                final Path ckptDirPath = Paths.get(ckptDir);
                if (Files.exists(ckptDirPath)) {
                    logger.info("[MPI INVOKER] Cleaning stale checkpoint directory: " + ckptDir);
                    cleanDirectory(ckptDirPath);
                }
                Files.createDirectories(ckptDirPath);
            } catch (IOException e) {
                throw new InvokeExecutionException("ERROR: Could not create MANA checkpoint directory", e);
            }
        }

        if (useScr) {
            // SCR_Init detects the restart itself, inside the application.
            try {
                Files.createDirectories(Paths.get(ckptDir));
                Files.createDirectories(Paths.get(scrCacheDir));
                Files.createDirectories(Paths.get(scrCntlDir));
            } catch (IOException e) {
                throw new InvokeExecutionException("ERROR: Could not create SCR checkpoint/cache/control directory", e);
            }
        }

        if (useMana && mpiFamily == MpiFamily.HYDRA && "1".equals(System.getenv(HYDRA_NO_AUTO_CLEANUP_ENV))) {
            String[] extendedFlags = new String[numMPIFlags + 1];
            if (mpiflagsArray != null) {
                System.arraycopy(mpiflagsArray, 0, extendedFlags, 0, numMPIFlags);
            }
            extendedFlags[numMPIFlags] = HYDRA_NO_AUTO_CLEANUP_FLAG;
            mpiflagsArray = extendedFlags;
            numMPIFlags = mpiflagsArray.length;
        }

        // Everything the checkpoint backend puts between the launcher and the application binary.
        final List<String> ckptPrefix = new ArrayList<>();
        if (useMana) {
            ckptPrefix.add("env");
            if (isRestart) {
                ckptPrefix.add(MANA_CHDIR_FLAG);
                ckptPrefix.add(ckptDir);
            }
            ckptPrefix.add("HOME=" + manaHome);
            ckptPrefix.addAll(manaTransportEnv(mpiFamily));
            if (isRestart) {
                ckptPrefix.add(resolveManaExecutable(MANA_RESTART_CMD));
                ckptPrefix.add(MANA_INTERVAL_FLAG);
                ckptPrefix.add(checkpointInterval);
                ckptPrefix.add(MANA_RESTARTDIR_FLAG);
                ckptPrefix.add(ckptDir);
            } else {
                ckptPrefix.add(resolveManaExecutable(MANA_LAUNCH_CMD));
                ckptPrefix.add(MANA_CKPTDIR_FLAG);
                ckptPrefix.add(ckptDir);
                ckptPrefix.add(MANA_INTERVAL_FLAG);
                ckptPrefix.add(checkpointInterval);
            }
        } else if (useScr) {
            // SCR is a library linked by the application: it is configured through the environment.
            ckptPrefix.add("env");
            ckptPrefix.add(SCR_PREFIX_ENV + "=" + ckptDir);
            ckptPrefix.add(SCR_CACHE_BASE_ENV + "=" + scrCacheDir);
            ckptPrefix.add(SCR_CNTL_BASE_ENV + "=" + scrCntlDir);
            ckptPrefix.add(SCR_CACHE_BYPASS_ENV + "=" + SCR_CACHE_BYPASS_DISABLED);
            final String userFlush = System.getenv(SCR_FLUSH_ENV);
            ckptPrefix.add(SCR_FLUSH_ENV + "="
                + (userFlush != null && !userFlush.isEmpty() ? userFlush : SCR_FLUSH_EVERY_CHECKPOINT));
            ckptPrefix.add(SCR_CHECKPOINT_SECONDS_ENV + "=" + checkpointInterval);
            if (ckptIter > 0) {
                ckptPrefix.add(SCR_CHECKPOINT_INTERVAL_ENV + "=" + ckptIter);
            }
        }

        int cmdLength = NUM_BASE_MPI_ARGS + numMPIFlags + ckptPrefix.size();
        if (useMana && isRestart) {
            // mana_restart replays the checkpointed process: no application binary, no app params.
            cmdLength -= 1;
        } else {
            if (this.mpiDef.hasParamsString()) {
                cmdLength += appParams.length;
            } else {
                cmdLength += binaryParams.size();
            }
        }

        // Check container and its options
        ContainerDescription container = this.mpiDef.getContainer();

        // if the master is launched inside a container
        if (isOnContainer() && container == null) {
            ContainerDescription.ContainerEngine engine =
                ContainerDescription.ContainerEngine.valueOf(System.getenv(COMPSsConstants.COMPSS_CONTAINER_ENGINE));
            String masterImage = System.getenv(COMPSsConstants.MASTER_CONTAINER_IMAGE);
            String contOptions = System.getenv(COMPSsConstants.NESTED_CONTAINER_OPTIONS);
            contOptions = contOptions == null || contOptions.equals("null") ? "" : contOptions;
            contOptions += " --pwd " + this.sandBox.getFolder().getAbsolutePath();
            container = new ContainerDescription(engine, masterImage, contOptions);
        }

        int numOptions = 0;
        String[] options = null;
        if (container != null) {
            // engine, exec_command, options[], image
            cmdLength += 3;
            if (!container.getOptions().isEmpty() && !container.getOptions().equals(Constants.UNASSIGNED)) {
                options =
                    BinaryRunner.buildAppParams(this.invocation.getParams(), container.getOptions(), pythonInterpreter);
                numOptions = options.length;
            }
            // -e COMPSS_CONTAINER=${COMPSS_CONTAINER}
            String compssContainer = System.getenv(COMPSsConstants.COMPSS_CONTAINER);
            if (compssContainer != null && !compssContainer.isEmpty()) {
                numOptions += 2;
            }
            cmdLength += container.getEngine().equals(SINGULARITY) ? numOptions : numOptions + 1;
        }

        if (isOnContainer()) {
            cmdLength++;
        }
        String[] cmd = new String[cmdLength];

        int pos = 0;

        if (isOnContainer()) {
            String script = System.getenv(COMPSsConstants.MPI_RUNNER_SCRIPT);
            cmd[pos++] = script;
        }

        cmd[pos++] = this.mpiDef.getMpiRunner();
        cmd[pos++] = this.mpiDef.getHostsFlag();
        String hostfilePath;
        try {
            hostfilePath =
                this.mpiDef.generateHostsDefinition(this.sandBox.getFolder(), this.hostnames, this.computingUnits);

            cmd[pos++] = hostfilePath;
        } catch (IOException ioe) {
            throw new InvokeExecutionException("ERROR: writting hostfile", ioe);
        }
        cmd[pos++] = "-n";
        cmd[pos++] = this.mpiDef.generateNumberOfProcesses(this.numWorkers, this.computingUnits);

        for (int i = 0; i < numMPIFlags; ++i) {
            cmd[pos++] = mpiflagsArray[i];
        }

        if (container != null) {
            // mpirun -H COMPSsWorker01,COMPSsWorker02 -n 2 <engine> <exec_command> <options> <image> binary args
            cmd[pos++] = container.getEngine().name().toLowerCase();
            if (container.getEngine().equals(SINGULARITY)) {
                cmd[pos++] = "exec";
            } else {
                cmd[pos++] = "run";
                cmd[pos++] = "--rm";
            }
            // Check options
            pos = ContainerInvoker.addContainerOptions(cmd, pos, options);
            // todo: nm: if the env variable is defined, use that
            // -e COMPSS_CONTAINER=${COMPSS_CONTAINER}
            String compssContainer = System.getenv(COMPSsConstants.COMPSS_CONTAINER);
            logger.info("COMPSs docker container: {}", compssContainer);
            if (compssContainer != null && !compssContainer.isEmpty()) {
                cmd[pos++] = "--volumes-from";
                cmd[pos++] = compssContainer;
            }
            cmd[pos++] = container.getImage();
        }

        for (String prefixToken : ckptPrefix) {
            cmd[pos++] = prefixToken;
        }

        if (!(useMana && isRestart)) {
            cmd[pos++] = this.mpiDef.getBinary();
            pos = appendAppParams(cmd, pos, appParams, binaryParams);
        }

        // Prepare environment
        if (this.invocation.isDebugEnabled()) {
            PrintStream outLog = context.getThreadOutStream();
            outLog.println();
            outLog.println("[MPI INVOKER] Begin MPI call to " + this.mpiDef.getBinary());
            outLog.println("[MPI INVOKER] On WorkingDir : " + this.sandBox.getFolder().getAbsolutePath());
            // Debug command
            outLog.print("[MPI INVOKER] MPI CMD: ");
            for (String s : cmd) {
                outLog.print(s + " ");
            }
            outLog.println();
            outLog.println("[MPI INVOKER] MPI STDIN: " + streamValues.getStdIn());
            outLog.println("[MPI INVOKER] MPI STDOUT: " + streamValues.getStdOut());
            outLog.println("[MPI INVOKER] MPI STDERR: " + streamValues.getStdErr());
        }

        if (useMana && !manaCoordinatorAlive(manaHome)) {
            logger.warn("[MANA] Coordinator not reachable right before launch (HOME=" + manaHome + ", contents="
                + listDir(manaHome) + "). Starting a new one.");
            ensureManaCoordinator(manaHome);
        }

        if (useMana) {
            final List<String> appArgs = this.mpiDef.hasParamsString() ? java.util.Arrays.asList(appParams)
                : new ArrayList<String>(binaryParams);
            if (isRestart) {
                linkOriginalPaths(ckptDir, appArgs);
            } else {
                saveLaunchArgs(ckptDir, appArgs, sandboxPath);
            }
        }

        // Launch command
        this.br = new BinaryRunner();
        final Object retValue;
        final ManaWatchdog watchdog =
            useMana
                ? startManaWatchdog(manaHome,
                    parseRanks(this.mpiDef.generateNumberOfProcesses(this.numWorkers, this.computingUnits)))
                : null;
        try {
            retValue = this.br.executeCMD(cmd, streamValues, this.sandBox, this.context.getThreadOutStream(),
                this.context.getThreadErrStream(), null, this.mpiDef.isFailByEV());
        } finally {
            if (watchdog != null) {
                watchdog.shutdown();
            }
            if (useMana) {
                stopManaCoordinators(manaHome);
                removeOriginalPathLinks();
            }
        }

        if (watchdog != null && watchdog.hasFired()) {
            throw new InvokeExecutionException("ERROR: MANA ranks were lost during the execution (see the [MANA] "
                + "watchdog messages in the worker log); the task has to be retried");
        }

        return retValue;
    }

    private int appendAppParams(String[] cmd, int pos, String[] appParams, ArrayList<String> binaryParams) {
        if (this.mpiDef.hasParamsString()) {
            for (String appParam : appParams) {
                cmd[pos++] = appParam;
            }
        } else {
            for (String binParam : binaryParams) {
                cmd[pos++] = binParam;
            }
        }
        return pos;
    }

    /**
     * Environment assignments forcing the MPI transport MANA can actually checkpoint and restore. Empty for Hydra
     * launchers (Intel MPI / MPICH pick their fabric through their own variables) and whenever the user already pinned
     * the OpenMPI transport, so an explicit choice is never overridden.
     */
    private List<String> manaTransportEnv(MpiFamily family) {
        List<String> transportEnv = new ArrayList<>();
        if (family == MpiFamily.HYDRA) {
            return transportEnv;
        }
        String pml = System.getenv(OMPI_PML_ENV);
        String btl = System.getenv(OMPI_BTL_ENV);
        if ((pml != null && !pml.isEmpty()) || (btl != null && !btl.isEmpty())) {
            if (pml != null && pml.contains("ucx")) {
                logger.warn("[MANA] " + OMPI_PML_ENV + "=" + pml + " is set in the environment: MANA cannot "
                    + "checkpoint/restart UCX, use " + OMPI_PML_ENV + "=" + OMPI_PML_OB1);
            } else {
                logger.info("[MANA] " + OMPI_PML_ENV + "/" + OMPI_BTL_ENV
                    + " already set in the environment; leaving the transport selection untouched");
            }
            return transportEnv;
        }
        transportEnv.add(OMPI_PML_ENV + "=" + OMPI_PML_OB1);
        transportEnv.add(OMPI_BTL_ENV + "=" + OMPI_BTL_TCP);
        String osc = System.getenv(OMPI_OSC_ENV);
        if (osc == null || osc.isEmpty()) {
            transportEnv.add(OMPI_OSC_ENV + "=" + OMPI_OSC_NO_UCX);
        }
        String tcpIf = manaTcpInterface();
        if (tcpIf != null) {
            transportEnv.add(OMPI_TCP_IF_INCLUDE_ENV + "=" + tcpIf);
        }
        logger.info("[MANA] Transport for the checkpointed run: " + String.join(" ", transportEnv));
        return transportEnv;
    }

    /**
     * Network interface for OpenMPI's TCP BTL on the MANA path, or null to let OpenMPI choose. The user's own
     * btl_tcp_if_include/exclude always wins; COMPSS_MANA_TCP_IF selects one explicitly (empty: OpenMPI's choice);
     * otherwise the IPoIB interface ib0 is used when it is up, so the traffic goes over the InfiniBand fabric.
     */
    private String manaTcpInterface() {
        String include = System.getenv(OMPI_TCP_IF_INCLUDE_ENV);
        String exclude = System.getenv(OMPI_TCP_IF_EXCLUDE_ENV);
        if ((include != null && !include.isEmpty()) || (exclude != null && !exclude.isEmpty())) {
            return null;
        }
        String requested = System.getenv(MANA_TCP_IF_ENV);
        if (requested != null) {
            return requested.trim().isEmpty() ? null : requested.trim();
        }
        Path operstate = Paths.get("/sys/class/net", DEFAULT_IPOIB_IF, "operstate");
        try {
            if (Files.exists(operstate)
                && "up".equals(new String(Files.readAllBytes(operstate), StandardCharsets.UTF_8).trim())) {
                return DEFAULT_IPOIB_IF;
            }
        } catch (IOException e) {
            logger.debug("[MANA] Could not read " + operstate + ": " + e.getMessage());
        }
        return null;
    }

    /**
     * Determines which launcher family the MPI runner belongs to: srun and the COMPSS_MPIRUN_TYPE configuration answer
     * it for free, otherwise the runner is asked for its version. When none of them tells, the family is UNKNOWN and no
     * launcher-specific option is added.
     */
    private MpiFamily resolveMpiFamily() {
        String runner = this.mpiDef.getMpiRunner();
        if (runner == null || runner.isEmpty()) {
            return MpiFamily.UNKNOWN;
        }
        if (runner.endsWith("srun")) {
            return MpiFamily.SLURM;
        }

        MpiFamily cached = MPI_FAMILY_CACHE.get(runner);
        if (cached != null) {
            return cached;
        }

        MpiFamily family = familyFromMpirunType(System.getenv(COMPSsConstants.COMPSS_MPIRUN_TYPE));
        if (family == null) {
            family = probeMpiFamily(runner);
        }
        if (family == null) {
            logger.warn("[MANA] Could not identify the MPI flavour of '" + runner + "'; no launcher-specific "
                + "options will be added. Set " + COMPSsConstants.COMPSS_MPIRUN_TYPE + " to openmpi, impi, mpich "
                + "or slurm to choose them.");
            family = MpiFamily.UNKNOWN;
        }
        MPI_FAMILY_CACHE.put(runner, family);
        return family;
    }

    private MpiFamily familyFromMpirunType(String type) {
        if (type == null || type.isEmpty()) {
            return null;
        }
        // The variable holds either a type name or the path of a <type>.properties file.
        String name = Paths.get(type).getFileName().toString().toLowerCase();
        if (name.endsWith(".properties")) {
            name = name.substring(0, name.length() - ".properties".length());
        }
        switch (name) {
            case "openmpi":
                return MpiFamily.OPENMPI;
            case "mpich":
            case "impi":
                return MpiFamily.HYDRA;
            case "slurm":
                return MpiFamily.SLURM;
            default:
                return null;
        }
    }

    private MpiFamily probeMpiFamily(String runner) {
        try {
            ProcessBuilder pb = new ProcessBuilder(runner, "--version");
            pb.redirectErrorStream(true);
            Process p = pb.start();
            String version = readStream(p.getInputStream()).toLowerCase();
            p.waitFor();
            if (version.contains("open mpi") || version.contains("open-rte") || version.contains("openrte")) {
                return MpiFamily.OPENMPI;
            }
            if (version.contains("hydra") || version.contains("intel(r) mpi")) {
                return MpiFamily.HYDRA;
            }
            if (version.contains("slurm")) {
                return MpiFamily.SLURM;
            }
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
        } catch (IOException e) {
            logger
                .warn("[MANA] Could not run '" + runner + " --version' to identify the MPI flavour: " + e.getMessage());
        }
        return null;
    }

    private void ensureManaCoordinator(String sandboxPath) throws InvokeExecutionException {
        try {
            String manaCoordinator = resolveManaExecutable(MANA_COORDINATOR_CMD);

            logger.info("[MANA] Using mana_coordinator: " + manaCoordinator);
            logger.info("[MANA] Using sandbox as MANA HOME: " + sandboxPath);

            String slurmJobId = System.getenv("SLURM_JOB_ID");
            String rcName =
                (slurmJobId != null && !slurmJobId.isEmpty()) ? ".mana-slurm-" + slurmJobId + ".rc" : ".mana.rc";
            java.io.File rcFile = new java.io.File(sandboxPath, rcName);

            if (rcFile.exists()) {
                logger.info("[MANA] Removing stale coordinator status file: " + rcFile.getAbsolutePath());
                if (!rcFile.delete()) {
                    logger.warn("[MANA] Could not delete stale status file: " + rcFile.getAbsolutePath());
                }
            }

            stopManaCoordinators(sandboxPath);
            logger.info("[MANA] Starting a fresh coordinator (HOME=" + sandboxPath + ")...");

            ProcessBuilder startPb = new ProcessBuilder(manaCoordinator, "-p", "0");
            startPb.environment().put("HOME", sandboxPath);
            startPb.redirectErrorStream(true);
            Process startProcess = startPb.start();
            String coordinatorOutput = readStream(startProcess.getInputStream());
            int startExit = startProcess.waitFor();

            logger.info("[MANA] Coordinator start output: " + coordinatorOutput);

            if (startExit != 0) {
                logger.error("[MANA] Coordinator failed with exit code " + startExit + ": " + coordinatorOutput);
                throw new InvokeExecutionException("ERROR: Could not start MANA coordinator. Exit code: " + startExit
                    + ". Output: " + coordinatorOutput);
            }

            logger.info("[MANA] Coordinator started, waiting for readiness...");

            int retries = 10;
            int retryCount = 0;
            while (retries-- > 0) {
                retryCount++;
                Thread.sleep(500);
                if (manaCoordinatorAlive(sandboxPath)) {
                    logger.info("[MANA] Coordinator is ready after " + (retryCount * 500) + "ms (HOME contents: "
                        + listDir(sandboxPath) + ")");
                    return;
                }
                logger.info("[MANA] Status check attempt " + retryCount + " failed");
            }
            throw new InvokeExecutionException(
                "ERROR: MANA coordinator did not become ready in time after " + retryCount + " retries");

        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            throw new InvokeExecutionException("ERROR: Could not ensure MANA coordinator", e);
        } catch (IOException e) {
            throw new InvokeExecutionException("ERROR: Could not ensure MANA coordinator", e);
        }
    }

    /** mana_status with the given HOME; true when it exits 0. */
    private boolean manaCoordinatorAlive(String home) throws InvokeExecutionException {
        try {
            ProcessBuilder checkPb = new ProcessBuilder(resolveManaExecutable(MANA_LAUNCHER_CHECK_CMD));
            checkPb.environment().put("HOME", home);
            checkPb.redirectErrorStream(true);
            Process check = checkPb.start();
            readStream(check.getInputStream());
            return check.waitFor() == 0;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return false;
        } catch (IOException e) {
            logger.warn("[MANA] Could not run mana_status: " + e.getMessage());
            return false;
        }
    }

    /**
     * Kills the MANA/DMTCP coordinators of this node started with HOME=home. The coordinator renames itself to
     * "dmtcp_coor:PORT", so it is found through /proc (comm + environment), not by its command line.
     */
    private void stopManaCoordinators(String home) {
        final byte[] needle = ("HOME=" + home + "\0").getBytes(StandardCharsets.UTF_8);
        java.io.File[] procs = new java.io.File("/proc").listFiles();
        if (procs == null) {
            return;
        }
        for (java.io.File p : procs) {
            String pid = p.getName();
            if (!pid.matches("[0-9]+")) {
                continue;
            }
            try {
                String comm = new String(Files.readAllBytes(Paths.get(p.getPath(), "comm")), StandardCharsets.UTF_8);
                if (!comm.startsWith("dmtcp_coor")) {
                    continue;
                }
                byte[] env = Files.readAllBytes(Paths.get(p.getPath(), "environ"));
                if (indexOf(env, needle) < 0) {
                    continue;
                }
                logger.info("[MANA] Stopping coordinator pid " + pid + " (HOME=" + home + ")");
                new ProcessBuilder("kill", "-9", pid).start().waitFor();
            } catch (IOException e) {
                // process gone or not ours: ignore
            } catch (InterruptedException e) {
                Thread.currentThread().interrupt();
                return;
            }
        }
    }

    private static int indexOf(byte[] hay, byte[] needle) {
        outer: for (int i = 0; i + needle.length <= hay.length; i++) {
            if (i > 0 && hay[i - 1] != 0) {
                continue; // only match at the start of an environment entry
            }
            for (int j = 0; j < needle.length; j++) {
                if (hay[i + j] != needle[j]) {
                    continue outer;
                }
            }
            return i;
        }
        return -1;
    }

    private static String listDir(String dir) {
        String[] names = new java.io.File(dir).list();
        return names == null ? "<missing>" : java.util.Arrays.toString(names);
    }

    /** Records the application arguments of this launch, so a later restart can map its file paths. */
    private void saveLaunchArgs(String ckptDir, List<String> args, String sandbox) {
        try {
            Files.write(Paths.get(ckptDir, MANA_ARGS_FILE), args, StandardCharsets.UTF_8);
            Files.write(Paths.get(ckptDir, MANA_SANDBOX_FILE), java.util.Collections.singletonList(sandbox),
                StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.warn("[MANA] Could not save the launch arguments in " + ckptDir + ": " + e.getMessage());
        }
    }

    /**
     * mana_restart replays the process with the file paths of its original launch: for every argument whose path
     * changed, leaves a symlink at the old path pointing to the current file.
     */
    private void linkOriginalPaths(String ckptDir, List<String> args) {
        Path argsFile = Paths.get(ckptDir, MANA_ARGS_FILE);
        if (!Files.exists(argsFile)) {
            logger.warn("[MANA] restart: no " + MANA_ARGS_FILE + " in " + ckptDir
                + "; if the task uses files, it will keep the paths of the original launch");
            return;
        }
        List<String> original;
        try {
            original = Files.readAllLines(argsFile, StandardCharsets.UTF_8);
        } catch (IOException e) {
            logger.warn("[MANA] restart: could not read " + argsFile + ": " + e.getMessage());
            return;
        }
        if (original.size() != args.size()) {
            logger.warn("[MANA] restart: the task now has " + args.size() + " arguments and the checkpoint was "
                + "taken with " + original.size() + "; file paths will not be mapped");
            return;
        }
        final Path originalSandbox = readOriginalSandbox(ckptDir);
        for (int i = 0; i < args.size(); i++) {
            String was = original.get(i);
            String now = args.get(i);
            if (was.equals(now) || !was.startsWith("/") || !now.startsWith("/")) {
                continue; // same path, or not a path at all
            }
            Path wasPath = Paths.get(was);
            Path nowPath = Paths.get(now);
            try {
                if (Files.exists(wasPath) && Files.isSameFile(wasPath, nowPath)) {
                    continue;
                }
                if (Files.isDirectory(wasPath, java.nio.file.LinkOption.NOFOLLOW_LINKS)) {
                    logger.warn("[MANA] restart: " + was + " is a directory; not mapping it");
                    continue;
                }
                if (!mayReplaceOriginalPath(wasPath, originalSandbox)) {
                    logger.warn("[MANA] restart: " + was + " is not in the sandbox of the original launch" + " ("
                        + originalSandbox + "); not replacing it");
                    continue;
                }
                if (wasPath.getParent() != null) {
                    Files.createDirectories(wasPath.getParent());
                }
                Files.deleteIfExists(wasPath);
                Files.createSymbolicLink(wasPath, nowPath);
                this.manaPathLinks.add(wasPath);
                logger.info("[MANA] restart: file of the original launch " + was + " -> " + now);
            } catch (IOException e) {
                logger.warn("[MANA] restart: could not map " + was + " to " + now + ": " + e.getMessage());
            }
        }
    }

    /** Sandbox of the launch that wrote the checkpoint in ckptDir, or null if unknown (older checkpoints). */
    private static Path readOriginalSandbox(String ckptDir) {
        try {
            List<String> lines = Files.readAllLines(Paths.get(ckptDir, MANA_SANDBOX_FILE), StandardCharsets.UTF_8);
            if (!lines.isEmpty() && lines.get(0).startsWith("/")) {
                return Paths.get(lines.get(0)).normalize();
            }
        } catch (IOException e) {
            return null; // no launch_sandbox.txt: checkpoint written before it existed
        }
        return null;
    }

    /**
     * Whether linkOriginalPaths may replace path (delete it and leave a link to this job's file). Only paths inside the
     * sandbox of the original launch, which belongs to COMPSs. Without it (older checkpoints), only paths that do not
     * exist or are already links: a regular file may be the user's own and is never deleted.
     */
    private static boolean mayReplaceOriginalPath(Path path, Path originalSandbox) {
        if (originalSandbox != null) {
            return path.normalize().startsWith(originalSandbox);
        }
        return !Files.exists(path, java.nio.file.LinkOption.NOFOLLOW_LINKS) || Files.isSymbolicLink(path);
    }

    /** Removes the symlinks created by linkOriginalPaths (the real files live in this job's sandbox). */
    private void removeOriginalPathLinks() {
        for (Path link : this.manaPathLinks) {
            try {
                Files.deleteIfExists(link);
            } catch (IOException e) {
                logger.warn("[MANA] Could not remove the symlink " + link + ": " + e.getMessage());
            }
        }
        this.manaPathLinks.clear();
    }

    /** Number of processes of the task, or -1 if it is not a plain number. */
    private static int parseRanks(String nprocs) {
        try {
            return Integer.parseInt(nprocs.trim());
        } catch (RuntimeException e) {
            return -1;
        }
    }

    /**
     * Why the MANA checkpoint in dir cannot be restored, or null if it looks complete: ckpt_rank_0 .. ckpt_rank_N-1
     * without gaps, N equal to the number of processes of the task (when known), and in each of them a finished,
     * non-empty *.dmtcp image. DMTCP writes every image as *.dmtcp.temp and renames it when it is complete, so a
     * checkpoint interrupted half-way leaves ranks without a *.dmtcp (first checkpoint) or with the previous one.
     */
    private static String manaCheckpointProblem(Path dir, int expectedRanks) {
        int found = 0;
        while (Files.isDirectory(dir.resolve("ckpt_rank_" + found))) {
            found++;
        }
        if (expectedRanks > 0 && found != expectedRanks) {
            return found + " rank directories for a task with " + expectedRanks + " processes";
        }
        for (int r = 0; r < found; r++) {
            Path rankDir = dir.resolve("ckpt_rank_" + r);
            boolean image = false;
            try (java.nio.file.DirectoryStream<Path> files = Files.newDirectoryStream(rankDir, "*.dmtcp")) {
                for (Path f : files) {
                    if (Files.isRegularFile(f) && Files.size(f) > 0) {
                        image = true;
                        break;
                    }
                }
            } catch (IOException e) {
                return "cannot read " + rankDir + ": " + e.getMessage();
            }
            if (!image) {
                return "no complete checkpoint image (*.dmtcp) in " + rankDir;
            }
        }
        return null;
    }

    /**
     * Starts this attempt's rank watchdog, unless COMPSS_MANA_WATCHDOG_GRACE is 0. expectedRanks is the number of
     * processes of the task (-n), or -1 if it is not known.
     */
    private ManaWatchdog startManaWatchdog(String home, int expectedRanks) {
        final int grace = watchdogSeconds(MANA_WATCHDOG_GRACE_ENV, DEFAULT_MANA_WATCHDOG_GRACE);
        if (grace <= 0) {
            return null;
        }
        final int startup = watchdogSeconds(MANA_WATCHDOG_STARTUP_ENV, DEFAULT_MANA_WATCHDOG_STARTUP);
        ManaWatchdog watchdog = new ManaWatchdog(home, grace, Math.max(startup, 0), expectedRanks);
        watchdog.start();
        return watchdog;
    }

    private static int watchdogSeconds(String env, int defaultValue) {
        final String value = System.getenv(env);
        if (value != null && !value.isEmpty()) {
            try {
                return Integer.parseInt(value.trim());
            } catch (NumberFormatException e) {
                logger.warn("[MANA] Ignoring " + env + "=" + value + ": not a number");
            }
        }
        return defaultValue;
    }

    /**
     * Ranks connected to the MANA coordinator of home, as {host, real pid}, or null if the coordinator cannot be asked.
     * Its host and port are left in coordinator[0] and coordinator[1].
     */
    private List<String[]> manaConnectedRanks(String home, String[] coordinator) {
        try {
            ProcessBuilder pb = new ProcessBuilder(resolveManaExecutable(MANA_LAUNCHER_CHECK_CMD));
            pb.environment().put("HOME", home);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            String out = readStream(p.getInputStream());
            if (p.waitFor() != 0 || !out.contains("Client List")) {
                return null;
            }
            List<String[]> ranks = new ArrayList<>();
            for (String line : out.split("\n")) {
                String t = line.trim();
                if (t.startsWith("Host:")) {
                    coordinator[0] = t.substring("Host:".length()).trim();
                } else if (t.startsWith("Port:")) {
                    coordinator[1] = t.substring("Port:".length()).trim();
                } else {
                    java.util.regex.Matcher client = MANA_CLIENT_LINE.matcher(line);
                    if (client.find()) {
                        ranks.add(new String[] { client.group(2),
                            client.group(1) });
                    }
                }
            }
            return ranks;
        } catch (InterruptedException e) {
            Thread.currentThread().interrupt();
            return null;
        } catch (IOException | InvokeExecutionException e) {
            return null;
        }
    }

    /** Kills the ranks still connected to the coordinator of home: through it first, then with kill -9 over ssh. */
    private void killManaRanks(String home) throws InterruptedException {
        final String[] coordinator = new String[2];
        List<String[]> ranks = manaConnectedRanks(home, coordinator);
        if (ranks == null || ranks.isEmpty()) {
            return;
        }
        if (coordinator[0] != null && coordinator[1] != null) {
            try {
                runQuietly(java.util.Arrays.asList(resolveManaExecutable(DMTCP_COMMAND_CMD), "--coord-host",
                    coordinator[0], "--coord-port", coordinator[1], "--kill"));
            } catch (InvokeExecutionException e) {
                logger.warn("[MANA] watchdog: " + e.getMessage());
            }
            Thread.sleep(MANA_KILL_WAIT_MS);
            ranks = manaConnectedRanks(home, coordinator);
            if (ranks == null || ranks.isEmpty()) {
                return;
            }
        }
        Map<String, StringBuilder> pidsByHost = new java.util.LinkedHashMap<>();
        for (String[] rank : ranks) {
            pidsByHost.computeIfAbsent(rank[0], h -> new StringBuilder("kill -9")).append(' ').append(rank[1]);
        }
        for (Map.Entry<String, StringBuilder> e : pidsByHost.entrySet()) {
            logger.warn("[MANA] watchdog: " + e.getKey() + ": " + e.getValue());
            runQuietly(java.util.Arrays.asList("ssh", "-o", "BatchMode=yes", "-o", "StrictHostKeyChecking=no",
                e.getKey(), e.getValue().toString()));
        }
    }

    /** Runs a short command, at most 30 s, and logs its output if it fails. */
    private void runQuietly(List<String> command) throws InterruptedException {
        try {
            ProcessBuilder pb = new ProcessBuilder(command);
            pb.redirectErrorStream(true);
            Process p = pb.start();
            if (!p.waitFor(30, java.util.concurrent.TimeUnit.SECONDS)) {
                p.destroyForcibly();
                logger.warn("[MANA] watchdog: timeout running " + command);
            } else if (p.exitValue() != 0) {
                logger.warn("[MANA] watchdog: " + command + " exited with " + p.exitValue() + ": "
                    + readStream(p.getInputStream()).trim());
            }
        } catch (IOException e) {
            logger.warn("[MANA] watchdog: could not run " + command + ": " + e.getMessage());
        }
    }

    private String resolveManaExecutable(String executable) throws InvokeExecutionException {
        logger.info("[MANA] Resolving executable: " + executable);

        final String userHome = System.getProperty("user.home");
        List<Path> candidates = new ArrayList<>();

        String manaHome = System.getenv(MANA_HOME_ENV);
        if (manaHome != null && !manaHome.isEmpty()) {
            candidates.add(Paths.get(manaHome, "bin", executable));
        }

        String manaBin = System.getenv(MANA_BIN_ENV);
        if (manaBin != null && !manaBin.isEmpty()) {
            candidates.add(Paths.get(manaBin, executable));
        }

        String pathEnv = System.getenv("PATH");
        if (pathEnv != null && !pathEnv.isEmpty()) {
            for (String dir : pathEnv.split(java.io.File.pathSeparator)) {
                candidates.add(Paths.get(dir, executable));
            }
        }

        // Last resort: a build in ~/mana (set MANA_HOME or put MANA's bin/ in the PATH instead).
        if (userHome != null && !userHome.isEmpty()) {
            candidates.add(Paths.get(userHome, "mana", "bin", executable));
        }

        for (Path candidate : candidates) {
            if (Files.isExecutable(candidate)) {
                String result = candidate.toAbsolutePath().toString();
                logger.info("[MANA] Found " + executable + " at: " + result);
                return result;
            }
        }

        String errorMsg = "ERROR: Could not find MANA executable '" + executable + "'. Searched in: "
            + (manaHome != null ? manaHome + "/bin" : "MANA_HOME not set") + ", "
            + (manaBin != null ? manaBin : "MANA_BIN not set") + ", " + "PATH, "
            + (userHome != null ? userHome + "/mana/bin" : "(no user home)");
        logger.error("[MANA] " + errorMsg);
        throw new InvokeExecutionException(errorMsg);
    }

    private void cleanDirectory(Path dir) throws IOException {
        try (java.util.stream.Stream<Path> entries = Files.list(dir)) {
            for (Path entry : (Iterable<Path>) entries::iterator) {
                if (Files.isDirectory(entry)) {
                    cleanDirectory(entry);
                    Files.delete(entry);
                } else {
                    Files.delete(entry);
                }
            }
        }
    }

    private String readStream(InputStream is) throws IOException {
        byte[] buffer = new byte[1024];
        int bytesRead;
        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        while ((bytesRead = is.read(buffer)) != -1) {
            baos.write(buffer, 0, bytesRead);
        }
        return new String(baos.toByteArray(), StandardCharsets.UTF_8);
    }

    private boolean isOnContainer() {
        String masterContImage = System.getenv(COMPSsConstants.MASTER_CONTAINER_IMAGE);
        return masterContImage != null && !masterContImage.isEmpty() && this.numWorkers > 1;
    }

    @Override
    public void cancelMethod() {
        logger.debug("Cancelling MPI process");
        if (this.br != null) {
            this.br.cancelProcess();
        }
    }

    @Override
    protected void setEnvironmentVariables() {
        super.setEnvironmentVariables();
        int ppn = mpiDef.getPPN();
        if (logger.isDebugEnabled()) {
            System.out.println("[MPI INVOKER] OVERWRITING COMPSS_NUM_PROCS: " + this.computingUnits);
        }
        System.setProperty(COMPSS_NUM_PROCS, String.valueOf(this.computingUnits));

        if (ppn > 1) {
            int threads = this.computingUnits / ppn;
            System.setProperty(COMPSS_NUM_THREADS, String.valueOf(threads));
            System.setProperty(OMP_NUM_THREADS, String.valueOf(threads));

            // LOG ENV VARS
            if (logger.isDebugEnabled()) {
                System.out.println("[MPI INVOKER] OVERWRITING COMPSS_NUM_THREADS: " + threads);
            }
        }
    }


    /**
     * Watches the ranks of a MANA task through its coordinator. When ranks are lost, or do not all connect at startup,
     * the remaining ones are killed so that the task fails and is retried from its checkpoint.
     */
    private final class ManaWatchdog extends Thread {

        private final String home;
        private final long graceMs;
        private final long startupMs;
        private final int expectedRanks;
        private volatile boolean active = true;
        private volatile boolean fired = false;


        private ManaWatchdog(String home, int graceSeconds, int startupSeconds, int expectedRanks) {
            super("MANA-watchdog");
            setDaemon(true);
            this.home = home;
            this.graceMs = graceSeconds * 1000L;
            this.startupMs = startupSeconds * 1000L;
            this.expectedRanks = expectedRanks;
        }

        private void shutdown() {
            this.active = false;
            interrupt();
        }

        /** True when the watchdog had to kill the ranks: the task must fail whatever mpirun returned. */
        private boolean hasFired() {
            return this.fired;
        }

        @Override
        public void run() {
            final ManaRankCount count =
                new ManaRankCount(this.expectedRanks, this.graceMs, this.startupMs, System.currentTimeMillis());
            final String[] coordinator = new String[2];
            boolean warned = false;
            boolean announced = false;
            try {
                while (this.active) {
                    Thread.sleep(MANA_WATCHDOG_PERIOD_MS);
                    List<String[]> ranks = manaConnectedRanks(this.home, coordinator);
                    if (!this.active || ranks == null) {
                        continue;
                    }
                    final int verdict = count.observe(ranks.size(), System.currentTimeMillis());
                    if (count.isComplete() && !announced) {
                        announced = true;
                        logger
                            .info("[MANA] watchdog: all " + this.expectedRanks + " ranks connected to the coordinator");
                    }
                    if (count.isLosing() && !warned) {
                        logger.warn("[MANA] watchdog: " + ranks.size() + " of " + count.target()
                            + " ranks connected to the coordinator");
                    }
                    warned = count.isLosing();
                    if (verdict == ManaRankCount.LOST) {
                        logger.error("[MANA] watchdog: only " + ranks.size() + " of " + count.target() + " ranks for "
                            + this.graceMs / 1000 + " s and the launcher has not ended the task. Killing the "
                            + "remaining ranks so that the task fails and is retried from its checkpoint.");
                    } else if (verdict == ManaRankCount.STALLED) {
                        logger.error("[MANA] watchdog: only " + ranks.size() + " of the " + this.expectedRanks
                            + " ranks of the task have connected to the coordinator, and none has joined for "
                            + this.startupMs / 1000 + " s: some ranks did not start or could not reach the "
                            + "coordinator. Killing the connected ones so that the task fails and is retried.");
                    } else {
                        continue;
                    }
                    this.fired = true;
                    killManaRanks(this.home);
                    return;
                }
            } catch (InterruptedException e) {
                // shutdown(): the task has ended
            }
        }
    }

    /**
     * Decides from the successive counts of ranks connected to the MANA coordinator whether the task lost ranks (LOST)
     * or not all of them ever connected (STALLED).
     */
    static final class ManaRankCount {

        static final int OK = 0;
        static final int LOST = 1;
        static final int STALLED = 2;

        private final int expected;
        private final long graceMs;
        private final long startupMs;
        private int maxSeen = 0;
        private boolean complete = false;
        private long lostSince = -1;
        private long lastGrowth;


        ManaRankCount(int expected, long graceMs, long startupMs, long now) {
            this.expected = expected;
            this.graceMs = graceMs;
            this.startupMs = startupMs;
            this.lastGrowth = now;
        }

        int observe(int connected, long now) {
            if (connected > this.maxSeen) {
                this.maxSeen = connected;
                this.lastGrowth = now;
            }
            if (this.expected > 0 && connected >= this.expected) {
                this.complete = true;
            }
            if (connected < target()) {
                if (this.lostSince < 0) {
                    this.lostSince = now;
                } else if (now - this.lostSince >= this.graceMs) {
                    return LOST;
                }
            } else {
                this.lostSince = -1;
            }
            if (!this.complete && this.expected > 0 && this.startupMs > 0 && connected > 0
                && now - this.lastGrowth >= this.startupMs) {
                return STALLED;
            }
            return OK;
        }

        int target() {
            return this.complete ? this.expected : this.maxSeen;
        }

        boolean isComplete() {
            return this.complete;
        }

        boolean isLosing() {
            return this.lostSince >= 0;
        }
    }
}
