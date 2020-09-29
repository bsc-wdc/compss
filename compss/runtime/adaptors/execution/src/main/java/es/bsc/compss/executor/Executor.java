/*
 *  Copyright 2002-2019 Barcelona Supercomputing Center (www.bsc.es)
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

package es.bsc.compss.executor;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.executor.external.persistent.PersistentMirror;
import es.bsc.compss.executor.external.piped.PipePair;
import es.bsc.compss.executor.external.piped.PipedMirror;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.types.InvocationResources;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.JavaInvoker;
import es.bsc.compss.invokers.JavaNestedInvoker;
import es.bsc.compss.invokers.OpenCLInvoker;
import es.bsc.compss.invokers.StorageInvoker;
import es.bsc.compss.invokers.binary.BinaryInvoker;
import es.bsc.compss.invokers.binary.COMPSsInvoker;
import es.bsc.compss.invokers.binary.ContainerInvoker;
import es.bsc.compss.invokers.binary.DecafInvoker;
import es.bsc.compss.invokers.binary.MPIInvoker;
import es.bsc.compss.invokers.binary.OmpSsInvoker;
import es.bsc.compss.invokers.external.PythonMPIInvoker;
import es.bsc.compss.invokers.external.persistent.CPersistentInvoker;
import es.bsc.compss.invokers.external.piped.CInvoker;
import es.bsc.compss.invokers.external.piped.PythonInvoker;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.annotations.parameter.OnFailure;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.implementations.BinaryImplementation;
import es.bsc.compss.types.implementations.COMPSsImplementation;
import es.bsc.compss.types.implementations.ContainerImplementation;
import es.bsc.compss.types.implementations.DecafImplementation;
import es.bsc.compss.types.implementations.MPIImplementation;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.OmpSsImplementation;
import es.bsc.compss.types.implementations.OpenCLImplementation;
import es.bsc.compss.types.implementations.PythonMPIImplementation;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.util.TraceEvent;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;
import es.bsc.compss.worker.TimeOutTask;
import es.bsc.wdc.affinity.ThreadAffinity;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.nio.file.AtomicMoveNotSupportedException;
import java.nio.file.FileVisitResult;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.SimpleFileVisitor;
import java.nio.file.StandardCopyOption;
import java.nio.file.attribute.BasicFileAttributes;
import java.util.Collection;
import java.util.Timer;

import org.apache.commons.lang.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Executor implements Runnable {

    // Loggers
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private static final boolean WORKER_DEBUG = LOGGER.isDebugEnabled();
    private static final Logger TIMER_LOGGER = LogManager.getLogger(Loggers.TIMER);

    // Error messages
    private static final String ERROR_OUT_FILES =
        "ERROR: One or more OUT files have not" + " been created by task with Method Definition [";
    private static final String WARN_ATOMIC_MOVE =
        "WARN: AtomicMoveNotSupportedException." + " File cannot be atomically moved. Trying to move without atomic";

    // JVM Flag for timers
    public static final boolean IS_TIMER_COMPSS_ENABLED;

    // Conversion
    private static final int NANO_TO_MS = 1_000_000;

    static {
        // Load timer property
        String isTimerCOMPSsEnabledProperty = System.getProperty(COMPSsConstants.TIMER_COMPSS_NAME);
        IS_TIMER_COMPSS_ENABLED = (isTimerCOMPSsEnabledProperty == null || isTimerCOMPSsEnabledProperty.isEmpty()
            || isTimerCOMPSsEnabledProperty.equals("null")) ? false : Boolean.valueOf(isTimerCOMPSsEnabledProperty);
    }

    // Attached component NIOWorker
    private final InvocationContext context;
    // Attached component Request queue
    protected final ExecutorContext platform;
    // Executor Id
    protected final String id;

    protected boolean isRegistered;
    protected PipePair cPipes;
    protected PipePair pyPipes;
    private Timer timer;
    protected InvocationResources previousAllocation;


    /**
     * Instantiates a new Executor.
     *
     * @param context Invocation context
     * @param platform Executor context (Execution Platform
     * @param executorId Executor Identifier
     */
    public Executor(InvocationContext context, ExecutorContext platform, String executorId) {
        LOGGER.info("Executor init");
        this.context = context;
        this.platform = platform;
        this.id = executorId;
        this.isRegistered = false;
        this.timer = new Timer("Timer executor" + this.getId());
    }

    /**
     * Starts the executor execution.
     */
    public void start() {
        // Nothing to do since everything is deleted in each task execution
        LOGGER.info("Executor started");
    }

    /**
     * Thread main code which enables the request processing.
     */
    @Override
    public void run() {
        start();

        // Main loop to process requests
        processRequests();

        // Close language specific properties
        finish();
    }

    /**
     * Stop executor.
     */
    public void finish() {
        // Nothing to do since everything is deleted in each task execution
        closeAssignedResourcesEvents();
        LOGGER.info("Executor finished");
        Collection<ExecutionPlatformMirror<?>> mirrors = platform.getMirrors();
        for (ExecutionPlatformMirror<?> mirror : mirrors) {
            mirror.unregisterExecutor(id);
        }

    }

    /**
     * Returns the executor id.
     *
     * @return executor id
     */
    public String getId() {
        return this.id;
    }

    private void processRequests() {
        Execution execution;
        while (true) {
            execution = this.platform.getJob(); // Get tasks until there are no more tasks pending
            if (execution == null) {
                LOGGER.error("ERROR: Execution is null!!!!!");
            }
            if (execution.getInvocation() == null) {
                LOGGER.debug("Dequeued job is null");
                break;
            }
            Invocation invocation = execution.getInvocation();

            if (WORKER_DEBUG) {
                LOGGER.debug("Dequeuing job " + invocation.getJobId());
            }

            Exception e = execute(invocation);
            boolean success = (e == null);

            if (WORKER_DEBUG) {
                LOGGER.debug("Job " + invocation.getJobId() + " finished (success: " + success + ")");
            }

            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof COMPSsException) {
                e = (COMPSsException) rootCause;
            }

            if (e instanceof COMPSsException) {
                execution.notifyEnd((COMPSsException) e, success);
            } else {
                execution.notifyEnd(null, success);
            }
        }
    }

    private Exception execute(Invocation invocation) {
        if (invocation.getMethodImplementation().getMethodType() == MethodType.METHOD
            && invocation.getLang() != Lang.JAVA && invocation.getLang() != Lang.PYTHON
            && invocation.getLang() != Lang.C) {

            LOGGER.error("Incorrect language " + invocation.getLang() + " in job " + invocation.getJobId());
            // Print to the job.err file
            System.err.println("Incorrect language " + invocation.getLang() + " in job " + invocation.getJobId());
            return null;
        }

        return executeTaskWrapper(invocation);
    }

    private Exception executeTaskWrapper(Invocation invocation) {
        if (Tracer.extraeEnabled()) {
            int nCPUs = ((MethodResourceDescription) invocation.getRequirements()).getTotalCPUComputingUnits();
            Tracer.emitEvent(nCPUs, Tracer.getCPUCountEventsType());
            int nGPUs = ((MethodResourceDescription) invocation.getRequirements()).getTotalGPUComputingUnits();
            Tracer.emitEvent(nGPUs, Tracer.getGPUCountEventsType());
            int memory = (int) ((MethodResourceDescription) invocation.getRequirements()).getMemorySize();
            if (memory < 0) {
                memory = 0;
            }
            Tracer.emitEvent(memory, Tracer.getMemoryEventsType());
            int diskBW = ((MethodResourceDescription) invocation.getRequirements()).getStorageBW();
            if (diskBW < 0) {
                diskBW = 0;
            }
            Tracer.emitEvent(diskBW, Tracer.getDiskBWEventsType());
            int taskType = invocation.getMethodImplementation().getMethodType().ordinal() + 1;
            Tracer.emitEvent(taskType, Tracer.getTaskTypeEventsType());
            Tracer.emitEvent(TraceEvent.TASK_RUNNING.getId(), TraceEvent.TASK_RUNNING.getType());
        }

        long timeTotalStart = 0L;
        if (IS_TIMER_COMPSS_ENABLED) {
            timeTotalStart = System.nanoTime();
        }

        int jobId = invocation.getJobId();
        TaskWorkingDir twd = null;
        // Flag to check if exception was unbinding files
        boolean failureUnbindingFiles = false;
        boolean areResourcesAcquired = false;
        long timeUnbindOriginalFilesStart = 0L;
        try {
            // Bind computing units
            LOGGER.debug("Asssigning resources for Job " + jobId);
            long timeAssignResourcesStart = 0L;
            if (IS_TIMER_COMPSS_ENABLED) {
                timeAssignResourcesStart = System.nanoTime();
            }
            final InvocationResources assignedResources =
                this.platform.acquireResources(jobId, invocation.getRequirements(), previousAllocation);
            previousAllocation = assignedResources;
            if (Tracer.extraeEnabled()) {
                emitAssignedResourcesEvents(assignedResources);
            }
            if (assignedResources.getAssignedCPUs() != null && assignedResources.getAssignedCPUs().length > 0) {

                try {
                    ThreadAffinity.setCurrentThreadAffinity(assignedResources.getAssignedCPUs());
                } catch (Exception e) {
                    LOGGER.warn("Error setting affinity for Job " + jobId, e);
                }
            }
            areResourcesAcquired = true;
            if (IS_TIMER_COMPSS_ENABLED) {
                final long timeAssignResourcesEnd = System.nanoTime();
                final float timeAssignResourcesElapsed =
                    (timeAssignResourcesEnd - timeAssignResourcesStart) / (float) NANO_TO_MS;
                TIMER_LOGGER
                    .debug("[TIMER] Assign resources for job " + jobId + ": " + timeAssignResourcesElapsed + " ms");
            }

            // Set the Task working directory
            LOGGER.debug("Creating task sandbox for Job " + jobId);
            long timeSandboxStart = 0L;
            if (IS_TIMER_COMPSS_ENABLED) {
                timeSandboxStart = System.nanoTime();
            }
            twd = createTaskSandbox(invocation);
            if (IS_TIMER_COMPSS_ENABLED) {
                final long timeSandboxEnd = System.nanoTime();
                final float timeSandboxElapsed = (timeSandboxEnd - timeSandboxStart) / (float) NANO_TO_MS;
                TIMER_LOGGER.debug("[TIMER] Create sandbox for job " + jobId + ": " + timeSandboxElapsed + " ms");
            }

            // Bind files to task sandbox working dir
            LOGGER.debug("Binding renamed files to sandboxed original names for Job " + jobId);
            long timeBindOriginalFilesStart = 0L;
            if (IS_TIMER_COMPSS_ENABLED) {
                timeBindOriginalFilesStart = System.nanoTime();
            }
            // todo: first trace
            bindOriginalFilenamesToRenames(invocation, twd.getWorkingDir());
            if (IS_TIMER_COMPSS_ENABLED) {
                final long timeBindOriginalFilesEnd = System.nanoTime();
                final float timeBindOriginalFilesElapsed =
                    (timeBindOriginalFilesEnd - timeBindOriginalFilesStart) / (float) NANO_TO_MS;
                TIMER_LOGGER.debug(
                    "[TIMER] Bind original files for job " + jobId + ": " + timeBindOriginalFilesElapsed + " ms");
            }

            // Execute task
            LOGGER.debug("Executing task invocation for Job " + jobId);
            long timeExecTaskStart = 0L;
            if (IS_TIMER_COMPSS_ENABLED) {
                timeExecTaskStart = System.nanoTime();
            }
            TimeOutTask timerTask = null;
            long timeout = invocation.getTimeOut();
            if (timeout > 0) {
                timerTask = new TimeOutTask(invocation.getTaskId());
                this.timer.schedule(timerTask, timeout);
            }
            executeTask(assignedResources, invocation, twd.getWorkingDir());
            if (timerTask != null) {
                timerTask.cancel();
            }
            if (IS_TIMER_COMPSS_ENABLED) {
                final long timeExecTaskEnd = System.nanoTime();
                final float timeExecTaskElapsed = (timeExecTaskEnd - timeExecTaskStart) / (float) NANO_TO_MS;
                TIMER_LOGGER.info("[TIMER] Execute job " + jobId + ": " + timeExecTaskElapsed + " ms");
            }
            failureUnbindingFiles = true;
            // Unbind files from task sandbox working dir
            LOGGER.debug("Removing renamed files to sandboxed original names for Job " + jobId);
            if (IS_TIMER_COMPSS_ENABLED) {
                timeUnbindOriginalFilesStart = System.nanoTime();
            }
            // todo: second trace
            unbindOriginalFileNamesToRenames(invocation, false);
        } catch (Exception e) {
            LOGGER.error(e.getMessage(), e);
            // Writing in the task .err/.out
            this.context.getThreadOutStream().println("Exception executing task " + e.getMessage());
            e.printStackTrace(this.context.getThreadErrStream());
            if (!failureUnbindingFiles) {
                LOGGER.debug("Removing renamed files to sandboxed original names for Job " + jobId);
                if (IS_TIMER_COMPSS_ENABLED) {
                    timeUnbindOriginalFilesStart = System.nanoTime();
                }
                try {
                    unbindOriginalFileNamesToRenames(invocation, true);
                } catch (IOException | JobExecutionException ex) {
                    LOGGER.warn("Another exception after unbinding files: " + ex.getMessage(), ex);
                    this.context.getThreadOutStream().println("Another exception unbinding files: " + ex.getMessage());
                    ex.printStackTrace(this.context.getThreadErrStream());
                }
            }
            return e;
        } finally {
            if (IS_TIMER_COMPSS_ENABLED) {
                final long timeUnbindOriginalFilesEnd = System.nanoTime();
                final float timeUnbindOriginalFilesElapsed =
                    (timeUnbindOriginalFilesEnd - timeUnbindOriginalFilesStart) / (float) NANO_TO_MS;
                TIMER_LOGGER.debug(
                    "[TIMER] Unbind original files for job " + jobId + ": " + timeUnbindOriginalFilesElapsed + " ms");
            }

            // Check job output files
            LOGGER.debug("Checking generated files for Job " + jobId);
            long timeCheckOutputFilesStart = 0L;
            if (IS_TIMER_COMPSS_ENABLED) {
                timeCheckOutputFilesStart = System.nanoTime();
            }
            try {
                // todo: third trace here
                checkJobFiles(invocation);
            } catch (JobExecutionException e) {
                LOGGER.error(e.getMessage(), e);
                // Writing in the task .err/.out
                this.context.getThreadOutStream().println("Exception executing task " + e.getMessage());
                e.printStackTrace(this.context.getThreadErrStream());
                return e;
            } finally {
                if (IS_TIMER_COMPSS_ENABLED) {
                    final long timeCheckOutputFilesEnd = System.nanoTime();
                    final float timeCheckOutputFilesElapsed =
                        (timeCheckOutputFilesEnd - timeCheckOutputFilesStart) / (float) NANO_TO_MS;
                    TIMER_LOGGER.debug(
                        "[TIMER] Check output files for job " + jobId + ": " + timeCheckOutputFilesElapsed + " ms");
                }

                // Always release the binded computing units
                if (areResourcesAcquired) {
                    LOGGER.debug("Release binded resources for Job " + jobId);
                    long timeUnassignResourcesStart = 0L;
                    if (IS_TIMER_COMPSS_ENABLED) {
                        timeUnassignResourcesStart = System.nanoTime();
                    }
                    this.platform.releaseResources(jobId);
                    if (IS_TIMER_COMPSS_ENABLED) {
                        final long timeUnassignResourcesEnd = System.nanoTime();
                        final float timeUnassignResourcesElapsed =
                            (timeUnassignResourcesEnd - timeUnassignResourcesStart) / (float) NANO_TO_MS;
                        TIMER_LOGGER.debug("[TIMER] Unassign resources for job " + jobId + ": "
                            + timeUnassignResourcesElapsed + " ms");
                    }
                }

                // Always clean the task sandbox working dir
                LOGGER.debug("Cleaning task sandbox for Job " + jobId);
                long timeCleanSandboxStart = 0L;
                if (IS_TIMER_COMPSS_ENABLED) {
                    timeCleanSandboxStart = System.nanoTime();
                }
                cleanTaskSandbox(twd);
                if (IS_TIMER_COMPSS_ENABLED) {
                    final long timeCleanSandboxEnd = System.nanoTime();
                    final float timeCleanSandboxElapsed =
                        (timeCleanSandboxEnd - timeCleanSandboxStart) / (float) NANO_TO_MS;
                    TIMER_LOGGER
                        .debug("[TIMER] Clean sandbox for job " + jobId + ": " + timeCleanSandboxElapsed + " ms");
                }

                // Always end task tracing
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getCPUCountEventsType());
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getGPUCountEventsType());
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getMemoryEventsType());
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getDiskBWEventsType());
                    Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTaskTypeEventsType());
                    Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.TASK_RUNNING.getType());
                }
                // Write timer if needed
                if (IS_TIMER_COMPSS_ENABLED) {
                    final long timeTotalEnd = System.nanoTime();
                    final float timeTotalElapsed = (timeTotalEnd - timeTotalStart) / (float) NANO_TO_MS;
                    TIMER_LOGGER.info("[TIMER] Total time for job " + jobId + ": " + timeTotalElapsed + " ms");
                }
            }
        }
        // Any exception thrown, successful execution
        return null;
    }

    private void executeTask(InvocationResources assignedResources, Invocation invocation, File taskSandboxWorkingDir)
        throws Exception {

        /* Register outputs **************************************** */
        String streamsPath = this.context.getStandardStreamsPath(invocation);
        this.context.registerOutputs(streamsPath);
        PrintStream out = this.context.getThreadOutStream();

        /* TRY TO PROCESS THE TASK ******************************** */
        if (invocation.isDebugEnabled()) {
            out.println("[EXECUTOR] executeTask - Begin task execution");
        }
        try {
            Invoker invoker = null;
            switch (invocation.getMethodImplementation().getMethodType()) {
                case METHOD:
                    invoker = selectNativeMethodInvoker(invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case CONTAINER:
                    invoker = new ContainerInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case BINARY:
                    invoker = new BinaryInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case PYTHON_MPI:
                    invoker = new PythonMPIInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case MPI:
                    invoker = new MPIInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case COMPSs:
                    invoker = new COMPSsInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case DECAF:
                    invoker = new DecafInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case MULTI_NODE:
                    invoker = selectNativeMethodInvoker(invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case OMPSS:
                    invoker = new OmpSsInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
                case OPENCL:
                    invoker = new OpenCLInvoker(this.context, invocation, taskSandboxWorkingDir, assignedResources);
                    break;
            }
            this.platform.registerRunningJob(invocation.getJobId(), invoker);
            invoker.processTask();
        } catch (Exception jee) {
            out.println("[EXECUTOR] executeTask - Error in task execution");
            PrintStream err = this.context.getThreadErrStream();
            err.println("[EXECUTOR] executeTask - Error in task execution");
            if (invocation.getOnFailure() != OnFailure.RETRY) {
                createEmptyFile(invocation);
            }
            jee.printStackTrace(err);
            throw jee;
        } finally {
            if (invocation.isDebugEnabled()) {
                out.println("[EXECUTOR] executeTask - End task execution");
            }
            this.platform.unregisterRunningJob(invocation.getJobId());
            this.context.unregisterOutputs();
        }
    }

    private void emitAssignedResourcesEvents(InvocationResources assignedResources) {
        if (assignedResources != null) {
            int[] cpus = assignedResources.getAssignedCPUs();
            if (cpus != null && cpus.length > 0) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTasksCPUAffinityEventsType());
                Tracer.emitEvent(cpus[0] + 1, Tracer.getTasksCPUAffinityEventsType());
            }
            int[] gpus = assignedResources.getAssignedGPUs();
            if (gpus != null && gpus.length > 0) {
                Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTasksGPUAffinityEventsType());
                Tracer.emitEvent(gpus[0] + 1, Tracer.getTasksGPUAffinityEventsType());
            }
        }

    }

    private void closeAssignedResourcesEvents() {
        Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTasksCPUAffinityEventsType());
        Tracer.emitEvent(Tracer.EVENT_END, Tracer.getTasksGPUAffinityEventsType());
    }

    /**
     * Creates a sandbox for a task.
     *
     * @param invocation task description
     * @return Sandbox dir
     * @throws IOException Error creating sandbox
     */
    private TaskWorkingDir createTaskSandbox(Invocation invocation) throws IOException {
        // Check if an specific working dir is provided
        String specificWD = null;
        switch (invocation.getMethodImplementation().getMethodType()) {
            case CONTAINER:
                ContainerImplementation contImpl = (ContainerImplementation) invocation.getMethodImplementation();
                specificWD = contImpl.getWorkingDir();
                break;
            case BINARY:
                BinaryImplementation binaryImpl = (BinaryImplementation) invocation.getMethodImplementation();
                specificWD = binaryImpl.getWorkingDir();
                break;
            case MPI:
                MPIImplementation mpiImpl = (MPIImplementation) invocation.getMethodImplementation();
                specificWD = mpiImpl.getWorkingDir();
                break;
            case PYTHON_MPI:
                PythonMPIImplementation nativeMPIImpl = (PythonMPIImplementation) invocation.getMethodImplementation();
                specificWD = nativeMPIImpl.getWorkingDir();
                break;
            case COMPSs:
                COMPSsImplementation compssImpl = (COMPSsImplementation) invocation.getMethodImplementation();
                specificWD = compssImpl.getWorkingDir() + File.separator + compssImpl.getParentAppId() + File.separator
                    + "compss_job_" + invocation.getJobId() + "_" + invocation.getHistory().name();
                break;
            case MULTI_NODE:
                // It is executed as a regular native method
                specificWD = null;
                break;
            case DECAF:
                DecafImplementation decafImpl = (DecafImplementation) invocation.getMethodImplementation();
                specificWD = decafImpl.getWorkingDir();
                break;
            case OMPSS:
                OmpSsImplementation ompssImpl = (OmpSsImplementation) invocation.getMethodImplementation();
                specificWD = ompssImpl.getWorkingDir();
                break;
            case OPENCL:
                OpenCLImplementation openclImpl = (OpenCLImplementation) invocation.getMethodImplementation();
                specificWD = openclImpl.getWorkingDir();
                break;
            case METHOD:
                specificWD = null;
                break;
        }
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.CREATING_TASK_SANDBOX.getId(), TraceEvent.CREATING_TASK_SANDBOX.getType());
        }
        TaskWorkingDir taskWD;
        if (specificWD != null && !specificWD.isEmpty() && !specificWD.equals(Constants.UNASSIGNED)) {
            // Binary has an specific working dir, set it
            File workingDir = new File(specificWD);
            taskWD = new TaskWorkingDir(workingDir, true);

            // Create structures
            Files.createDirectories(workingDir.toPath());
        } else {
            // No specific working dir provided, set default sandbox
            String completePath =
                this.context.getWorkingDir() + "sandBox" + File.separator + "job_" + invocation.getJobId();
            File workingDir = new File(completePath);
            taskWD = new TaskWorkingDir(workingDir, false);

            // Clean-up previous versions if any
            if (workingDir.exists()) {
                LOGGER.debug("Deleting folder " + workingDir.toString());
                if (!workingDir.delete()) {
                    LOGGER.warn("Cannot delete working dir folder: " + workingDir.toString());
                }
            }

            // Create structures
            Files.createDirectories(workingDir.toPath());
        }
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.CREATING_TASK_SANDBOX.getType());
        }
        return taskWD;
    }

    private void cleanTaskSandbox(TaskWorkingDir twd) {
        if (twd != null && !twd.isSpecific()) {
            // Only clean task sandbox if it is not specific
            File workingDir = twd.getWorkingDir();
            if (workingDir != null && workingDir.exists() && workingDir.isDirectory()) {
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(TraceEvent.REMOVING_TASK_SANDBOX.getId(),
                        TraceEvent.REMOVING_TASK_SANDBOX.getType());
                }
                try {
                    LOGGER.debug("Deleting sandbox " + workingDir.toPath());
                    deleteDirectory(workingDir);
                } catch (IOException e) {
                    LOGGER.warn("Error deleting sandbox " + e.getMessage(), e);
                }
                if (Tracer.extraeEnabled()) {
                    Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.REMOVING_TASK_SANDBOX.getType());
                }
            }

        }
    }

    /**
     * Check whether file1 corresponds to a file with a higher version than file2.
     *
     * @param file1 first file name
     * @param file2 second file name
     * @return True if file1 has a higher version. False otherwise (This includes the case where the name file's format
     *         is not correct)
     */
    private boolean isMajorVersion(String file1, String file2) {
        String[] version1array = file1.split("_")[0].split("v");
        String[] version2array = file2.split("_")[0].split("v");
        if (version1array.length < 2 || version2array.length < 2) {
            return false;
        }
        Integer version1int = null;
        Integer version2int = null;
        try {
            version1int = Integer.parseInt(version1array[1]);
            version2int = Integer.parseInt(version2array[1]);
        } catch (NumberFormatException e) {
            return false;
        }
        if (version1int > version2int) {
            return true;
        } else {
            return false;
        }
    }

    /**
     * Create symbolic links from files with the original name in task sandbox to the renamed file.
     *
     * @param invocation task description
     * @param sandbox created sandbox
     * @throws IOException returns exception is a problem occurs during creation
     */
    private void bindOriginalFilenamesToRenames(Invocation invocation, File sandbox) throws IOException {
        for (InvocationParam param : invocation.getParams()) {
            if (!param.isKeepRename()) {
                bindOriginalFilenameToRenames(param, sandbox);
            } else {
                // collection should enter here
                String renamedFilePath = (String) param.getValue();
                LOGGER.debug("Parameter keeps rename: " + renamedFilePath);
                param.setRenamedName(renamedFilePath);
                param.setOriginalName(renamedFilePath);
            }
        }
        if (invocation.getTarget() != null) {
            LOGGER.debug("Invocation has non-null target");
            InvocationParam param = invocation.getTarget();
            if (!param.isKeepRename()) {
                bindOriginalFilenameToRenames(param, sandbox);
            } else {
                String renamedFilePath = (String) param.getValue();
                LOGGER.debug("Parameter keeps rename: " + renamedFilePath);
                param.setRenamedName(renamedFilePath);
                param.setOriginalName(renamedFilePath);
            }
        }
        for (InvocationParam param : invocation.getResults()) {
            if (!param.isKeepRename()) {
                bindOriginalFilenameToRenames(param, sandbox);
            } else {
                String renamedFilePath = (String) param.getValue();
                LOGGER.debug("Parameter keeps rename: " + renamedFilePath);
                param.setRenamedName(renamedFilePath);
                param.setOriginalName(renamedFilePath);
            }
        }
    }

    private void bindOriginalFilenameToRenames(InvocationParam param, File sandbox) throws IOException {

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.BIND_ORIG_NAME.getId(), TraceEvent.BIND_ORIG_NAME.getType());
        }

        if (param.getType().equals(DataType.COLLECTION_T)) {
            // do not enter here
            @SuppressWarnings("unchecked")
            InvocationParamCollection<InvocationParam> cp = (InvocationParamCollection<InvocationParam>) param;
            for (InvocationParam p : cp.getCollectionParameters()) {
                bindOriginalFilenameToRenames(p, sandbox);
            }
        } else {
            if (param.getType().equals(DataType.FILE_T)) {
                String renamedFilePath = (String) param.getValue();
                File renamedFile = new File(renamedFilePath);
                param.setRenamedName(renamedFilePath);
                if (renamedFile.getName().equals(param.getOriginalName())) {
                    param.setOriginalName(renamedFilePath);
                } else {
                    String inSandboxPath = sandbox.getAbsolutePath() + File.separator + param.getOriginalName();
                    LOGGER.debug("Setting Original Name to " + inSandboxPath);
                    LOGGER.debug("Renamed File Path is " + renamedFilePath);
                    param.setOriginalName(inSandboxPath);
                    param.setValue(inSandboxPath);
                    File inSandboxFile = new File(inSandboxPath);
                    if (renamedFile.exists()) {
                        LOGGER.debug("File exists");
                        // IN or INOUT File creating a symbolic link
                        if (!inSandboxFile.exists()) {
                            LOGGER.debug(
                                "Creating symlink " + inSandboxFile.toPath() + " pointing to " + renamedFile.toPath());
                            Files.createSymbolicLink(inSandboxFile.toPath(), renamedFile.toPath());
                        } else {
                            if (Files.isSymbolicLink(inSandboxFile.toPath())) {
                                Path oldRenamed = Files.readSymbolicLink(inSandboxFile.toPath());
                                LOGGER.debug("Checking if " + renamedFile.getName() + " is equal to "
                                    + oldRenamed.getFileName().toString());
                                if (isMajorVersion(renamedFile.getName(), oldRenamed.getFileName().toString())) {
                                    Files.delete(inSandboxFile.toPath());
                                    Files.createSymbolicLink(inSandboxFile.toPath(), renamedFile.toPath());
                                }
                            }
                        }
                    }
                }
            }
        }
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.BIND_ORIG_NAME.getType());
        }
    }

    /**
     * Undo symbolic links and renames done with the original names in task sandbox to the renamed file.
     *
     * @param invocation task description
     * @throws IOException Exception with file operations
     * @throws JobExecutionException Exception unbinding original names to renamed names
     */
    private void unbindOriginalFileNamesToRenames(Invocation invocation, boolean alreadyFailed)
        throws IOException, JobExecutionException {
        String message = null;
        boolean failure = false;
        for (InvocationParam param : invocation.getParams()) {
            if (!param.isKeepRename()) {
                try {
                    // todo: second one is actually here
                    unbindOriginalFilenameToRename(param, invocation);
                } catch (JobExecutionException e) {
                    if (!failure) {
                        message = e.getMessage();
                    } else {
                        message = message.concat("\n" + e.getMessage());
                    }
                    failure = true;
                }
            }
        }
        if (invocation.getTarget() != null) {
            if (!invocation.getTarget().isKeepRename()) {
                try {
                    unbindOriginalFilenameToRename(invocation.getTarget(), invocation);
                } catch (JobExecutionException e) {
                    if (!failure) {
                        message = e.getMessage();
                    } else {
                        message = message.concat("\n" + e.getMessage());
                    }
                    failure = true;
                }
            }
        }
        for (InvocationParam param : invocation.getResults()) {
            if (!param.isKeepRename()) {
                try {
                    unbindOriginalFilenameToRename(param, invocation);
                } catch (JobExecutionException e) {
                    if (!failure) {
                        message = e.getMessage();
                    } else {
                        message = message.concat("\n" + e.getMessage());
                    }
                    failure = true;
                }
            }
        }
        if (failure && !alreadyFailed) {
            throw new JobExecutionException(message);
        }
    }

    private void unbindOriginalFilenameToRename(InvocationParam param, Invocation invocation)
        throws IOException, JobExecutionException {

        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(TraceEvent.UNBIND_ORIG_NAME.getId(), TraceEvent.UNBIND_ORIG_NAME.getType());
        }
        if (param.getType().equals(DataType.COLLECTION_T)) {
            @SuppressWarnings("unchecked")
            InvocationParamCollection<InvocationParam> cp = (InvocationParamCollection<InvocationParam>) param;
            for (InvocationParam p : cp.getCollectionParameters()) {
                unbindOriginalFilenameToRename(p, invocation);
            }
        } else {
            if (param.getType().equals(DataType.FILE_T)) {
                String inSandboxPath = param.getOriginalName();
                String renamedFilePath = param.getRenamedName();

                LOGGER.debug("Treating file " + inSandboxPath);
                File inSandboxFile = new File(inSandboxPath);
                String originalFileName = inSandboxFile.getName();
                if (!inSandboxPath.equals(renamedFilePath)) {
                    File renamedFile = new File(renamedFilePath);
                    if (renamedFile.exists()) {
                        // IN, INOUT
                        if (inSandboxFile.exists()) {
                            if (Files.isSymbolicLink(inSandboxFile.toPath())) {
                                // If a symbolic link is created remove it
                                LOGGER.debug("Deleting symlink " + inSandboxFile.toPath());
                                Files.delete(inSandboxFile.toPath());
                            } else {
                                // Rewrite inout param by moving the new file to the renaming
                                LOGGER.debug("Moving from " + inSandboxFile.toPath() + " to " + renamedFile.toPath());
                                Files.delete(renamedFile.toPath());
                                move(inSandboxFile.toPath(), renamedFile.toPath());
                            }
                        } else {
                            // Both files exist and are updated
                            LOGGER.debug("Repeated data for " + inSandboxPath + ". Nothing to do");
                        }
                    } else { // OUT
                        if (inSandboxFile.exists()) {
                            if (Files.isSymbolicLink(inSandboxFile.toPath())) {
                                // Unexpected case
                                String msg = "ERROR: Unexpected case. A Problem occurred with File " + inSandboxPath
                                    + ". Either this file or the original name " + renamedFilePath + " do not exist.";
                                LOGGER.error(msg);
                                System.err.println(msg);

                                param.setValue(renamedFilePath);
                                param.setOriginalName(originalFileName);

                                throw new JobExecutionException(msg);
                            } else {
                                // If an output file is created move to the renamed path (OUT Case)
                                move(inSandboxFile.toPath(), renamedFile.toPath());
                            }
                        } else {
                            // Unexpected case (except for C binding when not serializing outputs)
                            if (invocation.getOnFailure() != OnFailure.RETRY) {
                                LOGGER.debug("Generating empty renamed file (" + renamedFilePath
                                    + ") for on_failure management");
                                renamedFile.createNewFile();

                            }

                            param.setValue(renamedFilePath);
                            param.setOriginalName(originalFileName);

                            // Error output file does not exist
                            String msg = "WARN: Output file " + inSandboxFile.toPath() + " does not exist";
                            throw new JobExecutionException(msg);

                        }
                    }
                }
                param.setValue(renamedFilePath);
                param.setOriginalName(originalFileName);
            }
        }
        if (Tracer.extraeEnabled()) {
            Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.UNBIND_ORIG_NAME.getType());
        }
    }

    private void move(Path origFilePath, Path renamedFilePath) throws IOException {
        LOGGER.debug("Moving " + origFilePath.toString() + " to " + renamedFilePath.toString());
        try {
            Files.move(origFilePath, renamedFilePath, StandardCopyOption.ATOMIC_MOVE);
        } catch (AtomicMoveNotSupportedException amnse) {
            LOGGER.warn(WARN_ATOMIC_MOVE);
            Files.move(origFilePath, renamedFilePath);
        }
    }

    private void checkJobFiles(Invocation invocation) throws JobExecutionException {
        // Check if all the output files have been actually created (in case user has forgotten)
        // No need to distinguish between IN or OUT files, because IN files will exist, and
        // if there's one or more missing, they will be necessarily out.
        boolean allOutFilesCreated = true;
        for (InvocationParam param : invocation.getParams()) {
            allOutFilesCreated &= checkOutParam(param, invocation);
        }
        for (InvocationParam param : invocation.getResults()) {
            allOutFilesCreated &= checkOutParam(param, invocation);
        }
        if (!allOutFilesCreated) {
            throw new JobExecutionException(
                ERROR_OUT_FILES + invocation.getMethodImplementation().getMethodDefinition());
        }
    }

    private boolean checkOutParam(InvocationParam param, Invocation invocation) {
        if (param.getType().equals(DataType.FILE_T)) {
            // check if collection enter here
            // why enters here
            // todo: third can be here as well
            if (Tracer.extraeEnabled()) {
                Tracer.emitEvent(TraceEvent.CHECK_OUT_PARAM.getId(), TraceEvent.CHECK_OUT_PARAM.getType());
            }
            String filepath = (String) param.getValue();
            File f = new File(filepath);
            // If using C binding we ignore potential errors
            if (!f.exists()) {
                StringBuilder errMsg = new StringBuilder();
                errMsg.append("ERROR: File with path '").append(filepath);
                errMsg.append("' not generated by task with Method Definition ")
                    .append(invocation.getMethodImplementation().getMethodDefinition());
                System.out.println(errMsg.toString());
                System.err.println(errMsg.toString());
                return false;
            }
            if (Tracer.extraeEnabled()) {
                Tracer.emitEvent(Tracer.EVENT_END, TraceEvent.CHECK_OUT_PARAM.getType());
            }
        }
        return true;

    }

    private void createEmptyFile(Invocation invocation) {
        PrintStream out = context.getThreadOutStream();
        out.println("[EXECUTOR] executeTask - Checking if a blank file needs to be created");
        for (InvocationParam param : invocation.getParams()) {
            if (param.getType().equals(DataType.FILE_T)) {
                String filepath = (String) param.getValue();
                File f = new File(filepath);
                // If using C binding we ignore potential errors
                if (!f.exists()) {
                    out.println("[EXECUTOR] executeTask - Creating a new blank file");
                    try {
                        f.createNewFile();
                    } catch (IOException e) {
                        System.err.println("[EXECUTOR] checkJobFiles - Error in creating a new blank file");
                    }
                }
            }
        }
    }

    private Invoker selectNativeMethodInvoker(Invocation invocation, File taskSandboxWorkingDir,
        InvocationResources assignedResources) throws JobExecutionException {
        switch (invocation.getLang()) {
            case JAVA:
                Invoker javaInvoker = null;
                switch (context.getExecutionType()) {
                    case COMPSS:
                        if (context.getRuntimeAPI() != null && context.getLoaderAPI() != null) {
                            System.out.println("Nested Support enabled on the Invocation Context!");
                            javaInvoker =
                                new JavaNestedInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                        } else {
                            javaInvoker =
                                new JavaInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                        }
                        break;
                    case STORAGE:
                        javaInvoker = new StorageInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                        break;
                }
                return javaInvoker;
            case PYTHON:
                if (pyPipes == null) {
                    PipedMirror mirror;
                    synchronized (platform) {
                        mirror = (PipedMirror) platform.getMirror(PythonInvoker.class);
                        if (mirror == null) {
                            mirror = PythonInvoker.getMirror(context, platform);
                            platform.registerMirror(PythonInvoker.class, mirror);
                        }
                    }
                    pyPipes = mirror.registerExecutor(id);
                }
                return new PythonInvoker(context, invocation, taskSandboxWorkingDir, assignedResources, pyPipes);
            case C:
                Invoker cInvoker = null;
                if (context.isPersistentCEnabled()) {
                    cInvoker = new CPersistentInvoker(context, invocation, taskSandboxWorkingDir, assignedResources);
                    if (!isRegistered) {
                        PersistentMirror mirror;
                        synchronized (platform) {
                            mirror = (PersistentMirror) platform.getMirror(CPersistentInvoker.class);
                            if (mirror == null) {
                                mirror = CPersistentInvoker.getMirror(context, platform);
                                platform.registerMirror(CPersistentInvoker.class, mirror);
                            }
                        }
                        mirror.registerExecutor(id);
                        isRegistered = true;
                    }
                } else {
                    if (cPipes == null) {
                        PipedMirror mirror;
                        synchronized (platform) {
                            mirror = (PipedMirror) platform.getMirror(CInvoker.class);
                            if (mirror == null) {
                                mirror = (PipedMirror) CInvoker.getMirror(context, platform);
                                platform.registerMirror(CInvoker.class, mirror);
                            }
                        }
                        cPipes = mirror.registerExecutor(id);
                    }
                    cInvoker = new CInvoker(context, invocation, taskSandboxWorkingDir, assignedResources, cPipes);
                }
                return cInvoker;
            default:
                throw new JobExecutionException("Unrecognised lang for a method type invocation");
        }
    }

    /**
     * Delete a file or a directory and its children.
     *
     * @param directory The directory to delete.
     * @throws IOException Exception when problem occurs during deleting the directory.
     */
    private static void deleteDirectory(File directory) throws IOException {

        Path dPath = directory.toPath();
        Files.walkFileTree(dPath, new SimpleFileVisitor<Path>() {

            @Override
            public FileVisitResult visitFile(Path file, BasicFileAttributes attrs) throws IOException {
                Files.delete(file);
                return FileVisitResult.CONTINUE;
            }

            @Override
            public FileVisitResult postVisitDirectory(Path dir, IOException exc) throws IOException {
                Files.delete(dir);
                return FileVisitResult.CONTINUE;
            }
        });
    }


    private class TaskWorkingDir {

        private final File workingDir;
        private final boolean isSpecific;


        public TaskWorkingDir(File workingDir, boolean isSpecific) {
            this.workingDir = workingDir;
            this.isSpecific = isSpecific;
        }

        public File getWorkingDir() {
            return this.workingDir;
        }

        public boolean isSpecific() {
            return this.isSpecific;
        }
    }
}
