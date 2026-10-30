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

package es.bsc.compss.execution.executor;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.COMPSsConstants.Lang;
import es.bsc.compss.exceptions.WorkflowException;
import es.bsc.compss.execution.Invocation;
import es.bsc.compss.execution.InvocationContext;
import es.bsc.compss.execution.InvocationExecutionListener;
import es.bsc.compss.execution.InvocationParam;
import es.bsc.compss.execution.InvocationParamCollection;
import es.bsc.compss.execution.exceptions.JobExecutionException;
import es.bsc.compss.execution.exceptions.NonExistentDataException;
import es.bsc.compss.execution.exceptions.NonExistentElementException;
import es.bsc.compss.execution.exceptions.UnsufficientAvailableResourcesException;
import es.bsc.compss.execution.exceptions.UnwritableValueException;
import es.bsc.compss.execution.exceptions.UnwritableValuesException;
import es.bsc.compss.execution.invokers.Invoker;
import es.bsc.compss.execution.invokers.InvokerFactory;
import es.bsc.compss.execution.invokers.util.BinaryRunner;
import es.bsc.compss.execution.platform.CpuAffinity;
import es.bsc.compss.execution.platform.ExecutorRequest;
import es.bsc.compss.execution.platform.InvocationResources;
import es.bsc.compss.execution.tracing.TaskExecutionEvent;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.semantics.Constants;
import es.bsc.compss.semantics.data.DataType;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.JuliaDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.MethodDefinition;
import es.bsc.compss.types.implementations.definition.MpmdMPIDefinition;
import es.bsc.compss.types.implementations.definition.MultiNodeDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.tracing.TaskExecutionType;
import es.bsc.wdc.tracing.Tracer;
import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.time.OffsetDateTime;
import java.time.format.DateTimeFormatter;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;
import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class Executor implements Runnable, InvocationRunner {

    // Conversion
    private static final int NANO_TO_MS = 1_000_000;

    // Error messages
    private static final String ERROR_OUT_FILES =
        "ERROR: One or more OUT files have not" + " been created by task with Method Definition [";

    // Loggers
    private static final Logger workerLogger = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private static final boolean workerDebug = workerLogger.isDebugEnabled();

    private static final Logger dpLogger = LogManager.getLogger(Loggers.DATA_PROVENANCE);
    private static final boolean dpEnabled;

    private static final Logger timerLogger = LogManager.getLogger(Loggers.TIMER);
    public static final boolean timerEnabled;

    static {
        // Load data provenance property
        String isDPEnabledProperty = System.getProperty(COMPSsConstants.DATA_PROVENANCE);
        dpEnabled = isDPEnabledProperty != null && !isDPEnabledProperty.isEmpty() && !isDPEnabledProperty.equals("null")
            && Boolean.parseBoolean(isDPEnabledProperty);

        // Load timer property
        String isTimerCOMPSsEnabledProperty = System.getProperty(COMPSsConstants.TIMER_COMPSS_NAME);
        timerEnabled = isTimerCOMPSsEnabledProperty != null && !isTimerCOMPSsEnabledProperty.isEmpty()
            && !isTimerCOMPSsEnabledProperty.equals("null") && Boolean.parseBoolean(isTimerCOMPSsEnabledProperty);
    }

    // Attached component NIOWorker
    private final InvocationContext context;
    // Attached component Request queue
    protected final ExecutorContext platform;
    // Executor Id
    protected final int id;
    protected final String name;

    // First time boolean - to avoid the first 0 events in emitAffinityChangeEvents
    private boolean firstTimeAffinityCPU;
    private boolean firstTimeAffinityGPU;

    protected boolean isRegistered;

    protected Invocation invocation;
    protected InvocationExecutionListener invocationListener;
    protected InvocationResources resources;


    /**
     * Instantiates a new Executor.
     *
     * @param context Invocation context
     * @param platform Executor context (ExecutorRequest Platform
     * @param executorId Executor Identifier
     * @param executorName Executor Name
     */
    public Executor(InvocationContext context, ExecutorContext platform, int executorId, String executorName) {
        workerLogger.info("Executor {} init", executorName);
        this.context = context;
        this.platform = platform;
        this.id = executorId;
        this.name = executorName;
        this.isRegistered = false;
        this.firstTimeAffinityCPU = true; // Set to false after the first time
        this.firstTimeAffinityGPU = true; // Set to false after the first time
    }

    /**
     * Starts the executor execution.
     */
    public void start() {
        // Nothing to do since everything is deleted in each task execution
        workerLogger.info("Executor started");
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
        workerLogger.info("Executor {} finishing", this.name);
        // Nothing to do since everything is deleted in each task execution
        if (Tracer.isActivated()) {
            emitAffinityEndEvents();
        }
        this.platform.finishedExecutor(this);
        workerLogger.info("Executor {} finished", this.name);
    }

    /**
     * Returns the executor id.
     *
     * @return executor id
     */
    public int getId() {
        return this.id;
    }

    /**
     * Returns the Computing platform comprising the executor.
     * 
     * @return Computing platform comprising the executor
     */
    public ExecutorContext getPlatform() {
        return platform;
    }

    /**
     * Returns the executor name.
     *
     * @return executor name
     */
    public String getName() {
        return this.name;
    }

    private void processRequests() {

        ExecutorRequest execution = this.platform.newThread();
        while (true) {
            if (execution == null) {
                workerLogger.error("ERROR: Execution is null!!!!!");
            } else {
                try {
                    execution.run(this);
                } catch (ExecutorRequest.StopExecutorException se) {
                    workerLogger.debug("Stop request on Executor {}", this.name);
                    break;
                }
            }
            execution = this.platform.getJob(); // Get tasks until there are no more tasks pending
        }
    }

    /**
     * Runs the invocation in this executor.
     *
     * @param inv invocation to run
     * @param listener element to notify changes in the execution
     * @throws WorkflowException COMPSs exception raised by the user code
     * @throws Exception Error preparing, running or post-processing the invocation
     */
    public void processInvocation(Invocation inv, InvocationExecutionListener listener)
        throws WorkflowException, Exception {
        this.invocation = inv;
        this.invocationListener = listener;

        if (dpEnabled) {
            dpLogger.info("TASK={} STARTTIME={}", this.invocation.getTaskId(),
                OffsetDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")));
        }

        boolean success = false;
        invocation.executionStarts();
        if (workerDebug) {
            workerLogger.debug("Dequeuing job {}", invocation.getJobId());
        }

        try {
            execute();
            success = true;
        } catch (WorkflowException e) {
            throw e;
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof WorkflowException) {
                throw (WorkflowException) rootCause;
            } else {
                throw e;
            }
        } finally {

            if (workerDebug) {
                workerLogger.debug("Job {} finished (success: {})", invocation.getJobId(), success);
            }
            invocation.executionEnds();

            if (dpEnabled) {
                dpLogger.info("TASK={} ENDTIME={}", this.invocation.getTaskId(),
                    OffsetDateTime.now().format(DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm:ss.SSSSSSXXX")));
            }

            invocation = null;
            invocationListener = null;
        }
    }

    private void execute() throws Exception {
        Lang lang = null;
        if (invocation.getMethodImplementation().getMethodType() == MethodType.METHOD) {
            lang = ((MethodDefinition) invocation.getMethodImplementation().getDefinition()).getLang();
        } else {
            if (invocation.getMethodImplementation().getMethodType() == MethodType.MULTI_NODE) {
                lang = ((MultiNodeDefinition) invocation.getMethodImplementation().getDefinition()).getLang();
            }
        }
        if (lang != null && lang != Lang.JAVA && lang != Lang.PYTHON && lang != Lang.C && lang != Lang.R) {
            String errMsg = "Incorrect language " + lang + " in job " + invocation.getJobId();
            workerLogger.error(errMsg);
            // Print to the job.err file
            this.context.getThreadErrStream().println(errMsg);
            throw new JobExecutionException("Incorrect language " + lang);
        }
        totalTimerAndTracingWrapperAndRun();
    }

    private void logExecutionException(Exception e) {
        workerLogger.error("ERROR: Executing task {}", e.getMessage(), e);

        // Writing in the task .err/.out
        PrintStream out = this.context.getThreadOutStream();
        out.println("Exception executing task " + e.getMessage());

        PrintStream err = this.context.getThreadErrStream();
        e.printStackTrace(err);
    }

    private void totalTimerAndTracingWrapperAndRun() throws Exception {
        if (Tracer.isActivated()) {
            emitingTaskStartEvents();
        }

        long tsTotalStart = 0L;
        if (timerEnabled) {
            tsTotalStart = System.nanoTime();
        }
        try {
            resourcesWrapperAndRun();
        } finally {
            // Always end task tracing
            if (Tracer.isActivated()) {
                emitTaskEndEvents();
            }
            // Write timer if needed
            if (timerEnabled) {
                final long tsTotalEnd = System.nanoTime();
                final float lapseTotal = (tsTotalEnd - tsTotalStart) / (float) NANO_TO_MS;
                final int jobId = invocation.getJobId();
                timerLogger.info("[TIMER] Total time for job {}: {} ms", jobId, lapseTotal);
            }
        }
    }

    private void resourcesWrapperAndRun() throws Exception {
        final int jobId = invocation.getJobId();

        if (timerEnabled) {
            obtainExecutionResourcesWithTimer(jobId, invocation.getRequirements());
        } else {
            obtainExecutionResources(jobId, invocation.getRequirements());
        }

        try {
            sandBoxWrapperAndRun();
        } finally {
            if (timerEnabled) {
                releaseResourcesWithTimer(jobId);
            } else {
                releaseResources(jobId);
            }
        }
    }

    private void sandBoxWrapperAndRun() throws Exception {
        ExecutionSandbox twd = null;

        try {
            // Set the Task working directory
            if (timerEnabled) {
                twd = createTaskSandboxWithTimer();
            } else {
                twd = createTaskSandbox();
            }

            filesWrapperAndRun(twd);
            // Clean the task sandbox working dir if no error
            if (timerEnabled) {
                cleanTaskSandboxWithTimer(twd);
            } else {
                cleanTaskSandbox(twd);
            }
        } catch (WorkflowException e) {
            // Clean the task sandbox working dir if no error
            if (timerEnabled) {
                cleanTaskSandboxWithTimer(twd);
            } else {
                cleanTaskSandbox(twd);
            }
            throw e;
        }
    }

    private void filesWrapperAndRun(ExecutionSandbox twd) throws Exception {
        boolean alreadyFailed = false;
        boolean producesEmptyResultsOnFailure = invocation.producesEmptyResultsOnFailure();
        try {
            if (timerEnabled) {
                // Bind files to task sandbox working dir
                stageInWithTimer(twd);
                redirectStreamsAndRunWithTimer(twd);
            } else {
                // Bind files to task sandbox working dir
                stageIn(twd);
                redirectStreamsAndRun(twd);
            }
        } catch (WorkflowException e) {
            logExecutionException(e);
            alreadyFailed = true;
            producesEmptyResultsOnFailure = true;
            throw e;
        } catch (Exception e) {
            logExecutionException(e);
            alreadyFailed = true;
            throw e;
        } finally {
            if (!alreadyFailed || producesEmptyResultsOnFailure) {
                // Unbind files from task sandbox working dir
                try {
                    if (timerEnabled) {
                        stageOutWithTimer(twd, !alreadyFailed, producesEmptyResultsOnFailure);
                    } else {
                        stageOut(twd, !alreadyFailed, producesEmptyResultsOnFailure);
                    }
                } catch (Exception ex) {
                    if (alreadyFailed) {
                        workerLogger.warn("Another exception after unbinding files: {}", ex.getMessage(), ex);
                        PrintStream out = this.context.getThreadOutStream();
                        out.println("Another exception unbinding files: " + ex.getMessage());
                        PrintStream err = this.context.getThreadErrStream();
                        ex.printStackTrace(err);
                    } else {
                        logExecutionException(ex);
                        throw ex;
                    }
                }
            }
            if (dpEnabled) {
                dpLogger.info("TASK={} LOGFILE={}.out", invocation.getTaskId(),
                    this.context.getStandardStreamsPath(invocation));
                dpLogger.info("TASK={} LOGFILE={}.err", invocation.getTaskId(),
                    this.context.getStandardStreamsPath(invocation));
            }
        }
    }

    private void redirectStreamsAndRunWithTimer(ExecutionSandbox twd) throws Exception {
        int jobId = invocation.getJobId();
        // Execute task
        workerLogger.debug("Executing task invocation for Job {}", jobId);
        long tsExecStart = 0L;
        if (timerEnabled) {
            tsExecStart = System.nanoTime();
        }

        redirectStreamsAndRun(twd);
        final long tsExecEnd = System.nanoTime();
        final float lapseExec = (tsExecEnd - tsExecStart) / (float) NANO_TO_MS;
        timerLogger.info("[TIMER] Execute job {}: {} ms", jobId, lapseExec);
    }

    private void redirectStreamsAndRun(ExecutionSandbox twd) throws Exception {

        /* Register outputs **************************************** */
        String streamsPath = this.context.getStandardStreamsPath(invocation);
        this.context.registerOutputs(streamsPath);
        PrintStream out = this.context.getThreadOutStream();

        /* TRY TO PROCESS THE TASK ******************************** */
        if (invocation.isDebugEnabled()) {
            out.println("[EXECUTOR] executeTask - Begin task execution");
        }
        try {
            runInvocation(twd);
        } catch (WorkflowException ce) {
            workerLogger.warn("[EXECUTOR] executeTask - COMPSs Exception received");
            out.println("[EXECUTOR] executeTask - COMPSs Exception received");
            throw ce;
        } catch (Exception jee) {
            workerLogger.error("[EXECUTOR] executeTask - Error in task execution");
            out.println("[EXECUTOR] executeTask - Error in task execution");
            PrintStream err = this.context.getThreadErrStream();
            err.println("[EXECUTOR] executeTask - Error in task execution");
            jee.printStackTrace(err);
            throw jee;
        } finally {
            if (invocation.isDebugEnabled()) {
                out.println("[EXECUTOR] executeTask - End task execution");
            }
            this.context.unregisterOutputs();
        }
    }

    private void runInvocation(ExecutionSandbox twd) throws WorkflowException, JobExecutionException {
        Invoker<?> invoker = InvokerFactory.Registry.getInvoker(this, this.context, invocation, twd, resources);
        try {
            this.platform.registerRunningJob(invocation, invoker);
            invoker.runInvocation(this);
        } finally {
            this.platform.unregisterRunningJob(invocation.getJobId());
        }
    }

    private void obtainExecutionResourcesWithTimer(int jobId, ResourceDescription requirements)
        throws UnsufficientAvailableResourcesException {
        long tsAssignResStart = 0L;
        if (timerEnabled) {
            tsAssignResStart = System.nanoTime();
        }
        try {
            obtainExecutionResources(jobId, requirements);
        } finally {
            final long tsAssignResEnd = System.nanoTime();
            final float lapseAssignRes = (tsAssignResEnd - tsAssignResStart) / (float) NANO_TO_MS;
            timerLogger.debug("[TIMER] Assign resources for job {}: {} ms", jobId, lapseAssignRes);
        }
    }

    private void obtainExecutionResources(int jobId, ResourceDescription requirements)
        throws UnsufficientAvailableResourcesException {
        workerLogger.debug("Assigning resources for Job {}", jobId);
        try {
            this.resources = this.platform.acquireResources(jobId, requirements, resources);
            assignExecutionResources();
        } catch (Exception e) {
            logExecutionException(e);
            throw e;
        }
    }

    private void assignExecutionResources() {
        if (Tracer.isActivated()) {
            emitAffinityChangeEvents();
        }
        if (this.resources != null && this.resources.getAssignedCPUs() != null
            && this.resources.getAssignedCPUs().length > 0) {
            try {
                CpuAffinity.setCurrentThreadAffinity(this.resources.getAssignedCPUs());
            } catch (Exception e) {
                workerLogger.warn("Error setting affinity for Job {}", this.invocation.getJobId(), e);
            }
        }
    }

    @Override
    public void stalledCodeExecution() {
        int jobId = invocation.getJobId();
        workerLogger.debug("Release bound resources for Job {}", jobId);
        long tsUnassignResStart = 0L;
        if (timerEnabled) {
            tsUnassignResStart = System.nanoTime();
        }
        this.platform.blockedRunner(invocation, this, this.resources);
        if (timerEnabled) {
            final long tsUnassignResEnd = System.nanoTime();
            final float lapseUnassignRes = (tsUnassignResEnd - tsUnassignResStart) / (float) NANO_TO_MS;
            timerLogger.debug("[TIMER] Unassign resources for job {}: {} ms", jobId, lapseUnassignRes);
        }
    }

    @Override
    public void readyToContinueExecution(Semaphore sem) {
        long tsAssignResStart = 0L;
        if (timerEnabled) {
            tsAssignResStart = System.nanoTime();
        }
        this.platform.unblockedRunner(invocation, this, this.resources, sem);
        assignExecutionResources();

        if (timerEnabled) {
            final long tsAssignResEnd = System.nanoTime();
            final float lapseAssignRes = (tsAssignResEnd - tsAssignResStart) / (float) NANO_TO_MS;
            final int jobId = invocation.getJobId();
            timerLogger.debug("[TIMER] Re-assign resources for job {} : {} ms", jobId, lapseAssignRes);
        }
    }

    private void releaseResourcesWithTimer(int jobId) {
        long timeUnassignResStart = 0L;
        if (timerEnabled) {
            timeUnassignResStart = System.nanoTime();
        }
        try {
            this.platform.releaseResources(jobId);
        } finally {
            final long timeUnassignResEnd = System.nanoTime();
            final float lapseUnassignRes = (timeUnassignResEnd - timeUnassignResStart) / (float) NANO_TO_MS;
            timerLogger.debug("[TIMER] Unassign resources for job {} : {} ms", jobId, lapseUnassignRes);
        }

    }

    private void releaseResources(int jobId) {
        workerLogger.debug("Release binded resources for Job " + jobId);
        this.platform.releaseResources(jobId);
    }
    /*
     * ---------------------- SANDBOX MANAGEMENT --------------------------------
     */

    private ExecutionSandbox createTaskSandboxWithTimer() throws Exception {
        // Start timer
        long tsSandboxStart = 0L;
        if (timerEnabled) {
            tsSandboxStart = System.nanoTime();
        }
        try {
            return createTaskSandbox();
        } finally {
            final long tsSandboxEnd = System.nanoTime();
            final float lapseSandbox = (tsSandboxEnd - tsSandboxStart) / (float) NANO_TO_MS;
            timerLogger.debug("[TIMER] Create sandbox for job {}: {} ms", invocation.getJobId(), lapseSandbox);
        }
    }

    /**
     * Creates a sandbox for a task.
     *
     * @return Sandbox dir
     * @throws Exception Error creating sandbox
     */
    private ExecutionSandbox createTaskSandbox() throws Exception {
        final int jobId = invocation.getJobId();
        workerLogger.debug("Creating task sandbox for Job {}", jobId);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(TaskExecutionEvent.CREATING_TASK_SANDBOX);
        }
        AbstractMethodImplementation impl = invocation.getMethodImplementation();
        ExecutionSandbox taskWD;
        try {
            // Check if an specific working dir is provided
            String specificWD = "";
            switch (impl.getMethodType()) {
                case CONTAINER:
                    ContainerDefinition contImpl = (ContainerDefinition) impl.getDefinition();
                    specificWD = contImpl.getWorkingDir();
                    break;
                case BINARY:
                    BinaryDefinition binaryImpl = (BinaryDefinition) impl.getDefinition();
                    specificWD = binaryImpl.getWorkingDir();
                    break;
                case MPI:
                    MPIDefinition mpiImpl = (MPIDefinition) impl.getDefinition();
                    specificWD = mpiImpl.getWorkingDir();
                    break;
                case MPMDMPI:
                    MpmdMPIDefinition mpmpdDef = (MpmdMPIDefinition) impl.getDefinition();
                    specificWD = mpmpdDef.getWorkingDir();
                    break;
                case PYTHON_MPI:
                    PythonMPIDefinition nativeMPIImpl = (PythonMPIDefinition) impl.getDefinition();
                    specificWD = nativeMPIImpl.getWorkingDir();
                    break;
                case COMPSs:
                    COMPSsDefinition compssImpl = (COMPSsDefinition) impl.getDefinition();
                    // if working dir is unassigned skip it for the construction of the specific working dir
                    if (!compssImpl.getWorkingDir().equals(Constants.UNASSIGNED)) {
                        specificWD = compssImpl.getWorkingDir() + File.separator;
                    }
                    // if (compssImpl.getParentAppId() != null) {
                    // specificWD += compssImpl.getParentAppId() + File.separator;
                    // }
                    // specificWD += "compss_job_" + invocation.getJobId() + "_" + invocation.getHistory().name();
                    break;
                case DECAF:
                    DecafDefinition decafImpl = (DecafDefinition) impl.getDefinition();
                    specificWD = decafImpl.getWorkingDir();
                    break;
                case JULIA:
                    JuliaDefinition juliaImpl = (JuliaDefinition) impl.getDefinition();
                    specificWD = juliaImpl.getWorkingDir();
                    break;
                case OMPSS:
                    OmpSsDefinition ompssImpl = (OmpSsDefinition) impl.getDefinition();
                    specificWD = ompssImpl.getWorkingDir();
                    break;
                case OPENCL:
                    OpenCLDefinition openclImpl = (OpenCLDefinition) impl.getDefinition();
                    specificWD = openclImpl.getWorkingDir();
                    break;
                default:
                    specificWD = null;
            }

            File workingDir;
            boolean isSpecific;
            isSpecific = (specificWD != null && !specificWD.isEmpty() && !specificWD.equals(Constants.UNASSIGNED));
            if (isSpecific) {
                // Binary has an specific working dir, set it
                workingDir = BinaryRunner.getUpdatedWorkingDir(this.invocation.getParams(), specificWD);
            } else {
                // No specific working dir provided, set default sandbox
                String completePath = this.context.getWorkingDir() + "sandBox" + File.separator + "job_" + jobId;
                workingDir = new File(completePath);
            }

            taskWD = new ExecutionSandbox(workingDir, isSpecific);
            taskWD.create();
        } catch (Exception e) {
            logExecutionException(e);
            throw e;
        } finally {
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TaskExecutionEvent.CREATING_TASK_SANDBOX);
            }
        }
        return taskWD;
    }

    private void cleanTaskSandboxWithTimer(ExecutionSandbox twd) {
        long tsSandboxStart = 0L;
        if (timerEnabled) {
            tsSandboxStart = System.nanoTime();
        }
        try {
            cleanTaskSandbox(twd);
        } finally {
            final long tsSandboxEnd = System.nanoTime();
            final float lapseSandbox = (tsSandboxEnd - tsSandboxStart) / (float) NANO_TO_MS;
            final int jobId = invocation.getJobId();
            timerLogger.debug("[TIMER] Clean sandbox for job {}: {} ms", jobId, lapseSandbox);
        }

    }

    private void cleanTaskSandbox(ExecutionSandbox twd) {
        final int jobId = invocation.getJobId();
        workerLogger.debug("Cleaning task sandbox for Job {}", jobId);

        if (twd != null) {
            try {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(TaskExecutionEvent.REMOVING_TASK_SANDBOX);
                }
                twd.clean();
            } finally {
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TaskExecutionEvent.REMOVING_TASK_SANDBOX);
                }
            }
        }
    }

    private void stageInWithTimer(ExecutionSandbox sandbox) throws Exception {
        long tsBindStart = 0L;
        if (timerEnabled) {
            tsBindStart = System.nanoTime();
        }
        try {
            stageIn(sandbox);
        } finally {
            final long tsBindEnd = System.nanoTime();
            final float lapseBind = (tsBindEnd - tsBindStart) / (float) NANO_TO_MS;
            timerLogger.debug("[TIMER] Bind original files for job {}: {} ms", invocation.getJobId(), lapseBind);
        }
    }

    /**
     * Create symbolic links from files with the original name in task sandbox to the renamed file.
     *
     * @param sandbox created sandbox
     * @throws IOException returns exception is a problem occurs during creation
     */
    private void stageIn(ExecutionSandbox sandbox) throws Exception {
        final int jobId = invocation.getJobId();
        workerLogger.debug("Binding renamed files to sandboxed original names for Job {}", jobId);
        try {

            for (InvocationParam param : invocation.getParams()) {
                if (!param.isKeepRename()) {
                    bindOriginalFilenameToRenames(param, sandbox);
                } else {
                    // collection should enter here
                    String renamedFilePath = (String) param.getValue();
                    workerLogger.debug("Parameter keeps rename: {}", renamedFilePath);
                    param.setRenamedName(renamedFilePath);
                    param.setOriginalName(renamedFilePath);
                }
            }
            if (invocation.getTarget() != null) {
                workerLogger.debug("Invocation has non-null target");
                InvocationParam param = invocation.getTarget();
                if (!param.isKeepRename()) {
                    bindOriginalFilenameToRenames(param, sandbox);
                } else {
                    String renamedFilePath = (String) param.getValue();
                    workerLogger.debug("Parameter keeps rename: {}", renamedFilePath);
                    param.setRenamedName(renamedFilePath);
                    param.setOriginalName(renamedFilePath);
                }
            }
            for (InvocationParam param : invocation.getResults()) {
                if (!param.isKeepRename()) {
                    bindOriginalFilenameToRenames(param, sandbox);
                } else {
                    String renamedFilePath = (String) param.getValue();
                    workerLogger.debug("Parameter keeps rename: {}", renamedFilePath);
                    param.setRenamedName(renamedFilePath);
                    param.setOriginalName(renamedFilePath);
                }
            }
        } catch (Exception e) {
            logExecutionException(e);
            throw e;
        }
    }

    private void bindOriginalFilenameToRenames(InvocationParam param, ExecutionSandbox sandbox) throws IOException {

        if (Tracer.isActivated()) {
            Tracer.emitEvent(TaskExecutionEvent.BIND_ORIG_NAME);
        }

        if (param.isCollective()) {
            InvocationParamCollection<InvocationParam> cp = (InvocationParamCollection<InvocationParam>) param;
            for (InvocationParam p : cp.getCollectionParameters()) {
                bindOriginalFilenameToRenames(p, sandbox);
            }
        } else {
            if (param.getType() == DataType.FILE_T) {
                String renamedPath = (String) param.getValue();
                workerLogger.debug("Renamed File Path is {}", renamedPath);
                param.setRenamedName(renamedPath);

                File renamedFile = new File(renamedPath);
                if (renamedFile.getName().equals(param.getOriginalName())) {
                    param.setOriginalName(renamedPath);
                } else {
                    String originalName = param.getOriginalName();
                    String dataId = param.getDataMgmtId();

                    String inSandboxPath = sandbox.addFile(dataId, renamedFile, originalName);
                    if (inSandboxPath != null) {
                        workerLogger.debug("Setting Original Name to {}", inSandboxPath);
                        param.setOriginalName(inSandboxPath);
                        param.setValue(inSandboxPath);
                    } else {
                        // IN / INOUT file is originally located in the working dir, setting renamed name to
                        // original name to avoid the deletion at the end of task execution
                        param.setRenamedName(param.getOriginalName());
                        param.setValue(param.getOriginalName());
                    }
                }
            }
        }

        if (Tracer.isActivated()) {
            Tracer.emitEventEnd(TaskExecutionEvent.BIND_ORIG_NAME);
        }
    }

    private void stageOutWithTimer(ExecutionSandbox sandbox, boolean raiseExceptionIfNonExistent,
        boolean createifNonExistent) throws IOException, JobExecutionException {
        long tsUnbindStart = 0L;
        if (timerEnabled) {
            tsUnbindStart = System.nanoTime();
        }
        try {
            stageOut(sandbox, raiseExceptionIfNonExistent, createifNonExistent);
        } finally {
            final long tsUnbindEnd = System.nanoTime();
            final float lapseUnbind = (tsUnbindEnd - tsUnbindStart) / (float) NANO_TO_MS;
            final int jobId = invocation.getJobId();
            timerLogger.debug("[TIMER] Unbind original files for job {}: {} ms", jobId, lapseUnbind);
        }
    }

    /**
     * Undo symbolic links and renames done with the original names in task sandbox to the renamed file.
     *
     * @throws JobExecutionException Exception unbinding original names to renamed names
     */
    private void stageOut(ExecutionSandbox sandbox, boolean raiseExceptionIfNonExistent, boolean createifNonExistent)
        throws JobExecutionException {
        // Unbind files from task sandbox working dir
        final int jobId = invocation.getJobId();
        workerLogger.debug("Removing renamed files to sandboxed original names for Job {}", jobId);
        String message = null;
        boolean failure = false;
        for (InvocationParam param : invocation.getParams()) {
            try {
                stageOutParam(param, sandbox, raiseExceptionIfNonExistent, createifNonExistent);
            } catch (Exception e) {
                if (!failure) {
                    message = e.getMessage();
                } else {
                    message = message.concat("\n" + e.getMessage());
                }
                failure = true;
            }
        }
        if (invocation.getTarget() != null) {
            try {
                stageOutParam(invocation.getTarget(), sandbox, raiseExceptionIfNonExistent, createifNonExistent);
            } catch (Exception e) {
                if (!failure) {
                    message = e.getMessage();
                } else {
                    message = message.concat("\n" + e.getMessage());
                }
                failure = true;
            }
        }
        for (InvocationParam param : invocation.getResults()) {
            try {
                stageOutParam(param, sandbox, raiseExceptionIfNonExistent, createifNonExistent);
            } catch (Exception e) {
                if (!failure) {
                    message = e.getMessage();
                } else {
                    message = message.concat("\n" + e.getMessage());
                }
                failure = true;
            }
        }
        if (failure) {
            throw new JobExecutionException(message);
        }
    }

    private void stageOutParam(InvocationParam param, ExecutionSandbox sandbox, boolean raiseExceptionIfNonExistent,
        boolean createifNonExistent) throws NonExistentDataException, UnwritableValueException {
        LinkedList<NonExistentDataException> nonExistsExc = new LinkedList<>();
        LinkedList<UnwritableValueException> unwrittableExc = new LinkedList<>();

        if (param.isCollective()) {
            InvocationParamCollection<InvocationParam> cp = (InvocationParamCollection<InvocationParam>) param;
            for (InvocationParam p : cp.getCollectionParameters()) {
                try {
                    stageOutParam(p, sandbox, raiseExceptionIfNonExistent, createifNonExistent);
                } catch (NonExistentDataException nede) {
                    nonExistsExc.add(nede);
                } catch (UnwritableValueException uve) {
                    unwrittableExc.add(uve);
                }
            }
        } else {
            unbindOriginalFilenameToRename(param, sandbox);
        }

        if (param.isWriteFinalValue()) {
            try {
                if (!param.isForwardedResult()) {
                    this.context.storeParam(param, createifNonExistent);
                }
                this.invocationListener.onResultAvailable(param);
            } catch (NonExistentDataException nede) {
                if (raiseExceptionIfNonExistent) {
                    throw nede;
                }
            }
        }

        if (raiseExceptionIfNonExistent && !nonExistsExc.isEmpty()) {
            throw new NonExistentElementException(param.getName(), nonExistsExc);
        }
        if (!unwrittableExc.isEmpty()) {
            throw new UnwritableValuesException(unwrittableExc);
        }
    }

    private void unbindOriginalFilenameToRename(InvocationParam param, ExecutionSandbox sandbox) {
        if (param.getType() == DataType.FILE_T && !param.isKeepRename()) {
            if (Tracer.isActivated()) {
                Tracer.emitEvent(TaskExecutionEvent.UNBIND_ORIG_NAME);
            }
            try {
                String inSandboxPath = param.getOriginalName();
                String renamedFilePath = param.getRenamedName();

                workerLogger.debug("Treating file {}", inSandboxPath);
                File internalFile = new File(inSandboxPath);
                File externalFile = new File(renamedFilePath);
                try {
                    if (!inSandboxPath.equals(renamedFilePath)) {
                        if (internalFile.exists()) {
                            sandbox.removeFile(inSandboxPath, renamedFilePath);
                        } else {
                            if (externalFile.exists()) {
                                // Is an IN and the data was already updated
                                workerLogger.debug("Repeated data for {}. Nothing to do", internalFile);
                            } else {
                                // Error output file does not exist
                                String msg = "WARN: Output file " + inSandboxPath + " does not exist";
                                this.context.getThreadErrStream().println(msg);
                            }
                        }
                    }
                } catch (IOException e) {
                    this.context.getThreadErrStream().println(e.getMessage());
                } finally {
                    param.setValue(renamedFilePath);
                    File inSandboxFile = new File(inSandboxPath);
                    String originalFileName = inSandboxFile.getName();
                    param.setOriginalName(originalFileName);
                }

            } finally {
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TaskExecutionEvent.UNBIND_ORIG_NAME);
                }
            }
        }
    }

    /*
     * ---------------------- TRACE EVENTS MANAGEMENT --------------------------------
     */
    private void emitingTaskStartEvents() {
        if (Tracer.isActivated() & Tracer.isTracingTaskDependencies()) {
            if (invocation.getPredecessors() != null) {
                for (Integer i : invocation.getPredecessors()) {
                    Tracer.emitCommEvent(false, 1, 123, i, 0);
                }
            }
        }

        int numNodes = 1;
        if (invocation.getSlaveNodesNames() != null) {
            numNodes = invocation.getSlaveNodesNames().size() + 1;
        }
        int nCPUs = ((MethodResourceDescription) invocation.getRequirements()).getTotalCPUComputingUnits() * numNodes;
        Tracer.emitEvent(TaskExecutionType.CPU_COUNTS, nCPUs);
        int nGPUs = ((MethodResourceDescription) invocation.getRequirements()).getTotalGPUComputingUnits() * numNodes;
        Tracer.emitEvent(TaskExecutionType.GPU_COUNTS, nGPUs);
        int memory = (int) ((MethodResourceDescription) invocation.getRequirements()).getMemorySize() * numNodes;
        if (memory < 0) {
            memory = 0;
        }
        Tracer.emitEvent(TaskExecutionType.MEMORY, memory);
        int diskBW = ((MethodResourceDescription) invocation.getRequirements()).getStorageBW() * numNodes;
        if (diskBW < 0) {
            diskBW = 0;
        }
        Tracer.emitEvent(TaskExecutionType.DISK_BW, diskBW);
        int taskType = invocation.getMethodImplementation().getMethodType().ordinal() + 1;
        Tracer.emitEvent(TaskExecutionType.TASKTYPE, taskType);
        Tracer.emitEvent(TaskExecutionEvent.TASK_RUNNING);
    }

    private void emitTaskEndEvents() {
        if (Tracer.isActivated() & Tracer.isTracingTaskDependencies()) {
            for (int i = 0; i < this.invocation.getNumSuccessors(); i++) {
                Tracer.emitCommEvent(true, 1, 123, invocation.getTaskId(), 0);
            }
        }
        Tracer.emitEventEnd(TaskExecutionType.CPU_COUNTS);
        Tracer.emitEventEnd(TaskExecutionType.GPU_COUNTS);
        Tracer.emitEventEnd(TaskExecutionType.MEMORY);
        Tracer.emitEventEnd(TaskExecutionType.DISK_BW);
        Tracer.emitEventEnd(TaskExecutionType.TASKTYPE);
        Tracer.emitEventEnd(TaskExecutionEvent.TASK_RUNNING);
    }

    private void emitAffinityChangeEvents() {
        if (this.resources != null) {
            int[] cpus = this.resources.getAssignedCPUs();
            if (cpus != null && cpus.length > 0) {
                if (this.firstTimeAffinityCPU) {
                    this.firstTimeAffinityCPU = false;
                } else {
                    Tracer.emitEventEnd(TaskExecutionType.TASKS_CPU_AFFINITY);
                }
                Tracer.emitEvent(TaskExecutionType.TASKS_CPU_AFFINITY, cpus[0] + 1L);
            }
            int[] gpus = this.resources.getAssignedGPUs();
            if (gpus != null && gpus.length > 0) {
                if (this.firstTimeAffinityGPU) {
                    this.firstTimeAffinityGPU = false;
                } else {
                    Tracer.emitEventEnd(TaskExecutionType.TASKS_GPU_AFFINITY);
                }
                Tracer.emitEvent(TaskExecutionType.TASKS_GPU_AFFINITY, gpus[0] + 1L);
            }
        }
    }

    private void emitAffinityEndEvents() {
        if (!this.firstTimeAffinityCPU) {
            Tracer.emitEvent(TaskExecutionType.TASKS_CPU_AFFINITY, 0);
        }
        if (!this.firstTimeAffinityGPU) {
            Tracer.emitEvent(TaskExecutionType.TASKS_GPU_AFFINITY, 0);
        }
    }

}
