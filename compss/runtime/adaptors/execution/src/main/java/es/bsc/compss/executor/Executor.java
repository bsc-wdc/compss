/*
 *  Copyright 2002-2025 Barcelona Supercomputing Center (www.bsc.es)
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
import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.executor.external.persistent.PersistentMirror;
import es.bsc.compss.executor.external.piped.PipePair;
import es.bsc.compss.executor.external.piped.PipedMirror;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.JavaInvoker;
import es.bsc.compss.invokers.JavaNestedInvoker;
import es.bsc.compss.invokers.OpenCLInvoker;
import es.bsc.compss.invokers.StorageInvoker;
import es.bsc.compss.invokers.binary.BinaryInvoker;
import es.bsc.compss.invokers.binary.COMPSsInvoker;
import es.bsc.compss.invokers.binary.ContainerInvoker;
import es.bsc.compss.invokers.binary.DecafInvoker;
import es.bsc.compss.invokers.binary.JuliaInvoker;
import es.bsc.compss.invokers.binary.MPIInvoker;
import es.bsc.compss.invokers.binary.MpmdMPIInvoker;
import es.bsc.compss.invokers.binary.OmpSsInvoker;
import es.bsc.compss.invokers.external.PythonMPIInvoker;
import es.bsc.compss.invokers.external.persistent.CPersistentInvoker;
import es.bsc.compss.invokers.external.piped.CInvoker;
import es.bsc.compss.invokers.external.piped.PythonInvoker;
import es.bsc.compss.invokers.external.piped.RInvoker;
import es.bsc.compss.invokers.util.BinaryRunner;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.annotations.Constants;
import es.bsc.compss.types.annotations.parameter.DataType;
import es.bsc.compss.types.execution.ExecutionSandbox;
import es.bsc.compss.types.execution.ExecutorRequest;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.InvocationExecutionRequest;
import es.bsc.compss.types.execution.InvocationParam;
import es.bsc.compss.types.execution.InvocationParamCollection;
import es.bsc.compss.types.execution.exceptions.JobExecutionException;
import es.bsc.compss.types.execution.exceptions.NonExistentDataException;
import es.bsc.compss.types.execution.exceptions.NonExistentElementException;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableResourcesException;
import es.bsc.compss.types.execution.exceptions.UnwritableValueException;
import es.bsc.compss.types.execution.exceptions.UnwritableValuesException;
import es.bsc.compss.types.implementations.AbstractMethodImplementation;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.implementations.definition.BinaryDefinition;
import es.bsc.compss.types.implementations.definition.COMPSsDefinition;
import es.bsc.compss.types.implementations.definition.ContainerDefinition;
import es.bsc.compss.types.implementations.definition.DecafDefinition;
import es.bsc.compss.types.implementations.definition.JuliaDefinition;
import es.bsc.compss.types.implementations.definition.MPIDefinition;
import es.bsc.compss.types.implementations.definition.MpmdMPIDefinition;
import es.bsc.compss.types.implementations.definition.OmpSsDefinition;
import es.bsc.compss.types.implementations.definition.OpenCLDefinition;
import es.bsc.compss.types.implementations.definition.PythonMPIDefinition;
import es.bsc.compss.types.resources.MethodResourceDescription;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.tracing.TraceEvent;
import es.bsc.compss.types.tracing.TraceEventType;
import es.bsc.compss.util.Tracer;
import es.bsc.compss.worker.COMPSsException;
import es.bsc.compss.worker.TimeOutInvokerTask;
import es.bsc.compss.worker.TimeOutTask;
import es.bsc.wdc.affinity.ThreadAffinity;

import java.io.File;
import java.io.IOException;
import java.io.PrintStream;
import java.util.Collection;
import java.util.LinkedList;
import java.util.TimerTask;
import java.util.concurrent.Semaphore;

import org.apache.commons.lang3.exception.ExceptionUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


public class Executor implements Runnable, InvocationRunner {

    // Loggers
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_EXECUTOR);
    private static final boolean WORKER_DEBUG = LOGGER.isDebugEnabled();
    private static final Logger TIMER_LOGGER = LogManager.getLogger(Loggers.TIMER);

    // Error messages
    private static final String ERROR_OUT_FILES =
        "ERROR: One or more OUT files have not" + " been created by task with Method Definition [";

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
    protected final int id;
    protected final String name;

    // First time boolean - to avoid the first 0 events in emitAffinityChangeEvents
    private boolean firstTimeAffinityCPU;
    private boolean firstTimeAffinityGPU;

    protected boolean isRegistered;
    protected PipePair cPipes;
    protected PipePair rPipes;
    protected PipePair pyPipes;

    protected Invocation invocation;
    protected InvocationExecutionRequest.Listener invocationListener;
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
        LOGGER.info("Executor " + executorName + " init");
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
        if (Tracer.isActivated()) {
            emitAffinityEndEvents();
        }
        LOGGER.info("Executor " + this.name + " finished");
        Collection<ExecutionPlatformMirror<?>> mirrors = platform.getMirrors();
        for (ExecutionPlatformMirror<?> mirror : mirrors) {
            mirror.unregisterExecutor(name);
        }

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
                LOGGER.error("ERROR: Execution is null!!!!!");
            } else {
                try {
                    execution.run(this);
                } catch (ExecutorRequest.StopExecutorException se) {
                    LOGGER.debug("Stop request on Executor " + this.name);
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
     * @throws COMPSsException COMPSs exception raised by the user code
     * @throws Exception Error preparing, running or post-processing the invocation
     */
    public void processInvocation(Invocation inv, InvocationExecutionRequest.Listener listener)
        throws COMPSsException, Exception {
        this.invocation = inv;
        this.invocationListener = listener;

        boolean success = false;
        invocation.executionStarts();
        if (WORKER_DEBUG) {
            LOGGER.debug("Dequeuing job " + invocation.getJobId());
        }

        try {
            execute();
            success = true;
        } catch (COMPSsException e) {
            throw e;
        } catch (Exception e) {
            Throwable rootCause = ExceptionUtils.getRootCause(e);
            if (rootCause instanceof COMPSsException) {
                throw (COMPSsException) rootCause;
            } else {
                throw e;
            }
        } finally {

            if (WORKER_DEBUG) {
                LOGGER.debug("Job " + invocation.getJobId() + " finished (success: " + success + ")");
            }
            invocation.executionEnds();

            invocation = null;
            invocationListener = null;
        }
    }

    private void execute() throws Exception {
        if (invocation.getMethodImplementation().getMethodType() == MethodType.METHOD
            && invocation.getLang() != Lang.JAVA && invocation.getLang() != Lang.PYTHON
            && invocation.getLang() != Lang.C && invocation.getLang() != Lang.R) {
            String errMsg = "Incorrect language " + invocation.getLang() + " in job " + invocation.getJobId();
            LOGGER.error(errMsg);
            // Print to the job.err file
            this.context.getThreadErrStream().println(errMsg);
            throw new JobExecutionException("Incorrect language " + invocation.getLang());
        }

        totalTimerAndTracingWrapperAndRun();
    }

    private void logExecutionException(Exception e) {
        LOGGER.error("ERROR: Executing task " + e.getMessage(), e);

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

        long timeTotalStart = 0L;
        if (IS_TIMER_COMPSS_ENABLED) {
            timeTotalStart = System.nanoTime();
        }
        try {
            resourcesWrapperAndRun();
        } finally {
            // Always end task tracing
            if (Tracer.isActivated()) {
                emitTaskEndEvents();
            }
            // Write timer if needed
            if (IS_TIMER_COMPSS_ENABLED) {
                final long timeTotalEnd = System.nanoTime();
                final float timeTotalElapsed = (timeTotalEnd - timeTotalStart) / (float) NANO_TO_MS;
                final int jobId = invocation.getJobId();
                TIMER_LOGGER.info("[TIMER] Total time for job " + jobId + ": " + timeTotalElapsed + " ms");
            }
        }
    }

    private void resourcesWrapperAndRun() throws Exception {
        final int jobId = invocation.getJobId();

        if (IS_TIMER_COMPSS_ENABLED) {
            obtainExecutionResourcesWithTimer(jobId, invocation.getRequirements());
        } else {
            obtainExecutionResources(jobId, invocation.getRequirements());
        }

        try {
            sandBoxWrapperAndRun();
        } finally {
            if (IS_TIMER_COMPSS_ENABLED) {
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
            if (IS_TIMER_COMPSS_ENABLED) {
                twd = createTaskSandboxWithTimer();
            } else {
                twd = createTaskSandbox();
            }

            filesWrapperAndRun(twd);
            // Clean the task sandbox working dir if no error
            if (IS_TIMER_COMPSS_ENABLED) {
                cleanTaskSandboxWithTimer(twd);
            } else {
                cleanTaskSandbox(twd);
            }
        } catch (COMPSsException e) {
            // Clean the task sandbox working dir if no error
            if (IS_TIMER_COMPSS_ENABLED) {
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
            if (IS_TIMER_COMPSS_ENABLED) {
                // Bind files to task sandbox working dir
                stageInWithTimer(twd);
                redirectStreamsAndRunWithTimer(twd);
            } else {
                // Bind files to task sandbox working dir
                stageIn(twd);
                redirectStreamsAndRun(twd);
            }
        } catch (COMPSsException e) {
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
                    if (IS_TIMER_COMPSS_ENABLED) {
                        stageOutWithTimer(twd, !alreadyFailed, producesEmptyResultsOnFailure);
                    } else {
                        stageOut(twd, !alreadyFailed, producesEmptyResultsOnFailure);
                    }
                } catch (Exception ex) {
                    if (alreadyFailed) {
                        LOGGER.warn("Another exception after unbinding files: " + ex.getMessage(), ex);
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
        }
    }

    private void redirectStreamsAndRunWithTimer(ExecutionSandbox twd) throws Exception {
        int jobId = invocation.getJobId();
        // Execute task
        LOGGER.debug("Executing task invocation for Job " + jobId);
        long timeExecTaskStart = 0L;
        timeExecTaskStart = System.nanoTime();

        redirectStreamsAndRun(twd);
        final long timeExecTaskEnd = System.nanoTime();
        final float timeExecTaskElapsed = (timeExecTaskEnd - timeExecTaskStart) / (float) NANO_TO_MS;
        TIMER_LOGGER.info("[TIMER] Execute job " + jobId + ": " + timeExecTaskElapsed + " ms");
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
        } catch (COMPSsException ce) {
            LOGGER.warn("[EXECUTOR] executeTask - COMPSs Exception received");
            out.println("[EXECUTOR] executeTask - COMPSs Exception received");
            throw ce;
        } catch (Exception jee) {
            LOGGER.error("[EXECUTOR] executeTask - Error in task execution");
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

    private void runInvocation(ExecutionSandbox twd) throws COMPSsException, JobExecutionException {
        Invoker invoker;
        TimerTask timerTask = null;
        switch (invocation.getMethodImplementation().getMethodType()) {
            case METHOD:
            case MULTI_NODE:
                invoker = selectNativeMethodInvoker(twd, resources);
                timerTask = new TimeOutTask(invocation.getTaskId());
                break;
            case CONTAINER:
                invoker = new ContainerInvoker(this.context, invocation, twd, resources);
                break;
            case BINARY:
                invoker = new BinaryInvoker(this.context, invocation, twd, resources);
                break;
            case PYTHON_MPI:
                invoker = new PythonMPIInvoker(this.context, invocation, twd, resources);
                break;
            case MPI:
                invoker = new MPIInvoker(this.context, invocation, twd, resources);
                break;
            case MPMDMPI:
                invoker = new MpmdMPIInvoker(this.context, invocation, twd, resources);
                break;
            case COMPSs:
                invoker = new COMPSsInvoker(this.context, invocation, twd, resources);
                break;
            case DECAF:
                invoker = new DecafInvoker(this.context, invocation, twd, resources);
                break;
            case JULIA:
                invoker = new JuliaInvoker(this.context, invocation, twd, resources);
                break;
            case OMPSS:
                invoker = new OmpSsInvoker(this.context, invocation, twd, resources);
                break;
            case OPENCL:
                invoker = new OpenCLInvoker(this.context, invocation, twd, resources);
                break;
            default:
                throw new JobExecutionException("Undefined invoker. It could be cause by an incoherent task type");
        }
        if (timerTask == null) {
            timerTask = new TimeOutInvokerTask(invocation.getTaskId(), invoker);
        }
        try {
            this.platform.registerRunningJob(invocation, invoker, timerTask);
            invoker.runInvocation(this);
        } finally {
            timerTask.cancel();
            this.platform.unregisterRunningJob(invocation.getJobId());
        }
    }

    private Invoker selectNativeMethodInvoker(ExecutionSandbox sandbox, InvocationResources assignedResources)
        throws JobExecutionException {
        PrintStream out = context.getThreadOutStream();
        switch (invocation.getLang()) {
            case JAVA:
                Invoker javaInvoker = null;
                switch (context.getExecutionType()) {
                    case COMPSS:
                        if (context.getRuntimeAPI() != null && context.getLoaderAPI() != null) {
                            out.println("Nested Support enabled on the Invocation Context!");
                            javaInvoker = new JavaNestedInvoker(context, invocation, sandbox, assignedResources);
                        } else {
                            javaInvoker = new JavaInvoker(context, invocation, sandbox, assignedResources);
                        }
                        break;
                    case STORAGE:
                        javaInvoker = new StorageInvoker(context, invocation, sandbox, assignedResources);
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
                    pyPipes = mirror.registerExecutor(this.id, this.name);
                }
                return new PythonInvoker(context, invocation, sandbox, assignedResources, pyPipes);
            case C:
                Invoker cInvoker = null;
                if (context.isPersistentCEnabled()) {
                    cInvoker = new CPersistentInvoker(context, invocation, sandbox, assignedResources);
                    if (!isRegistered) {
                        PersistentMirror mirror;
                        synchronized (platform) {
                            mirror = (PersistentMirror) platform.getMirror(CPersistentInvoker.class);
                            if (mirror == null) {
                                mirror = CPersistentInvoker.getMirror(context, platform);
                                platform.registerMirror(CPersistentInvoker.class, mirror);
                            }
                        }
                        mirror.registerExecutor(this.id, this.name);
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
                        cPipes = mirror.registerExecutor(this.id, this.name);
                    }
                    cInvoker = new CInvoker(context, invocation, sandbox, assignedResources, cPipes);
                }
                return cInvoker;
            case R:
                if (rPipes == null) {
                    PipedMirror mirror;
                    synchronized (platform) {
                        mirror = (PipedMirror) platform.getMirror(RInvoker.class);
                        if (mirror == null) {
                            mirror = (PipedMirror) RInvoker.getMirror(context, platform);
                            platform.registerMirror(RInvoker.class, mirror);
                        }
                    }
                    rPipes = mirror.registerExecutor(this.id, this.name);

                }
                return new RInvoker(context, invocation, sandbox, assignedResources, rPipes);

            default:
                throw new JobExecutionException("Unrecognised lang for a method type invocation");
        }
    }

    private void obtainExecutionResourcesWithTimer(int jobId, ResourceDescription requirements)
        throws UnsufficientAvailableResourcesException {

        long timeAssignResourcesStart = 0L;
        timeAssignResourcesStart = System.nanoTime();
        try {
            obtainExecutionResources(jobId, requirements);
        } finally {
            final long timeAssignResourcesEnd = System.nanoTime();
            final float timeAssignResourcesElapsed =
                (timeAssignResourcesEnd - timeAssignResourcesStart) / (float) NANO_TO_MS;
            TIMER_LOGGER.debug("[TIMER] Assign resources for job " + jobId + ": " + timeAssignResourcesElapsed + " ms");
        }
    }

    private void obtainExecutionResources(int jobId, ResourceDescription requirements)
        throws UnsufficientAvailableResourcesException {
        LOGGER.debug("Assigning resources for Job " + jobId);
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
                ThreadAffinity.setCurrentThreadAffinity(this.resources.getAssignedCPUs());
            } catch (Exception e) {
                LOGGER.warn("Error setting affinity for Job " + this.invocation.getJobId(), e);
            }
        }
    }

    @Override
    public void stalledCodeExecution() {
        int jobId = invocation.getJobId();
        LOGGER.debug("Release binded resources for Job " + jobId);
        long timeUnassignResourcesStart = 0L;
        if (IS_TIMER_COMPSS_ENABLED) {
            timeUnassignResourcesStart = System.nanoTime();
        }
        this.platform.blockedRunner(invocation, this, this.resources);
        if (IS_TIMER_COMPSS_ENABLED) {
            final long timeUnassignResourcesEnd = System.nanoTime();
            final float timeUnassignResourcesElapsed =
                (timeUnassignResourcesEnd - timeUnassignResourcesStart) / (float) NANO_TO_MS;
            TIMER_LOGGER
                .debug("[TIMER] Unassign resources for job " + jobId + ": " + timeUnassignResourcesElapsed + " ms");
        }
    }

    @Override
    public void readyToContinueExecution(Semaphore sem) {
        long timeAssignResourcesStart = 0L;
        if (IS_TIMER_COMPSS_ENABLED) {
            timeAssignResourcesStart = System.nanoTime();
        }
        this.platform.unblockedRunner(invocation, this, this.resources, sem);
        assignExecutionResources();

        if (IS_TIMER_COMPSS_ENABLED) {
            final long timeAssignResourcesEnd = System.nanoTime();
            final float timeAssignResourcesElapsed =
                (timeAssignResourcesEnd - timeAssignResourcesStart) / (float) NANO_TO_MS;
            final int jobId = invocation.getJobId();
            TIMER_LOGGER
                .debug("[TIMER] Re-assign resources for job " + jobId + ": " + timeAssignResourcesElapsed + " ms");
        }
    }

    private void releaseResourcesWithTimer(int jobId) {
        long timeUnassignResourcesStart = 0L;
        timeUnassignResourcesStart = System.nanoTime();
        try {
            this.platform.releaseResources(jobId);
        } finally {
            final long timeUnassignResourcesEnd = System.nanoTime();
            final float timeUnassignResourcesElapsed =
                (timeUnassignResourcesEnd - timeUnassignResourcesStart) / (float) NANO_TO_MS;
            TIMER_LOGGER
                .debug("[TIMER] Unassign resources for job " + jobId + ": " + timeUnassignResourcesElapsed + " ms");
        }

    }

    private void releaseResources(int jobId) {
        LOGGER.debug("Release binded resources for Job " + jobId);
        this.platform.releaseResources(jobId);
    }
    /*
     * ---------------------- SANDBOX MANAGEMENT --------------------------------
     */

    private ExecutionSandbox createTaskSandboxWithTimer() throws Exception {
        // Start timer
        long timeSandboxStart = 0L;
        timeSandboxStart = System.nanoTime();
        try {
            return createTaskSandbox();
        } finally {
            final long timeSandboxEnd = System.nanoTime();
            final float timeSandboxElapsed = (timeSandboxEnd - timeSandboxStart) / (float) NANO_TO_MS;
            TIMER_LOGGER
                .debug("[TIMER] Create sandbox for job " + invocation.getJobId() + ": " + timeSandboxElapsed + " ms");
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
        LOGGER.debug("Creating task sandbox for Job " + jobId);

        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.CREATING_TASK_SANDBOX);
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
                Tracer.emitEventEnd(TraceEvent.CREATING_TASK_SANDBOX);
            }
        }
        return taskWD;
    }

    private void cleanTaskSandboxWithTimer(ExecutionSandbox twd) {
        long timeCleanSandboxStart = 0L;

        timeCleanSandboxStart = System.nanoTime();
        try {
            cleanTaskSandbox(twd);
        } finally {
            final long timeCleanSandboxEnd = System.nanoTime();
            final float timeCleanSandboxElapsed = (timeCleanSandboxEnd - timeCleanSandboxStart) / (float) NANO_TO_MS;
            final int jobId = invocation.getJobId();
            TIMER_LOGGER.debug("[TIMER] Clean sandbox for job " + jobId + ": " + timeCleanSandboxElapsed + " ms");
        }

    }

    private void cleanTaskSandbox(ExecutionSandbox twd) {
        final int jobId = invocation.getJobId();
        LOGGER.debug("Cleaning task sandbox for Job " + jobId);

        if (twd != null) {
            try {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(TraceEvent.REMOVING_TASK_SANDBOX);
                }
                twd.clean();
            } finally {
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(TraceEvent.REMOVING_TASK_SANDBOX);
                }
            }
        }
    }

    private void stageInWithTimer(ExecutionSandbox sandbox) throws Exception {
        long timeBindOriginalFilesStart = 0L;
        timeBindOriginalFilesStart = System.nanoTime();
        try {
            stageIn(sandbox);
        } finally {
            final long timeBindOriginalFilesEnd = System.nanoTime();
            final float timeBindOriginalFilesElapsed =
                (timeBindOriginalFilesEnd - timeBindOriginalFilesStart) / (float) NANO_TO_MS;
            TIMER_LOGGER.debug("[TIMER] Bind original files for job " + invocation.getJobId() + ": "
                + timeBindOriginalFilesElapsed + " ms");
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
        LOGGER.debug("Binding renamed files to sandboxed original names for Job " + jobId);
        try {

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
        } catch (Exception e) {
            logExecutionException(e);
            throw e;
        }
    }

    private void bindOriginalFilenameToRenames(InvocationParam param, ExecutionSandbox sandbox) throws IOException {

        if (Tracer.isActivated()) {
            Tracer.emitEvent(TraceEvent.BIND_ORIG_NAME);
        }

        if (param.isCollective()) {
            InvocationParamCollection<InvocationParam> cp = (InvocationParamCollection<InvocationParam>) param;
            for (InvocationParam p : cp.getCollectionParameters()) {
                bindOriginalFilenameToRenames(p, sandbox);
            }
        } else {
            if (param.getType() == DataType.FILE_T) {
                String renamedPath = (String) param.getValue();
                LOGGER.debug("Renamed File Path is " + renamedPath);
                param.setRenamedName(renamedPath);

                File renamedFile = new File(renamedPath);
                if (renamedFile.getName().equals(param.getOriginalName())) {
                    param.setOriginalName(renamedPath);
                } else {
                    String originalName = param.getOriginalName();
                    String dataId = param.getDataMgmtId();

                    String inSandboxPath = sandbox.addFile(dataId, renamedFile, originalName);
                    if (inSandboxPath != null) {
                        LOGGER.debug("Setting Original Name to " + inSandboxPath);
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
            Tracer.emitEventEnd(TraceEvent.BIND_ORIG_NAME);
        }
    }

    private void stageOutWithTimer(ExecutionSandbox sandbox, boolean raiseExceptionIfNonExistent,
        boolean createifNonExistent) throws IOException, JobExecutionException {
        long timeUnbindOriginalFilesStart = 0L;
        timeUnbindOriginalFilesStart = System.nanoTime();
        try {
            stageOut(sandbox, raiseExceptionIfNonExistent, createifNonExistent);
        } finally {
            final long timeUnbindOriginalFilesEnd = System.nanoTime();
            final float timeUnbindOriginalFilesElapsed =
                (timeUnbindOriginalFilesEnd - timeUnbindOriginalFilesStart) / (float) NANO_TO_MS;
            final int jobId = invocation.getJobId();
            TIMER_LOGGER.debug(
                "[TIMER] Unbind original files for job " + jobId + ": " + timeUnbindOriginalFilesElapsed + " ms");
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
        LOGGER.debug("Removing renamed files to sandboxed original names for Job " + jobId);
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
                Tracer.emitEvent(TraceEvent.UNBIND_ORIG_NAME);
            }
            try {
                String inSandboxPath = param.getOriginalName();
                String renamedFilePath = param.getRenamedName();

                LOGGER.debug("Treating file " + inSandboxPath);
                File internalFile = new File(inSandboxPath);
                File externalFile = new File(renamedFilePath);
                try {
                    if (!inSandboxPath.equals(renamedFilePath)) {
                        if (internalFile.exists()) {
                            sandbox.removeFile(inSandboxPath, renamedFilePath);
                        } else {
                            if (externalFile.exists()) {
                                // Is an IN and the data was already updated
                                LOGGER.debug("Repeated data for " + internalFile + ". Nothing to do");
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
                    Tracer.emitEventEnd(TraceEvent.UNBIND_ORIG_NAME);
                }
            }
        }
    }

    /*
     * ---------------------- TRACE EVENTS MAGEMENT --------------------------------
     */
    private void emitingTaskStartEvents() {
        if (Tracer.isActivated() & Tracer.isTracingTaskDependencies()) {
            if (invocation.getPredecessors() != null) {
                for (Integer i : invocation.getPredecessors()) {
                    Tracer.emitCommEvent(false, 123, 1, i, 0);
                }
            }
        }

        int numNodes = 1;
        if (invocation.getSlaveNodesNames() != null) {
            numNodes = invocation.getSlaveNodesNames().size() + 1;
        }
        int nCPUs = ((MethodResourceDescription) invocation.getRequirements()).getTotalCPUComputingUnits() * numNodes;
        Tracer.emitEvent(TraceEventType.CPU_COUNTS, nCPUs);
        int nGPUs = ((MethodResourceDescription) invocation.getRequirements()).getTotalGPUComputingUnits() * numNodes;
        Tracer.emitEvent(TraceEventType.GPU_COUNTS, nGPUs);
        int memory = (int) ((MethodResourceDescription) invocation.getRequirements()).getMemorySize() * numNodes;
        if (memory < 0) {
            memory = 0;
        }
        Tracer.emitEvent(TraceEventType.MEMORY, memory);
        int diskBW = ((MethodResourceDescription) invocation.getRequirements()).getStorageBW() * numNodes;
        if (diskBW < 0) {
            diskBW = 0;
        }
        Tracer.emitEvent(TraceEventType.DISK_BW, diskBW);
        int taskType = invocation.getMethodImplementation().getMethodType().ordinal() + 1;
        Tracer.emitEvent(TraceEventType.TASKTYPE, taskType);
        Tracer.emitEvent(TraceEvent.TASK_RUNNING);
    }

    private void emitTaskEndEvents() {
        if (Tracer.isActivated() & Tracer.isTracingTaskDependencies()) {
            for (int i = 0; i < this.invocation.getNumSuccessors(); i++) {
                Tracer.emitCommEvent(true, 123, 1, invocation.getTaskId(), 0);
            }
        }
        Tracer.emitEventEnd(TraceEventType.CPU_COUNTS);
        Tracer.emitEventEnd(TraceEventType.GPU_COUNTS);
        Tracer.emitEventEnd(TraceEventType.MEMORY);
        Tracer.emitEventEnd(TraceEventType.DISK_BW);
        Tracer.emitEventEnd(TraceEventType.TASKTYPE);
        Tracer.emitEventEnd(TraceEvent.TASK_RUNNING);
    }

    private void emitAffinityChangeEvents() {
        if (this.resources != null) {
            int[] cpus = this.resources.getAssignedCPUs();
            if (cpus != null && cpus.length > 0) {
                if (this.firstTimeAffinityCPU) {
                    this.firstTimeAffinityCPU = false;
                } else {
                    Tracer.emitEventEnd(TraceEventType.TASKS_CPU_AFFINITY);
                }
                Tracer.emitEvent(TraceEventType.TASKS_CPU_AFFINITY, cpus[0] + 1L);
            }
            int[] gpus = this.resources.getAssignedGPUs();
            if (gpus != null && gpus.length > 0) {
                if (this.firstTimeAffinityGPU) {
                    this.firstTimeAffinityGPU = false;
                } else {
                    Tracer.emitEventEnd(TraceEventType.TASKS_GPU_AFFINITY);
                }
                Tracer.emitEvent(TraceEventType.TASKS_GPU_AFFINITY, gpus[0] + 1L);
            }
        }
    }

    private void emitAffinityEndEvents() {
        if (!this.firstTimeAffinityCPU) {
            Tracer.emitEvent(TraceEventType.TASKS_CPU_AFFINITY, 0);
        }
        if (!this.firstTimeAffinityGPU) {
            Tracer.emitEvent(TraceEventType.TASKS_GPU_AFFINITY, 0);
        }
    }

}
