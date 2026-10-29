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
package es.bsc.compss.execution;

import es.bsc.compss.COMPSsConstants;
import es.bsc.compss.execution.types.ExecutorContext;
import es.bsc.compss.execution.types.InvocationResources;
import es.bsc.compss.execution.utils.JobQueue;
import es.bsc.compss.execution.utils.ResourceManager;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.InvocationRunner;
import es.bsc.compss.executor.external.ExecutorContextMirror;
import es.bsc.compss.executor.external.ExternalExecutor;
import es.bsc.compss.executor.external.MirrorFactory;
import es.bsc.compss.invokers.Invoker;
import es.bsc.compss.invokers.InvokerFactory;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.ActivateExecutorRequest;
import es.bsc.compss.types.execution.ExecutorRequest;
import es.bsc.compss.types.execution.Invocation;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.StopExecutorRequest;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableResourcesException;
import es.bsc.compss.types.implementations.MethodType;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.types.tracing.ExecutorInfraEvent;
import es.bsc.compss.types.tracing.ExecutorInfraType;
import es.bsc.compss.types.tracing.TaskExecutionEvent;
import es.bsc.compss.utils.execution.ThreadedProperties;
import es.bsc.wdc.tracing.Tracer;
import java.util.Comparator;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.Map;
import java.util.Set;
import java.util.Timer;
import java.util.TimerTask;
import java.util.TreeSet;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.Semaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * The thread pool is an utility to manage a set of threads for job execution.
 */
public class ExecutionPlatform implements ExecutorContext {

    public static final int EXECUTION_TIMER_COMPONENT_ID = 6;
    public static final int EXECUTOR_COMPONENT_ID = 8;

    private static final Logger logger = LogManager.getLogger(Loggers.WORKER_POOL);

    private final String platformName;
    private final InvocationContext context;
    private final ResourceManager rm;

    private boolean reuseResourcesOnBlockedInvocation = true;
    private final JobQueue queue;

    private boolean started = false;
    private int nextThreadId = 0;
    private final TreeSet<Thread> workerThreads;
    private final Semaphore frozenSemaphore;
    private final LinkedList<Thread> finishedWorkerThreads;

    private final Semaphore startSemaphore;
    private final Semaphore stopSemaphore;
    private final Map<Class<?>, ExecutorContextMirror<?>> mirrors;

    private Timer timer;
    private final Set<Integer> toCancel;
    private final Map<Integer, RunningJob> executingJobs;


    private static class RunningJob extends TimerTask {

        private final int taskId;
        private final Invoker<?> invoker;


        private RunningJob(int taskId, Invoker<?> invoker) {
            this.taskId = taskId;
            this.invoker = invoker;
        }

        @Override
        public void run() {
            logger.info("Task {} timed out", this.taskId);
            if (Tracer.isActivated()) {
                Tracer.emitEvent(TaskExecutionEvent.TASK_TIMEOUT);
            }
            invoker.cancel();
            if (Tracer.isActivated()) {
                Tracer.emitEventEnd(TaskExecutionEvent.TASK_TIMEOUT);
            }
        }
    }


    /**
     * Constructs a new thread pool but not the threads inside it.
     *
     * @param platformName Platform name
     * @param context Invocation Context
     * @param config configuration of the execution platform
     * @param resManager Resource Manager
     */
    public ExecutionPlatform(String platformName, InvocationContext context, ExecutionPlatformConfiguration config,
        ResourceManager resManager) {
        logger.info("Initializing execution platform {}", platformName);
        this.platformName = platformName;

        this.context = context;
        this.rm = resManager;
        this.mirrors = new HashMap<>();

        // Make system properties local to each thread
        System.setProperties(new ThreadedProperties(System.getProperties()));

        // Instantiate the message queue and the stop semaphore
        this.queue = new JobQueue();
        this.startSemaphore = new Semaphore(0);
        this.frozenSemaphore = new Semaphore(0);
        this.stopSemaphore = new Semaphore(0);

        // Instantiate worker thread structure
        this.reuseResourcesOnBlockedInvocation = config.isReuseResourcesOnBlockedRunner();
        this.workerThreads = new TreeSet<>(new Comparator<Thread>() {

            @Override
            public int compare(Thread t1, Thread t2) {
                return Long.compare(t1.getId(), t2.getId());
            }
        });
        this.finishedWorkerThreads = new LinkedList<>();

        this.toCancel = new HashSet<>();
        this.executingJobs = new ConcurrentHashMap<>();

        addWorkerThreads(config.getInitialSize());
    }

    @Override
    public InvocationContext getContext() {
        return context;
    }

    /**
     * Adds a new task to the queue.
     *
     * @param exec Task execution description
     */
    public void execute(ExecutorRequest exec) {
        this.queue.enqueue(exec);
    }

    /**
     * Creates and starts the threads of the pool and waits until they are created.
     */
    public final synchronized void start() {
        logger.info("Starting execution platform {}", this.platformName);
        startTimer();
        // Start is in inverse order so that Thread 1 is the last available
        for (Thread t : this.workerThreads.descendingSet()) {
            logger.info("Starting Thread " + t.getName());
            t.start();
        }
        int size = this.workerThreads.size();
        this.startSemaphore.acquireUninterruptibly(size);
        this.started = true;
        logger.info("Started execution platform {} with {}", this.platformName, size);
    }

    /**
     * Creates and starts the Mirror for the lang.
     */
    public final synchronized void startMirror(MethodType mt, COMPSsConstants.Lang l) {
        InvokerFactory.Registry.preLoad(this, mt, l);
    }

    /**
     * Stops all the threads. Inserts as many null objects to the queue as threads are managed. It wakes up all the
     * threads and wait until they process the null objects inserted which will stop them.
     */
    public final void stop() {
        logger.info("Stopping execution platform {}", this.platformName);
        /*
         * Empty queue to discard any pending requests and make threads finish
         */
        int size = this.workerThreads.size();
        removeWorkerThreads(size);
        logger.info("Stopping mirrors for execution platform {}", this.platformName);
        for (ExecutorContextMirror<?> mirror : this.mirrors.values()) {
            mirror.stop();
        }
        this.mirrors.clear();
        stopTimer();
        this.started = false;
        logger.info("Stopped execution platform {}", this.platformName);
    }

    private void startTimer() {
        if (Tracer.isActivated()) {
            Tracer.enablePThreads(1);
        }
        this.timer = new Timer(platformName + " deadline reapper");
        if (Tracer.isActivated()) {
            this.timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    Tracer.disablePThreads(1);
                    Tracer.activeComponent(EXECUTION_TIMER_COMPONENT_ID, "Timer");
                }
            }, 0);
        }
    }

    private void stopTimer() {
        if (Tracer.isActivated()) {
            Semaphore sem = new Semaphore(0);
            this.timer.schedule(new TimerTask() {

                @Override
                public void run() {
                    Tracer.inactiveComponent();
                    sem.release();
                }
            }, 0);
            try {
                sem.acquire();
            } catch (InterruptedException ie) {
                // No need to do anything
            }
        }
        this.timer.cancel();
    }

    /**
     * Add worker threads to ExecutorRequest Platform.
     *
     * @param numWorkerThreads Number of new worker threads
     */
    public final void addWorkerThreads(int numWorkerThreads) {
        boolean wait = false;
        Semaphore startSem;
        synchronized (this) {
            int numFrozenThreads = this.frozenSemaphore.getQueueLength();
            if (numFrozenThreads > 0) {
                logger.info("Recovering {} frozen executors", numFrozenThreads);
                int wakeUpThreads = numFrozenThreads;
                if (numFrozenThreads > numWorkerThreads) {
                    wakeUpThreads = numWorkerThreads;
                }
                this.frozenSemaphore.release(wakeUpThreads);
                numWorkerThreads = numWorkerThreads - wakeUpThreads;
            }

            if (this.started) {
                startSem = new Semaphore(numWorkerThreads);
                wait = true;
            } else {
                startSem = this.startSemaphore;
            }
            if (Tracer.isActivated()) {
                Tracer.enablePThreads(numWorkerThreads);
            }
            for (int i = 0; i < numWorkerThreads; i++) {
                int executorId = this.nextThreadId++;
                Executor e = createExecutor(executorId, startSem);
                Thread t = new Thread(e);
                t.setName(this.platformName + " executor thread # " + executorId);
                this.workerThreads.add(t);
                if (this.started) {
                    t.start();
                }
            }
        }
        if (wait) {
            startSem.acquireUninterruptibly(numWorkerThreads);
        }
    }

    private Executor createExecutor(int executorId, Semaphore startSem) {
        return new Executor(this.context, this, executorId, "executor" + executorId) {

            @Override
            public void run() {
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(ExecutorInfraEvent.EXECUTOR_COUNTS);
                    Tracer.activeComponent(EXECUTOR_COMPONENT_ID, "Executor thread");
                    Tracer.emitEvent(ExecutorInfraType.EXECUTOR_IDENTIFICATION, executorId);
                    Tracer.emitEvent(ExecutorInfraEvent.EXECUTOR_ACTIVE);
                    Tracer.disablePThreads(1);
                }
                startSem.release();
                super.run();
                synchronized (ExecutionPlatform.this) {
                    ExecutionPlatform.this.finishedWorkerThreads.add(Thread.currentThread());
                }
                ExecutionPlatform.this.stopSemaphore.release();
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(ExecutorInfraEvent.EXECUTOR_ACTIVE);
                    Tracer.inactiveComponent();
                    Tracer.emitEventEnd(ExecutorInfraEvent.EXECUTOR_COUNTS);
                }
            }

        };
    }

    /**
     * Remove worker threads from execution platform.
     * 
     * @param numWorkerThreads Number of worker threads to reduce
     */
    public final void removeWorkerThreads(int numWorkerThreads) {
        if (numWorkerThreads > 0) {
            logger.info("Stopping {} executors from execution platform {}", numWorkerThreads, this.platformName);
            synchronized (this) {
                int numFrozenThreads = this.frozenSemaphore.getQueueLength();
                if (numFrozenThreads > 0) {
                    logger.info("Resuming {} frozen executors to stop them", numFrozenThreads);
                    int wakeUpThreads = numFrozenThreads;
                    if (numFrozenThreads > numWorkerThreads) {
                        wakeUpThreads = numWorkerThreads;
                    }
                    this.frozenSemaphore.release(wakeUpThreads);
                }
            }
            // if (Tracer.basicModeEnabled()) {
            // Tracer.enablePThreads();
            // } // Request N threads to finish
            for (int i = 0; i < numWorkerThreads; i++) {
                this.queue.enqueue(new StopExecutorRequest());
            }
            logger.info("Waking up all locks");
            this.queue.wakeUpAll();

            // Wait until all threads have completed their last request
            logger.info("Waiting for {} threads finished", numWorkerThreads);
            this.stopSemaphore.acquireUninterruptibly(numWorkerThreads);

            // Stop specific language components
            joinThreads();
            logger.info("Stopped {} executors from execution platform {}", numWorkerThreads, this.platformName);
        }
    }

    private void joinThreads() {
        synchronized (this) {
            Iterator<Thread> iter = this.finishedWorkerThreads.iterator();
            while (iter.hasNext()) {
                Thread t = iter.next();
                if (t != null) {
                    try {
                        t.join();
                        iter.remove();
                        this.workerThreads.remove(t);
                        t = null;
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                }
            }
        }
        // For tracing
        Runtime.getRuntime().gc();
    }

    /**
     * Cancels a running job or sets it to cancel if it is not running.
     *
     * @param jobId Id of the job to cancel.
     */
    public void cancelJob(int jobId) {
        RunningJob job = this.executingJobs.get(jobId);
        if (job == null) {
            logger.debug("Job {} is to be cancelled", jobId);
            this.toCancel.add(jobId);
        } else {
            logger.debug("Cancelling running job {}", jobId);
            // Cancel timeout task (if enabled)
            job.cancel();
            // Interrupt job execution
            job.invoker.cancel();
        }
    }

    @Override
    public void registerRunningJob(Invocation invocation, Invoker invoker) {
        int jobId = invocation.getJobId();
        logger.debug("Registering job {}", jobId);
        RunningJob job = new RunningJob(invocation.getTaskId(), invoker);
        this.executingJobs.put(jobId, job);
        if (this.toCancel.contains(jobId)) {
            cancelJob(jobId);
        } else {
            long timeout = invocation.getTimeOut();
            if (timeout > 0) {
                logger.debug("Setting timeout for job {} with timeout {}", jobId, timeout);
                // set timeout (in seconds)
                timer.schedule(job, timeout * 1000);
            }
        }
    }

    @Override
    public InvocationResources acquireResources(int jobId, ResourceDescription requirements,
        InvocationResources preferredAllocation) throws UnsufficientAvailableResourcesException {

        return this.rm.acquireResources(jobId, requirements, preferredAllocation);
    }

    @Override
    public void blockedRunner(Invocation invocation, InvocationRunner runner, InvocationResources assignedResources) {
        logger.debug("Stalled execution of job {}", invocation.getJobId());
        if (reuseResourcesOnBlockedInvocation) {
            logger.debug("Releasing resources assigned to job {}", invocation.getJobId());
            int jobId = invocation.getJobId();
            this.rm.releaseResources(jobId);
            this.addWorkerThreads(1);
            this.context.idleReservedResourcesDetected(invocation.getRequirements());
        }
    }

    @Override
    public void unblockedRunner(Invocation invocation, InvocationRunner runner, InvocationResources previousAllocation,
        Semaphore sem) {
        logger.debug("Execution of job {} ready to continue", invocation.getJobId());
        if (reuseResourcesOnBlockedInvocation) {
            logger.debug("Reacquiring resources assigned to job {}", invocation.getJobId());
            this.context.reactivatedReservedResourcesDetected(invocation.getRequirements());
            this.freezeThreads(1);
            int jobId = invocation.getJobId();
            this.rm.reacquireResources(jobId, invocation.getRequirements(), previousAllocation, sem);
        } else {
            sem.release();
        }
    }

    private void freezeThreads(int numThreads) {
        final Semaphore freezeSem = new Semaphore(0);
        ActivateExecutorRequest.Listener el = new ActivateExecutorRequest.Listener() {

            @Override
            public void onActivation() {
                freezeSem.release();
                if (Tracer.isActivated()) {
                    Tracer.emitEventEnd(ExecutorInfraEvent.EXECUTOR_ACTIVE);
                }
                String threadName = Thread.currentThread().getName();
                logger.info("{} freezes", threadName);
                ExecutionPlatform.this.frozenSemaphore.acquireUninterruptibly();
                logger.info("{} unfreezes", threadName);
                if (Tracer.isActivated()) {
                    Tracer.emitEvent(ExecutorInfraEvent.EXECUTOR_ACTIVE);
                }
            }
        };
        for (int i = 0; i < numThreads; i++) {
            queue.enqueue(new ActivateExecutorRequest(el));
        }
        freezeSem.acquireUninterruptibly(numThreads);
    }

    @Override
    public void releaseResources(int jobId) {
        this.rm.releaseResources(jobId);
    }

    @Override
    public void unregisterRunningJob(int jobId) {
        logger.debug("Unregistering job {}", jobId);
        RunningJob job = this.executingJobs.remove(jobId);
        // Cancel timeout interruption, if enabled
        if (job != null) {
            job.cancel();
        }
        toCancel.remove(jobId);
    }

    @Override
    public void finishedExecutor(Executor executor) {
        for (ExecutorContextMirror<?> mirror : mirrors.values()) {
            mirror.unregisterExecutor(executor);
        }
    }

    @Override
    public int getSize() {
        return this.workerThreads.size();
    }

    @Override
    public ExecutorRequest getJob() {
        return this.queue.dequeue();
    }

    @Override
    public ExecutorRequest newThread() {
        return this.queue.newThreadDequeue();
    }

    @Override
    public ExecutorContextMirror<?> getMirror(Class<?> invoker) {
        return this.mirrors.get(invoker);
    }

    @Override
    public void registerMirror(Class<?> invoker, ExecutorContextMirror<?> mirror) {
        this.mirrors.put(invoker, mirror);
    }

    @Override
    public <E extends ExternalExecutor, M extends ExecutorContextMirror<E>> E getMirroredExecutor(Executor executor,
        Class<M> mirrorClass, MirrorFactory<M> mirrorFactory) {

        M mirror;
        // The mirror registry is not synchronized, so the find-or-create of the mirror runs on the platform lock
        synchronized (this) {
            mirror = mirrorClass.cast(this.mirrors.get(mirrorClass));
            if (mirror == null) {
                mirror = mirrorFactory.create(this);
                this.mirrors.put(mirrorClass, mirror);
            }
        }
        return mirror.getMirroredExecutor(executor);
    }
}
