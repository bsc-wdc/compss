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

import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.ExecutorContext;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.util.JobQueue;


/**
 * The thread pool is an utility to manage a set of threads for job execution
 */
public class ExecutionPlatform implements ExecutorContext {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    protected final InvocationContext context;
    protected final int size;
    protected final ResourceManager rm;
    protected final Thread[] workerThreads;
    protected final JobQueue queue;
    protected final Semaphore sem;
    protected final Map<Class<?>, ExecutionPlatformMirror> mirrors;


    /**
     * Constructs a new thread pool but not the threads inside it.
     *
     * @param platformName
     * @param context
     * @param size
     *            number of threads that will be in the pool
     * @param resManager
     */
    public ExecutionPlatform(String platformName, InvocationContext context, int size, ResourceManager resManager) {
        LOGGER.info("Init JobsThreadPool");
        this.context = context;
        this.size = size;

        // Make system properties local to each thread
        System.setProperties(new ThreadedProperties(System.getProperties()));

        // Instantiate the message queue and the stop semaphore
        this.queue = new JobQueue();
        this.sem = new Semaphore(size);

        // Instantiate worker thread structure
        this.workerThreads = new Thread[size];
        for (int i = 0; i < size; ++i) {
            Executor executor = new Executor(context, this, "compute" + i) {

                @Override
                public void run() {
                    super.run();
                    ExecutionPlatform.this.sem.release();
                }
            };
            Thread t = new Thread(executor);
            t.setName(platformName + " compute thread # " + i);
            this.workerThreads[i] = t;
        }
        this.rm = resManager;
        this.mirrors = new HashMap<>();
    }

    /**
     * Adds a new task to the queue
     *
     * @param exec
     */
    public void execute(Execution exec) {
        this.queue.enqueue(exec);
    }

    /**
     * Creates and starts the threads of the pool and waits until they are created
     *
     *
     */
    public void start() {
        LOGGER.info("Start threads of ThreadPool");
        // Start is in inverse order so that Thread 1 is the last available
        for (int i = this.workerThreads.length - 1; i >= 0; --i) {
            this.workerThreads[i].start();
        }

        sem.acquireUninterruptibly(this.size);
    }

    /**
     * Stops all the threads. Inserts as many null objects to the queue as threads are managed. It wakes up all the
     * threads and wait until they process the null objects inserted which will stop them.
     */
    public void stop() {
        LOGGER.info("Stopping Jobs Thread Pool");
        /*
         * Empty queue to discard any pending requests and make threads finish
         */
        for (int i = 0; i < this.size; i++) {
            this.queue.enqueue(null);
        }
        this.queue.wakeUpAll();

        // Wait until all threads have completed their last request
        this.sem.acquireUninterruptibly(this.size);

        // Stop specific language components
        joinThreads();
        LOGGER.info("ThreadPool stopped");

        LOGGER.info("Stopping mirrors");
        for (ExecutionPlatformMirror mirror : this.mirrors.values()) {
            mirror.stop();
        }
    }

    private void joinThreads() {
        for (Thread t : this.workerThreads) {
            if (t != null) {
                try {
                    t.join();
                    t = null;
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        // For tracing
        Runtime.getRuntime().gc();
    }

    @Override
    public int getSize() {
        return this.size;
    }

    @Override
    public Execution getJob() {
        return this.queue.dequeue();
    }

    @Override
    public InvocationResources acquireResources(int jobId, ResourceDescription requirements)
            throws UnsufficientAvailableComputingUnitsException {

        return this.rm.acquireResources(jobId, requirements);
    }

    @Override
    public void releaseResources(int jobId) {
        this.rm.releaseResources(jobId);
    }

    @Override
    public ExecutionPlatformMirror getMirror(Class<?> invoker) {
        return this.mirrors.get(invoker);
    }

    @Override
    public void registerMirror(Class<?> invoker, ExecutionPlatformMirror mirror) {
        this.mirrors.put(invoker, mirror);
    }

    @Override
    public Collection<ExecutionPlatformMirror> getMirrors() {
        return mirrors.values();
    }
}
