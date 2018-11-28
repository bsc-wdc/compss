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

import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.ExecutorContext;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.util.RequestQueue;
import java.util.HashMap;
import java.util.Map;


/**
 * The thread pool is an utility to manage a set of threads for job execution
 */
public class ExecutionPlatform implements ExecutorContext {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    protected final InvocationContext context;
    protected final int size;
    protected final ResourceManager rm;
    protected final Thread[] workerThreads;
    protected final RequestQueue<Execution> queue;
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
        this.queue = new RequestQueue<>();
        this.sem = new Semaphore(size);

        // Instantiate worker thread structure
        this.workerThreads = new Thread[size];
        for (int i = 0; i < size; i++) {
            Thread t;
            Executor executor = new Executor(context, this, "compute" + i) {

                @Override
                public void run() {
                    super.run();
                    ExecutionPlatform.this.sem.release();
                }
            };
            t = new Thread(executor);
            t.setName(platformName + " compute thread # " + i);
            workerThreads[i] = t;
        }
        this.rm = resManager;
        mirrors = new HashMap<>();
    }

    /**
     * Adds a new task to the queue
     *
     * @param exec
     */
    public void execute(Execution exec) {
        synchronized (queue) {
            this.queue.enqueue(exec);
        }
    }

    /**
     * Creates and starts the threads of the pool and waits until they are created
     *
     *
     */
    public void start() {
        LOGGER.info("Start threads of ThreadPool");
        for (Thread t : workerThreads) {
            t.start();
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
        synchronized (queue) {
            for (int i = 0; i < this.size; i++) {
                queue.addToFront(null);
            }
            queue.wakeUpAll();
        }

        // Wait until all threads have completed their last request
        sem.acquireUninterruptibly(this.size);

        // Stop specific language components
        joinThreads();
        LOGGER.info("ThreadPool stopped");

        LOGGER.info("Stopping mirrors");
        for (ExecutionPlatformMirror mirror : mirrors.values()) {
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
        Runtime.getRuntime().gc();
    }

    @Override
    public int getSize() {
        return size;
    }

    @Override
    public Execution getJob() {
        return queue.dequeue();
    }

    @Override
    public InvocationResources acquireComputingUnits(int jobId, ResourceDescription requirements)
            throws UnsufficientAvailableComputingUnitsException {
        return rm.acquireComputingUnits(jobId, requirements);
    }

    @Override
    public void releaseComputingUnits(int jobId) {
        rm.releaseComputingUnits(jobId);
    }

    @Override
    public ExecutionPlatformMirror getMirror(Class<?> invoker) {
        return mirrors.get(invoker);
    }

    @Override
    public void registerMirror(Class<?> invoker, ExecutionPlatformMirror mirror) {
        mirrors.put(invoker, mirror);
    }

}
