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

import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.execution.InvocationContext;
import es.bsc.compss.types.execution.exceptions.UnsufficientAvailableComputingUnitsException;
import es.bsc.compss.types.resources.ResourceDescription;
import es.bsc.compss.executor.Executor;
import es.bsc.compss.executor.ExecutorContext;
import es.bsc.compss.executor.external.ExecutionPlatformMirror;
import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.executor.utils.ResourceManager.InvocationResources;
import es.bsc.compss.invokers.util.JobQueue;
import java.util.Collection;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.Semaphore;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import java.util.Comparator;
import java.util.Iterator;
import java.util.LinkedList;
import java.util.TreeSet;


/**
 * The thread pool is an utility to manage a set of threads for job execution
 */
public class ExecutionPlatform implements ExecutorContext {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    private final String platformName;
    private final InvocationContext context;
    private final ResourceManager rm;

    private final JobQueue queue;

    private boolean started = false;
    private int nextThreadId = 0;
    private final TreeSet<Thread> workerThreads;
    private final LinkedList<Thread> finishedWorkerThreads;

    private final Semaphore startSemaphore;
    private final Semaphore stopSemaphore;
    private final Map<Class<?>, ExecutionPlatformMirror<?>> mirrors;


    /**
     * Constructs a new thread pool but not the threads inside it.
     *
     * @param platformName
     * @param context
     * @param initialSize
     * @param resManager
     */
    public ExecutionPlatform(String platformName, InvocationContext context, int initialSize,
            ResourceManager resManager) {
        LOGGER.info("Initializing execution platform " + platformName);
        this.platformName = platformName;

        this.context = context;
        this.rm = resManager;
        this.mirrors = new HashMap<>();

        // Make system properties local to each thread
        System.setProperties(new ThreadedProperties(System.getProperties()));

        // Instantiate the message queue and the stop semaphore
        this.queue = new JobQueue();
        this.startSemaphore = new Semaphore(0);
        this.stopSemaphore = new Semaphore(0);

        // Instantiate worker thread structure
        this.workerThreads = new TreeSet<>(new Comparator<Thread>() {

            @Override
            public int compare(Thread t1, Thread t2) {
                return Long.compare(t1.getId(), t2.getId());
            }
        });
        this.finishedWorkerThreads = new LinkedList<>();
        addWorkerThreads(initialSize);

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
     */
    public final synchronized void start() {
        LOGGER.info("Starting execution platform " + this.platformName);
        // Start is in inverse order so that Thread 1 is the last available
        for (Thread t : this.workerThreads.descendingSet()) {
            t.start();
        }
        int size = this.workerThreads.size();
        this.startSemaphore.acquireUninterruptibly(size);
        this.started = true;
        LOGGER.info("Started execution platform " + this.platformName + " with " + size);
    }

    /**
     * Stops all the threads. Inserts as many null objects to the queue as threads are managed. It wakes up all the
     * threads and wait until they process the null objects inserted which will stop them.
     */
    public final synchronized void stop() {
        LOGGER.info("Stopping execution platform " + this.platformName);
        /*
         * Empty queue to discard any pending requests and make threads finish
         */
        int size = this.workerThreads.size();
        removeWorkerThreads(size);

        LOGGER.info("Stopping mirrors for execution platform " + this.platformName);
        for (ExecutionPlatformMirror<?> mirror : this.mirrors.values()) {
            mirror.stop();
        }
        mirrors.clear();

        started = false;
        LOGGER.info("Stopped execution platform " + this.platformName);
    }

    public final synchronized void addWorkerThreads(int numWorkerThreads) {
        Semaphore startSem;
        if (started) {
            startSem = new Semaphore(numWorkerThreads);
        } else {
            startSem = this.startSemaphore;
        }
        for (int i = 0; i < numWorkerThreads; i++) {
            int id = nextThreadId++;
            Executor executor = new Executor(context, this, "compute" + id) {

                @Override
                public void run() {
                    startSem.release();
                    super.run();
                    synchronized (ExecutionPlatform.this.finishedWorkerThreads) {
                        ExecutionPlatform.this.finishedWorkerThreads.add(Thread.currentThread());
                    }
                    ExecutionPlatform.this.stopSemaphore.release();
                }
            };
            Thread t = new Thread(executor);
            t.setName(platformName + " compute thread # " + id);
            workerThreads.add(t);
            if (started) {
                t.start();
            }
        }
        if (started) {
            startSem.acquireUninterruptibly(numWorkerThreads);
        }
    }

    public synchronized final void removeWorkerThreads(int numWorkerThreads) {
        LOGGER.info("Stopping " + numWorkerThreads + " executors from execution platform " + this.platformName);
        // Request N threads to finish
        for (int i = 0; i < numWorkerThreads; i++) {
            this.queue.enqueue(null);
        }
        this.queue.wakeUpAll();

        // Wait until all threads have completed their last request
        this.stopSemaphore.acquireUninterruptibly(numWorkerThreads);

        // Stop specific language components
        joinThreads();
        LOGGER.info("Stopped " + numWorkerThreads + " executors from execution platform " + this.platformName);
    }

    private void joinThreads() {
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
        // For tracing
        Runtime.getRuntime().gc();
    }

    @Override
    public int getSize() {
        return this.workerThreads.size();
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
    public ExecutionPlatformMirror<?> getMirror(Class<?> invoker) {
        return this.mirrors.get(invoker);
    }

    @Override
    public void registerMirror(Class<?> invoker, ExecutionPlatformMirror<?> mirror) {
        this.mirrors.put(invoker, mirror);
    }

    @Override
    public Collection<ExecutionPlatformMirror<?>> getMirrors() {
        return mirrors.values();
    }
}
