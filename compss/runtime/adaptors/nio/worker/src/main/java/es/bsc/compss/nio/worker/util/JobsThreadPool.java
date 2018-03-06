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
package es.bsc.compss.nio.worker.util;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.NIOTask;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.exceptions.InitializationException;
import es.bsc.compss.util.RequestQueue;


/**
 * The thread pool is an utility to manage a set of threads for job execution
 */
public abstract class JobsThreadPool {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    protected static final String JOB_THREADS_POOL_NAME = "JobsThreadPool";

    protected final NIOWorker nw;
    protected final int size;
    protected Thread[] workerThreads;
    protected final RequestQueue<NIOTask> queue;
    protected final Semaphore sem;


    /**
     * Constructs a new thread pool but not the threads inside it.
     *
     * @param size
     *            number of threads that will be in the pool
     * @param name
     *            name of the thread pool inherited by the threads
     * @param runObject
     *            Request Dispatcher associated to the pool which implements the function executed by the threads
     */
    public JobsThreadPool(NIOWorker nw, int size) {
        LOGGER.info("Init JobsThreadPool");
        this.nw = nw;
        this.size = size;

        // Instantiate worker thread structure
        this.workerThreads = new Thread[this.size];

        // Make system properties local to each thread
        System.setProperties(new ThreadProperties(System.getProperties()));

        // Instantiate the message queue and the stop semaphore
        this.queue = new RequestQueue<>();
        this.sem = new Semaphore(size);
    }

    /**
     * Adds a new task to the queue
     * 
     * @param nt
     */
    public void enqueue(NIOTask nt) {
        synchronized (queue) {
            this.queue.enqueue(nt);
        }
    }

    /**
     * Creates and starts the threads of the pool and waits until they are created
     * 
     */
    public abstract void startThreads() throws InitializationException;

    /**
     * Stops all the threads. Inserts as many null objects to the queue as threads are managed. It wakes up all the
     * threads and wait until they process the null objects inserted which will stop them.
     */
    public void stopThreads() {
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
        LOGGER.info("Performing specific stop");
        specificStop();
        joinThreads();
        LOGGER.info("ThreadPool stopped");
    }

    /**
     * Stops specific language components
     * 
     */
    protected abstract void specificStop();

    private void joinThreads() {
        for (Thread t : this.workerThreads) {
            if (t != null) {
                try {
                    t.join();
                } catch (InterruptedException e) {
                    Thread.currentThread().interrupt();
                }
            }
        }
        this.workerThreads = null;
        
        Runtime.getRuntime().gc();
    }

    /**
     * Notifies that one of the threads as completed an action required by the Threadpool (start or stop)
     */
    public void threadEnd() {
        sem.release();
    }

    /**
     * Returns the number of Threads in the pool
     *
     * @return number of Threads in the pool
     */
    public int getNumThreads() {
        return this.size;
    }
}
