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
package es.bsc.compss.util;

/**
 * The RequestDispatcher is an abstract utility to join a group of threads which execute the requests collected in a
 * RequestQueue.
 *
 * @param <T> type of requests that the Dispatcher will process.
 */
public abstract class RequestDispatcher<T> implements Runnable {

    /**
     * Set of threads.
     */
    protected ThreadPool pool;

    /**
     * Queue where all the pending requests are collected.
     */
    protected RequestQueue<T> queue;


    /**
     * Constructs a new RequestDispatcher without a pool of threads but already assigns its RequestQueue.
     *
     * @param queue Queue where the pending requests are.
     */
    public RequestDispatcher(RequestQueue<T> queue) {
        this.queue = queue;
        this.pool = null;
    }

    /**
     * Assigns the pool of threads to the dispatcher.
     *
     * @param pool Pool of threads in charge of processing the requests.
     */
    public void setPool(ThreadPool pool) {
        this.pool = pool;
    }

    /**
     * Thread main code which enables the request processing.
     */
    @Override
    public void run() {
        processRequests();
        if (pool != null) {
            pool.threadEnd();
        }
    }

    /**
     * Returns the associated RequestQueue.
     *
     * @return The associated RequestQueue.
     */
    public RequestQueue<T> getQueue() {
        return queue;
    }

    /**
     * Abstract method to process the requests of the queue.
     */
    protected abstract void processRequests();

}
