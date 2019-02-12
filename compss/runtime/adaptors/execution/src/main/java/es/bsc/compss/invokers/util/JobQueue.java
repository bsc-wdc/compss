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
package es.bsc.compss.invokers.util;

import java.util.LinkedList;
import java.util.Stack;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.executor.types.Execution;
import es.bsc.compss.log.Loggers;


/**
 * The RequestQueue class is an utility to enqueue requests from a certain type. Any component can add a Request to the
 * queue at the end or prioritize the treatment of that request by adding it to the head of the queue. At any point of
 * the execution a thread can dequeue a Request from the queue to treat it, if there are no requests on the queue it
 * falls asleep until a new request is enqueued.
 * 
 * @param <T>
 *            Type of the Requests
 */
public class JobQueue {

    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);

    /** Job requests **/
    private final LinkedList<Execution> queue;

    /** Stack storing the objects blocking the waiting threads */
    private Stack<Object> waitingLocks;


    /**
     * Constructs a new RequestQueue without any pending request nor asleep threads waiting for requests
     */
    public JobQueue() {
        this.queue = new LinkedList<>();
        this.waitingLocks = new Stack<>();
    }

    /**
     * Adds a request at the tail of the queue
     * 
     * @param request
     *            Request to be added
     */
    public void enqueue(Execution request) {
        // Add new job to queue
        synchronized (this.queue) {
            this.queue.add(request);
        }

        // Wake up the last executor thread if any
        if (!this.waitingLocks.isEmpty()) {
            Object lock = this.waitingLocks.pop();
            synchronized (lock) {
                lock.notify();
            }
        }
    }

    /**
     * Dequeues a request from the queue. If there are no pending requests, the current Thread falls asleep until a new
     * Request is added. This request will be return value of the method.
     * 
     * @return the first request from the queue
     */
    public Execution dequeue() {
        boolean isEmpty;
        synchronized (this.queue) {
            isEmpty = this.queue.isEmpty();
        }
        while (isEmpty) {
            // The queue is empty, register the thread on the waiting stack
            Object lock = new Object();
            this.waitingLocks.push(lock);
            try {
                synchronized (lock) {
                    lock.wait();
                }
            } catch (InterruptedException ie) {
                LOGGER.error("ERROR: Job Thread was interrupted while waiting for next job", ie);
                return null;
            }

            // Check queue status
            synchronized (this.queue) {
                isEmpty = this.queue.isEmpty();
            }
        }

        // The queue is not empty, take the first available job
        synchronized (this.queue) {
            return this.queue.poll();
        }
    }

    /**
     * Sends a signal to all the Threads that are waiting for a request.
     */
    public void wakeUpAll() {
        // Wake up all waiting threads
        while (!this.waitingLocks.isEmpty()) {
            Object lock = this.waitingLocks.pop();
            synchronized (lock) {
                lock.notify();
            }
        }
    }

}
