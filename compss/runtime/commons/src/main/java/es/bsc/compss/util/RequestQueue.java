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

import java.util.LinkedList;
import java.util.List;


/**
 * The RequestQueue class is an utility to enqueue requests from a certain type. Any component can add a Request to the
 * queue at the end or prioritize the treatment of that request by adding it to the head of the queue. At any point of
 * the execution a thread can dequeue a Request from the queue to treat it, if there are no requests on the queue it
 * falls asleep until a new request is enqueued.
 * 
 * @param <T> Type of the Requests
 */
public class RequestQueue<T> {

    /**
     * Queue of requests.
     **/
    private final LinkedList<T> queue;

    /**
     * Number of threads waiting for requests.
     */
    private int waiting;


    /**
     * Constructs a new RequestQueue without any pending request nor asleep threads waiting for requests.
     */
    public RequestQueue() {
        this.queue = new LinkedList<>();
        this.waiting = 0;
    }

    /**
     * Adds a request at the tail of the queue.
     * 
     * @param request Request to be added
     */
    public synchronized void enqueue(T request) {
        this.queue.add(request);
        notify();
    }

    /**
     * Dequeues a request from the queue. If there are no pending requests, the current Thread falls asleep until a new
     * Request is added. This request will be return value of the method.
     * 
     * @return The first request from the queue.
     */
    public synchronized T dequeue() {
        while (this.queue.isEmpty()) {
            this.waiting++;
            try {
                wait();
            } catch (InterruptedException e) {
                return null;
            }
            this.waiting--;
        }

        return this.queue.poll();

    }

    /**
     * Removes a request from the queue.
     * 
     * @param request Request to be removed from the queue.
     */
    public synchronized void remove(T request) {
        this.queue.remove(request);
    }

    /**
     * Adds a new request on the head of the queue.
     * 
     * @param request Request to be added.
     */
    public synchronized void addToFront(T request) {
        this.queue.addFirst(request);
        notify();
    }

    /**
     * Returns the number of pending requests.
     * 
     * @return Number of pending requests in the queue.
     */
    public synchronized int getNumRequests() {
        return this.queue.size();
    }

    /**
     * Returns the whole queue of pending requests.
     * 
     * @return The queue of pending requests.
     */
    public synchronized List<T> getQueue() {
        return this.queue;
    }

    /**
     * Checks if there are pending requests on the queue.
     * 
     * @return {@code true} if there are no requests waiting on the queue, {@code false} otherwise.
     */
    public synchronized boolean isEmpty() {
        return this.queue.isEmpty();
    }

    /**
     * Returns the number of threads waiting for a request to be added.
     * 
     * @return Number of threads waiting for a request.
     */
    public synchronized int getWaiting() {
        return this.waiting;
    }

    /**
     * Removes all the requests from the queue.
     */
    public synchronized void clear() {
        this.queue.clear();
    }

    /**
     * Sends a signal to all the Threads that are waiting for a request.
     */
    public void wakeUpAll() {
        notifyAll();
    }

}
