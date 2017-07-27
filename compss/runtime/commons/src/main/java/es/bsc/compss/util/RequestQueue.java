package es.bsc.compss.util;

import java.util.LinkedList;
import java.util.List;


/**
 * The RequestQueue class is an utility to enqueue requests from a certain type. Any component can add a Request to the
 * queue at the end or priorize the treatment of that request by adding it to the head of the queue. At any point of the
 * execution a thread can dequeue a Request from the queue to treat it, if there are no requests on the queue it falls
 * asleep until a new request is enqueued.
 * 
 * @param <T>
 *            Type of the Requests
 */
public class RequestQueue<T> {

    /** Queue of requests **/
    private final LinkedList<T> queue;

    /** Number of threads waiting for requests */
    private int waiting;


    /**
     * Constructs a new RequestQueue without any pending request nor asleep threads waiting for requests
     */
    public RequestQueue() {
        queue = new LinkedList<>();
        waiting = 0;
    }

    /**
     * Adds a request at the tail of the queue
     * 
     * @param request
     *            Request to be added
     */
    public synchronized void enqueue(T request) {
        queue.add(request);
        notify();
    }

    /**
     * Dequeues a request from the queue. If there are no pending requests, the current Thread falls asleep until a new
     * Request is added. This request will be return value of the method.
     * 
     * @return the first request from the queue
     */
    public synchronized T dequeue() {
        while (queue.isEmpty()) {
            waiting++;
            try {
                wait();
            } catch (InterruptedException e) {
                return null;
            }
            waiting--;
        }

        return queue.poll();

    }

    /**
     * Removes a request from the queue
     * 
     * @param request
     *            Request to be removed from the queue
     */
    public synchronized void remove(T request) {
        queue.remove(request);
    }

    /**
     * Adds a new request on the head of the queue
     * 
     * @param request
     *            Request to be added
     */
    public synchronized void addToFront(T request) {
        queue.addFirst(request);
        notify();
    }

    /**
     * Returns the number of pending requests
     * 
     * @return number of pending requests in the queue
     */
    public synchronized int getNumRequests() {
        return queue.size();
    }

    /**
     * Returns the whole queue of pending requests
     * 
     * @return the queue of pending requests
     */
    public synchronized List<T> getQueue() {
        return queue;
    }

    /**
     * Checks if there are pending requests on the queue
     * 
     * @return true if there are no requests waiting on the queue
     */
    public synchronized boolean isEmpty() {
        return queue.isEmpty();
    }

    /**
     * Returns the number of threads waiting for a request to be added
     * 
     * @return number of threads waiting for a request
     */
    public synchronized int getWaiting() {
        return waiting;
    }

    /**
     * Removes all the requests from the queue
     */
    public synchronized void clear() {
        queue.clear();
    }

    /**
     * Sends a signal to all the Threads that are waiting for a request.
     */
    public void wakeUpAll() {
        notifyAll();
    }

}
