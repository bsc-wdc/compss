package integratedtoolkit.util;

/**
 * The RequestDispatcher is an abstract utility to join a group of threads which execute the requests collected in a
 * RequestQueue
 *
 * @param <T>
 *            type of requests that the Dispatcher will process
 */
public abstract class RequestDispatcher<T> implements Runnable {

    /**
     * Set of threads
     */
    protected ThreadPool pool;

    /**
     * Queue where all the pending requests are collected
     */
    protected RequestQueue<T> queue;


    /**
     * Constructs a new RequestDispatcher without a pool of threads but already assigns its RequestQueue
     *
     * @param queue
     *            queue where the pending requests are
     */
    public RequestDispatcher(RequestQueue<T> queue) {
        this.queue = queue;
        this.pool = null;
    }

    /**
     * Assigns the pool of threads to the dispatcher
     *
     * @param pool
     *            pool of threads in charge of processing the requests
     */
    public void setPool(ThreadPool pool) {
        this.pool = pool;
    }

    /**
     * Thread main code which enables the request processing
     */
    @Override
    public void run() {
        processRequests();
        if (pool != null) {
            pool.threadEnd();
        }
    }

    /**
     * Returns the associated RequestQueue
     *
     * @return the associated RequestQueue
     */
    public RequestQueue<T> getQueue() {
        return queue;
    }

    /**
     * Abstract method to process the requests of the queue
     */
    protected abstract void processRequests();

}
