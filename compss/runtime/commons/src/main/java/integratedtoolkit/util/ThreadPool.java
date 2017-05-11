package integratedtoolkit.util;

import java.util.concurrent.Semaphore;


/**
 * The threadpool is an utility to manage a set of threads
 */
public class ThreadPool {

    private final int size;
    private final String name;
    private final Thread[] workerThreads;
    private final RequestDispatcher<?> runObject;
    private final RequestQueue<?> queue;
    private final Semaphore sem;


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
    public ThreadPool(int size, String name, RequestDispatcher<?> runObject) {
        this.size = size;

        this.workerThreads = new Thread[this.size];
        this.name = name;

        this.runObject = runObject;
        this.runObject.setPool(this);

        this.queue = runObject.getQueue();

        this.sem = new Semaphore(size);
    }

    /**
     * Creates and starts the threads of the pool and waits until they are created
     * 
     */
    public void startThreads() {
        int i = 0;
        for (Thread t : workerThreads) {
            t = new Thread(runObject);
            t.setName(name + " pool thread # " + i++);
            t.start();
        }

        sem.acquireUninterruptibly(this.size);
    }

    /**
     * Stops all the threads. Inserts as many null objects to the queue as threads are managed. It wakes up all the
     * threads and wait until they process the null objects inserted which will stop them.
     */
    public void stopThreads() {
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
