package integratedtoolkit.nio.worker.util;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import integratedtoolkit.log.Loggers;
import integratedtoolkit.nio.NIOTask;
import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.util.RequestQueue;


/**
 * The thread pool is an utility to manage a set of threads for job execution
 */
public abstract class JobsThreadPool {

    protected static final Logger logger = LogManager.getLogger(Loggers.WORKER_POOL);
    protected static final boolean debug = logger.isDebugEnabled();

    protected static final String JOB_THREADS_POOL_NAME = "JobsThreadPool";

    protected final NIOWorker nw;
    protected final int size;
    protected final Thread[] workerThreads;
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
        logger.info("Init JobsThreadPool");
        this.nw = nw;
        this.size = size;

        this.workerThreads = new Thread[this.size];
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
        logger.info("Stopping Jobs Thread Pool");
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
        logger.info("Performing specific stop");
        specificStop();

        logger.info("ThreadPool stopped");
    }

    /**
     * Stops specific language components
     * 
     */
    protected abstract void specificStop();

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
