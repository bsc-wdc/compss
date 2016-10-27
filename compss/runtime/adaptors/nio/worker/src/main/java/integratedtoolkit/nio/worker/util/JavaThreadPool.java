package integratedtoolkit.nio.worker.util;

import integratedtoolkit.nio.worker.NIOWorker;
import integratedtoolkit.nio.worker.exceptions.InitializationException;
import integratedtoolkit.nio.worker.executors.JavaExecutor;


public class JavaThreadPool extends JobsThreadPool {

    public JavaThreadPool(NIOWorker nw, int size) {
        super(nw, size);
    }

    /**
     * Starts the threads of the pool
     * 
     */
    public void startThreads() throws InitializationException {
        logger.info("Start threads of ThreadPool");
        int i = 0;
        for (Thread t : workerThreads) {
            JavaExecutor executor = new JavaExecutor(nw, this, queue);
            t = new Thread(executor);
            t.setName(JOB_THREADS_POOL_NAME + " pool thread # " + i++);
            t.start();
        }

        sem.acquireUninterruptibly(this.size);
    }

    /**
     * Stops specific language components
     * 
     */
    protected void specificStop() {
        // Nothing to do
    }

}
