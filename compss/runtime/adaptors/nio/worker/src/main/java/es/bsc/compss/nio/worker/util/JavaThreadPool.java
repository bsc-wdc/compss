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

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.exceptions.InitializationException;
import es.bsc.compss.nio.worker.executors.JavaExecutor;


/**
 * Representation of a Java Thread Pool
 *
 */
public class JavaThreadPool extends JobsThreadPool {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);


    /**
     * Creates a new Java Thread Pool associated to the given worker and with fixed size
     * 
     * @param nw
     * @param size
     */
    public JavaThreadPool(NIOWorker nw, int size) {
        super(nw, size);
    }

    /**
     * Starts the threads of the pool
     * 
     */
    @Override
    public void startThreads() throws InitializationException {
        LOGGER.info("Start threads of ThreadPool");
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
    @Override
    protected void specificStop() {
        // Nothing to do
    }

}
