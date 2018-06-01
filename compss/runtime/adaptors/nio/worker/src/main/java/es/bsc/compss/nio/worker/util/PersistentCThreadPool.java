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

import java.io.IOException;


import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import es.bsc.compss.log.Loggers;
import es.bsc.compss.nio.worker.NIOWorker;
import es.bsc.compss.nio.worker.exceptions.InitializationException;
import es.bsc.compss.nio.worker.executors.CPersistentExecutor;


/**
 * Handles the bash piper script and its Gobblers The processes opened by each Thread inside the pool are managed by
 * their finish() method
 * 
 */
public class PersistentCThreadPool extends JobsThreadPool {

    // Logger
    private static final Logger LOGGER = LogManager.getLogger(Loggers.WORKER_POOL);
    private static final String LOG_PREFIX = "[PersistentCThreadPool] ";

    /**
     * Instantiates a generic external thread pool associated to the given worker and with fixed size
     * 
     * @param nw
     * @param size
     * @throws IOException
     */
    public PersistentCThreadPool(NIOWorker nw, int size) {
        super(nw, size);
    }

    /**
     * Stops specific language components. It is executed after all the threads in the pool have been stopped
     * 
     */
    @Override
    protected void specificStop() {
        // Wait for piper process builder to end
        // Check out end status and close gobblers
        // TODO: Check if specific stop is required
       // ---------------------------------------------------------------------------
        LOGGER.info("PersistentCThreadPool finished");
    }

    @Override
    public void startThreads() throws InitializationException {
        LOGGER.info(LOG_PREFIX + "Start threads for persistent C binding");
        int i = 0;
        for (Thread t : workerThreads) {
            CPersistentExecutor executor = new CPersistentExecutor(nw, this, queue);
            t = new Thread(executor);
            t.setName(JOB_THREADS_POOL_NAME + " pool thread # " + i);
            t.start();
            i = i + 1;
        }
        sem.acquireUninterruptibly(this.size);
        LOGGER.debug(LOG_PREFIX + "Finished starting persistent C Threads");
        
    }

    
}
