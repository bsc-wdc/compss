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
package es.bsc.compss.connectors.utils;

import es.bsc.compss.connectors.ConnectorException;
import es.bsc.compss.connectors.VM;
import es.bsc.compss.log.Loggers;
import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;

import java.util.concurrent.atomic.AtomicInteger;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;


/**
 * Support thread for VM destruction.
 */
public class DeletionThread extends Thread {

    private static final Logger RESOURCE_LOGGER = LogManager.getLogger(Loggers.CONNECTORS_UTILS);
    private static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    private static final boolean DEBUG = RESOURCE_LOGGER.isDebugEnabled();

    private static final AtomicInteger COUNT = new AtomicInteger(0);

    private final Operations operations;
    private final CloudMethodWorker worker;
    private final CloudMethodResourceDescription reduction;
    private VM vm;


    /**
     * Creates a new support thread for VM reduction with the given properties.
     *
     * @param ops Operations to perform.
     * @param worker Associated worker.
     * @param reduction Reduction description.
     */
    public DeletionThread(Operations ops, CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        this.setName("DeletionThread " + worker.getName());

        this.operations = ops;
        this.worker = worker;
        this.reduction = reduction;
        this.vm = null;

        COUNT.incrementAndGet();
    }

    /**
     * Creates a new support thread for VM destruction with the given properties.
     *
     * @param ops Operations to perform.
     * @param vm Associated VM description.
     */
    public DeletionThread(Operations ops, VM vm) {
        this.setName("DeletionThread " + vm.getName());

        this.operations = ops;
        this.worker = null;
        this.reduction = null;
        this.vm = vm;

        COUNT.incrementAndGet();
    }

    @Override
    public void run() {
        if (this.reduction != null) {
            if (DEBUG) {
                RUNTIME_LOGGER.debug("[Deletion Thread] Pausing worker " + this.worker.getName());
            }
            this.vm = this.operations.pause(this.worker);
        }

        if (this.vm != null) {
            CloudMethodWorker cloudWorker = this.vm.getWorker();
            // I think this part now is not needed
            /*
             * if (cloudWorker.shouldBeStopped()) { cloudWorker.retrieveUniqueData(); cloudWorker.retrieveTracingData();
             * Semaphore sem = new Semaphore(0); ShutdownListener sl = new ShutdownListener(sem);
             * RUNTIME_LOGGER.info("[Deletion Thread] Stopping worker " + cloudWorker.getName() + "...");
             * cloudWorker.stop(sl);
             * 
             * sl.enable(); try { sem.acquire(); } catch (Exception e) {
             * RESOURCE_LOGGER.error("ERROR: Exception raised on worker shutdown"); } if (DEBUG) {
             * RUNTIME_LOGGER.debug("[Deletion Thread] Stopping worker " + cloudWorker.getName() + "..."); } } else if
             * (DEBUG) { RUNTIME_LOGGER.debug("[Deletion Thread] Worker " + cloudWorker.getName() +
             * " should not be stopped."); }
             */
            if (DEBUG) {
                RUNTIME_LOGGER
                    .debug("[Deletion Thread] Worker " + cloudWorker.getName() + " stopped. Powering of the VM");
            }
            try {
                this.operations.poweroff(this.vm);
            } catch (ConnectorException e) {
                RESOURCE_LOGGER.error("ERROR: Powering off the resource", e);
            }

        }

        int count = COUNT.decrementAndGet();
        if (DEBUG) {
            RUNTIME_LOGGER.debug("Number of current VMs deletions decreased (" + count + ").");
        }
    }

    /**
     * Returns the number of active deletion threads.
     *
     * @return The number of active deletion threads.
     */
    public static int getCount() {
        return COUNT.get();
    }

}
