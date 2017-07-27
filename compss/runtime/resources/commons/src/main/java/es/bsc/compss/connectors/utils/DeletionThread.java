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
 * Support thread for VM destruction
 *
 */
public class DeletionThread extends Thread {

    private static final Logger RESOURCE_LOGGER = LogManager.getLogger(Loggers.CONNECTORS_UTILS);
    private static final Logger RUNTIME_LOGGER = LogManager.getLogger(Loggers.RM_COMP);
    private static final boolean DEBUG = RESOURCE_LOGGER.isDebugEnabled();

    private final static AtomicInteger COUNT = new AtomicInteger(0);

    private final Operations operations;
    private final CloudMethodWorker worker;
    private final CloudMethodResourceDescription reduction;
    private VM vm;


    /**
     * Creates a new support thread for VM reduction with the given properties
     *
     * @param connector
     * @param worker
     * @param reduction
     */
    public DeletionThread(Operations connector, CloudMethodWorker worker, CloudMethodResourceDescription reduction) {
        this.setName("DeletionThread " + worker.getName());

        this.operations = connector;
        this.worker = worker;
        this.reduction = reduction;
        this.vm = null;

        COUNT.incrementAndGet();
    }

    /**
     * Creates a new support thread for VM destruction with the given properties
     *
     * @param connector
     * @param vm
     */
    public DeletionThread(Operations connector, VM vm) {
        this.setName("DeletionThread " + vm.getName());

        this.operations = connector;
        this.worker = null;
        this.reduction = null;
        this.vm = vm;

        COUNT.incrementAndGet();
    }

    @Override
    public void run() {
        if (reduction != null) {
            if (DEBUG) {
                RUNTIME_LOGGER.debug("[Deletion Thread] Pausing worker " + worker.getName());
            }
            this.vm = this.operations.pause(worker);
        }
        if (vm != null) {
            CloudMethodWorker cloudWorker = vm.getWorker();
            // I think this part now is not needed
            /*
             * if (cloudWorker.shouldBeStopped()) { cloudWorker.retrieveData(true); Semaphore sem = new Semaphore(0);
             * ShutdownListener sl = new ShutdownListener(sem); RUNTIME_LOGGER.info("[Deletion Thread] Stopping worker "
             * + cloudWorker.getName() + "..."); cloudWorker.stop(sl);
             * 
             * sl.enable(); try { sem.acquire(); } catch (Exception e) {
             * RESOURCE_LOGGER.error("ERROR: Exception raised on worker shutdown"); } if (DEBUG) {
             * RUNTIME_LOGGER.debug("[Deletion Thread] Stopping worker " + cloudWorker.getName() + "..."); } } else if
             * (DEBUG) { RUNTIME_LOGGER.debug("[Deletion Thread] Worker " + cloudWorker.getName() +
             * " should not be stopped."); }
             */
            if (DEBUG) {
                RUNTIME_LOGGER.debug("[Deletion Thread] Worker " + cloudWorker.getName() + " stopped. Powering of the VM");
            }
            try {
                this.operations.poweroff(vm);
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
     * Returns the number of active deletion threads
     *
     * @return
     */
    public static int getCount() {
        return COUNT.get();
    }

}
