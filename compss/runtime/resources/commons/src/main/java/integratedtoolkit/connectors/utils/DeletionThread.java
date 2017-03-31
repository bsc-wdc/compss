package integratedtoolkit.connectors.utils;

import integratedtoolkit.connectors.ConnectorException;
import integratedtoolkit.connectors.VM;
import integratedtoolkit.log.Loggers;
import integratedtoolkit.types.resources.CloudMethodWorker;
import integratedtoolkit.types.resources.ShutdownListener;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;
import integratedtoolkit.util.ResourceManager;

import java.util.concurrent.Semaphore;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Support thread for VM destruction
 *
 */
public class DeletionThread extends Thread {

    private final Operations operations;
    private final CloudMethodWorker worker;
    private final CloudMethodResourceDescription reduction;
    private VM vm;
    
    private static final Object countSynchronizer = new Object();
    private static Integer count = 0;

    private static final Logger resourcesLogger = LogManager.getLogger(Loggers.CONNECTORS_UTILS);
    private static final Logger runtimeLogger = LogManager.getLogger(Loggers.RM_COMP);
    private static final boolean debug = resourcesLogger.isDebugEnabled();


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
        
        synchronized (countSynchronizer) {
            count++;
        }
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
        
        synchronized (countSynchronizer) {
            count++;
        }
    }

    @Override
    public void run() {
        if (reduction != null) {
        	
        		Semaphore sem = ResourceManager.reduceCloudWorker(worker, reduction);
        		try {
        			if (sem != null) {
        				if (debug) {
        					runtimeLogger.debug("[Deletion Thread] Waiting until all tasks finishes for resource " + worker.getName() + "...");
        				}
        				sem.acquire();
        			}
        		} catch (InterruptedException e) {
        			Thread.currentThread().interrupt();
        		}
        	
            if (debug) {
                runtimeLogger.debug("[Deletion Thread] All tasks finished for resource " + worker.getName() + ". Pausing worker...");
            }
            this.vm = this.operations.pause(worker);
        }
        if (vm != null) {
            CloudMethodWorker cloudWorker = vm.getWorker();
            if (cloudWorker.shouldBeStopped()) {
                cloudWorker.retrieveData(true);
                Semaphore sem = new Semaphore(0);
                ShutdownListener sl = new ShutdownListener(sem);
                runtimeLogger.info("[Deletion Thread] Stopping worker " + cloudWorker.getName() + "...");
                cloudWorker.stop(sl);

                sl.enable();
                try {
                    sem.acquire();
                } catch (Exception e) {
                    resourcesLogger.error("ERROR: Exception raised on worker shutdown");
                }
                if (debug) {
                    runtimeLogger.debug("[Deletion Thread] Stopping worker " + cloudWorker.getName() + "...");
                }
            } else {
                if (debug) {
                    runtimeLogger.debug("[Deletion Thread] Worker " + cloudWorker.getName() + " should not be stopped.");
                }
            }
            if (debug) {
                runtimeLogger.debug("[Deletion Thread] Worker " + cloudWorker.getName() + " stopped. Powering of the VM");
            }
            try {
                this.operations.poweroff(vm);
                ResourceManager.removeWorker(cloudWorker);
                
            } catch (ConnectorException e) {
                resourcesLogger.error("ERROR: Powering off the resource", e);
            }

        }

        synchronized (countSynchronizer) {
            count--;
            if (debug) {
                runtimeLogger.debug("[Deletion Thread] Number of current VMs deletions decreased (" + count + ").");
            }
        }
    }
    
    /**
     * Returns the number of active deletion threads
     * 
     * @return
     */
    public static int getCount() {
        return count;
    }

}
