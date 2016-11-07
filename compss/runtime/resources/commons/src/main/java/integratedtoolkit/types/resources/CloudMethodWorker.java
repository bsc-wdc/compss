package integratedtoolkit.types.resources;

import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.resources.configuration.MethodConfiguration;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.concurrent.Semaphore;


public class CloudMethodWorker extends MethodWorker {

    // Pending removals
    private final LinkedList<PendingReduction> pendingReductions;
    private final CloudMethodResourceDescription toRemove;


    public CloudMethodWorker(CloudMethodResourceDescription description, COMPSsWorker worker, int limitOfTasks,
            HashMap<String, String> sharedDisks) {
        super(description.getName(), description, worker, limitOfTasks, sharedDisks);
        this.toRemove = new CloudMethodResourceDescription();
        this.pendingReductions = new LinkedList<>();
    }

    public CloudMethodWorker(String name, CloudMethodResourceDescription description, MethodConfiguration config,
            HashMap<String, String> sharedDisks) {
        
        super(name, description, config, sharedDisks);

        if (this.description != null) {
            // Add name
            ((CloudMethodResourceDescription) this.description).setName(name);
        }

        this.toRemove = new CloudMethodResourceDescription();
        this.pendingReductions = new LinkedList<>();
    }

    public CloudMethodWorker(CloudMethodWorker cmw) {
        super(cmw);
        this.toRemove = cmw.toRemove.copy();
        this.pendingReductions = cmw.pendingReductions;
    }

    @Override
    public Type getType() {
        return Type.WORKER;
    }

    @Override
    public String getMonitoringData(String prefix) {
        // TODO: Add full information about description (mem type, each processor information, etc)
        StringBuilder sb = new StringBuilder();
        sb.append(super.getMonitoringData(prefix));
        String providerName = ((CloudMethodResourceDescription) description).getProviderName();
        if (providerName == null) {
            providerName = new String("");
        }
        sb.append(prefix).append("<Provider>").append(providerName).append("</Provider>").append("\n");
        String imageName = ((CloudMethodResourceDescription) description).getName();
        if (imageName == null) {
            imageName = new String("");
        }
        sb.append(prefix).append("<Image>").append(imageName).append("</Image>").append("\n");

        return sb.toString();
    }

    public void increaseFeatures(CloudMethodResourceDescription increment) {
        synchronized (available) {
            available.increase(increment);
        }
        synchronized (description) {
            description.increase(increment);
        }
        updatedFeatures();
    }

    @Override
    public MethodResourceDescription reserveResource(MethodResourceDescription consumption) {
        if (!hasAvailable(consumption)) {
            return null;
        }

        return super.reserveResource(consumption);
    }

    @Override
    public synchronized void releaseResource(MethodResourceDescription consumption) {
        logger.debug("Checking cloud resources to release...");
        // Freeing task constraints
        super.releaseResource(consumption);

        // Performing as much as possible reductions
        synchronized (pendingReductions) {
            if (!pendingReductions.isEmpty()) {
                PendingReduction[] lpr = pendingReductions.toArray(new PendingReduction[pendingReductions.size()]);
                for (PendingReduction pRed : lpr) {
                    if (isValidReduction(pRed.reduction)) {
                        // Perform reduction
                        synchronized (available) {
                            available.reduce(pRed.reduction);
                        }
                        // Untag pending to remove reduction
                        synchronized (toRemove) {
                            toRemove.reduce(pRed.reduction);
                        }
                        // Reduction is done, release sem
                        logger.debug("Releasing cloud resource " + this.getName());
                        pRed.sem.release();
                        pendingReductions.remove(pRed);
                    } else {
                        break;
                    }
                }
            }
        }
    }

    public synchronized Semaphore reduceFeatures(CloudMethodResourceDescription reduction) {
        synchronized (description) {
            description.reduce(reduction);
        }
        Semaphore sem = null;
        if (hasAvailable(reduction)) {
            synchronized (available) {
                available.reduce(reduction);
            }
        } else {
            if (this.getUsedTaskCount() > 0) {
                // This resource is still running tasks. Wait for them to finish...
                // Mark to remove and enqueue pending reduction
                synchronized (toRemove) {
                    toRemove.reduce(reduction);
                }
                PendingReduction pRed = new PendingReduction(reduction);
                synchronized (pendingReductions) {
                    pendingReductions.add(pRed);
                }
                sem = pRed.sem;
            } else {
                // Resource is not executing tasks. We can erase it, nothing to do
            }
        }
        updatedFeatures();

        return sem;
    }

    private boolean isValidReduction(MethodResourceDescription red) {
        synchronized (available) {
            boolean fits = available.containsDynamic(red);

            if (logger.isDebugEnabled()) {
                logger.debug("Cloud Method reduction received:");
                logger.debug("With result: " + fits);
            }

            return fits;
        }
    }

    @Override
    public boolean hasAvailable(MethodResourceDescription consumption) {
        synchronized (available) {
            synchronized (toRemove) {
                consumption.increase(toRemove);
                boolean fits = available.containsDynamic(consumption);
                consumption.reduce(toRemove);

                if (logger.isDebugEnabled()) {
                    logger.debug("Cloud Method Worker received:");
                    logger.debug("With result: " + fits);
                }

                return fits;
            }
        }
    }

    public boolean shouldBeStopped() {
        synchronized (available) {
            synchronized (toRemove) {
                return ((available.getTotalCPUComputingUnits() == 0) && (toRemove.getTotalCPUComputingUnits() == 0));
            }
        }
    }
    
    @Override
    public Worker<?> getSchedulingCopy() {
        return new CloudMethodWorker(this);
    }
    

    private class PendingReduction {

        private CloudMethodResourceDescription reduction;
        private Semaphore sem;


        private PendingReduction(CloudMethodResourceDescription reduction) {
            this.reduction = reduction;
            this.sem = new Semaphore(0);
        }
        
    }

}
