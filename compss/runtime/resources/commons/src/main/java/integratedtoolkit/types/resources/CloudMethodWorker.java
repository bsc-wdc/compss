package integratedtoolkit.types.resources;

import integratedtoolkit.types.AdaptorDescription;
import integratedtoolkit.types.COMPSsWorker;
import integratedtoolkit.types.resources.description.CloudMethodResourceDescription;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.TreeMap;
import java.util.concurrent.Semaphore;

public class CloudMethodWorker<P> extends MethodWorker<P> {

    // Pending removals
    private final LinkedList<PendingReduction> pendingReductions;
    private final CloudMethodResourceDescription toRemove;

    public CloudMethodWorker(CloudMethodResourceDescription description, COMPSsWorker worker, Integer maxTaskCount) {
        super(description.getName(), description, worker, maxTaskCount);
        this.toRemove = new CloudMethodResourceDescription();
        this.pendingReductions = new LinkedList<PendingReduction>();
    }

    public CloudMethodWorker(String name, CloudMethodResourceDescription description, TreeMap<String, AdaptorDescription> adaptorsDesc, HashMap<String, String> properties, Integer maxTaskCount) throws Exception {
        super(name, description, adaptorsDesc, properties, maxTaskCount);

        if (description != null) {
            this.description.setSlots(maxTaskCount);
            ((CloudMethodResourceDescription) this.description).setName(name);
        }

        this.toRemove = new CloudMethodResourceDescription();
        this.pendingReductions = new LinkedList<PendingReduction>();
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
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append("<CPU>").append(description.getProcessorCPUCount()).append("</CPU>").append("\n");
        sb.append(prefix).append("<Core>").append(description.getProcessorCoreCount()).append("</Core>").append("\n");
        sb.append(prefix).append("<Memory>").append(description.getMemoryPhysicalSize()).append("</Memory>").append("\n");
        sb.append(prefix).append("<Disk>").append(description.getStorageElemSize()).append("</Disk>").append("\n");
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
            available.setProcessorCoreCount(available.getProcessorCoreCount() + increment.getProcessorCoreCount());
            available.setMemoryPhysicalSize(available.getMemoryPhysicalSize() + increment.getMemoryPhysicalSize());
        }
        synchronized (description) {
            description.setProcessorCoreCount(description.getProcessorCoreCount() + increment.getProcessorCoreCount());
            description.setMemoryPhysicalSize(description.getMemoryPhysicalSize() + increment.getMemoryPhysicalSize());
        }
        updatedFeatures();
    }

    @Override
    public boolean reserveResource(MethodResourceDescription consumption) {
        if (!hasAvailable(consumption)) {
            return false;
        }
        return super.reserveResource(consumption);
    }

    @Override
    public synchronized void releaseResource(MethodResourceDescription consumption) {
        // Freeing task constraints
        super.releaseResource(consumption);

        // Performing as much as possible reductions
        synchronized (pendingReductions) {
            if (!pendingReductions.isEmpty()) {
                logger.debug("Checking cloud resources to release...");
                for (PendingReduction pRed : pendingReductions) {
                    if (isValidReduction(pRed.reduction)) {
                        // Perform reduction
                        synchronized (available) {
                            available.setProcessorCoreCount(available.getProcessorCoreCount() - pRed.reduction.getProcessorCoreCount());
                            available.setMemoryPhysicalSize(available.getMemoryPhysicalSize() - pRed.reduction.getMemoryPhysicalSize());
                        }
                        // Untag pending to remove reduction
                        synchronized (toRemove) {
                            toRemove.setProcessorCoreCount(toRemove.getProcessorCoreCount() - pRed.reduction.getProcessorCoreCount());
                            toRemove.setMemoryPhysicalSize(toRemove.getMemoryPhysicalSize() - pRed.reduction.getMemoryPhysicalSize());
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
            description.setProcessorCoreCount(description.getProcessorCoreCount() - reduction.getProcessorCoreCount());
            description.setMemoryPhysicalSize(description.getMemoryPhysicalSize() - reduction.getMemoryPhysicalSize());
        }
        Semaphore sem = null;
        if (hasAvailable(reduction)) {
            synchronized (available) {
                available.setProcessorCoreCount(available.getProcessorCoreCount() - reduction.getProcessorCoreCount());
                available.setMemoryPhysicalSize(available.getMemoryPhysicalSize() - reduction.getMemoryPhysicalSize());
            }
        } else {
            if (taskCount > 0) {
                // This resource is still running tasks. Wait for them to finish...
                // Mark to remove and enqueue pending reduction
                synchronized (toRemove) {
                    toRemove.setProcessorCoreCount(toRemove.getProcessorCoreCount() + reduction.getProcessorCoreCount());
                    toRemove.setMemoryPhysicalSize(toRemove.getMemoryPhysicalSize() + reduction.getMemoryPhysicalSize());
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
            boolean coreFits = (available.getProcessorCoreCount() >= red.getProcessorCoreCount());
            boolean memFits = (available.getMemoryPhysicalSize() >= red.getMemoryPhysicalSize());

            if (logger.isDebugEnabled()) {
                logger.debug("Cloud Method reduction received:");
                logger.debug(available.getProcessorCoreCount() + " >= " + red.getProcessorCoreCount());
                logger.debug(available.getMemoryPhysicalSize() + " >= " + red.getMemoryPhysicalSize());
                logger.debug("With result: " + (coreFits & memFits));
            }

            return (coreFits & memFits);
        }
    }

    @Override
    public boolean hasAvailable(MethodResourceDescription consumption) {
        synchronized (available) {
            synchronized (toRemove) {
                boolean coreFits = (available.getProcessorCoreCount() - toRemove.getProcessorCoreCount() >= consumption.getProcessorCoreCount());
                boolean memFits = (available.getMemoryPhysicalSize() - toRemove.getMemoryPhysicalSize() >= consumption.getMemoryPhysicalSize());

                if (logger.isDebugEnabled()) {
                    logger.debug("Cloud Method Worker received:");
                    logger.debug(available.getProcessorCoreCount() + " - " + toRemove.getProcessorCoreCount() + " >= " + consumption.getProcessorCoreCount());
                    logger.debug(available.getMemoryPhysicalSize() + " - " + toRemove.getMemoryPhysicalSize() + " >= " + consumption.getMemoryPhysicalSize());
                    logger.debug("With result: " + (coreFits & memFits));
                }

                return (coreFits & memFits);
            }
        }
    }

    @Override
    public String getResourceLinks(String prefix) {
        StringBuilder sb = new StringBuilder(super.getResourceLinks(prefix));
        sb.append(prefix).append("TYPE = WORKER").append("\n");
        sb.append(prefix).append("CPU = ").append(description.getProcessorCPUCount()).append("\n");
        sb.append(prefix).append("MEMORY = ").append(description.getMemoryPhysicalSize()).append("\n");

        return sb.toString();
    }

    public boolean shouldBeStopped() {
        synchronized (available) {
            synchronized (toRemove) {
                return ((available.getProcessorCoreCount() == 0) && (toRemove.getProcessorCoreCount() == 0));
            }
        }
    }

    private class PendingReduction {

        CloudMethodResourceDescription reduction;
        Semaphore sem;

        private PendingReduction(CloudMethodResourceDescription reduction) {
            this.reduction = reduction;
            this.sem = new Semaphore(0);
        }
    }

    @Override
    public Worker getSchedulingCopy() {
        return new CloudMethodWorker(this);
    }
}
