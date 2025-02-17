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
package es.bsc.compss.types.resources;

import es.bsc.compss.types.COMPSsWorker;
import es.bsc.compss.types.resources.configuration.MethodConfiguration;
import es.bsc.compss.types.resources.updates.PendingReduction;
import es.bsc.compss.util.ResourceManager;

import java.util.Iterator;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;


public class DynamicMethodWorker extends MethodWorker {

    // Pending removals
    private final List<PendingReduction<MethodResourceDescription>> pendingReductions;
    private final MethodResourceDescription toRemove;


    /**
     * Creates a new Dynamic Method Worker instance.
     * 
     * @param name Worker name.
     * @param description Worker resource description.
     * @param worker Associated COMPSs worker.
     * @param limitOfTasks Limit of CPU tasks.
     * @param limitGPUTasks Limit of GPU tasks.
     * @param limitFPGATasks Limit of FPGA tasks.
     * @param limitOTHERTasks Limit of OTHER tasks.
     * @param sharedDisks Map of the available shared disks.
     */
    public DynamicMethodWorker(String name, MethodResourceDescription description, COMPSsWorker worker,
        int limitOfTasks, int limitGPUTasks, int limitFPGATasks, int limitOTHERTasks, Map<String, String> sharedDisks) {

        super(name, description, worker, limitOfTasks, limitGPUTasks, limitFPGATasks, limitOTHERTasks, sharedDisks);
        this.toRemove = new MethodResourceDescription();
        this.pendingReductions = new LinkedList<>();
    }

    /**
     * Creates a new Dynamic Method Worker instance.
     * 
     * @param name Worker name.
     * @param description Worker resource description.
     * @param config Method configuration.
     * @param sharedDisks Map of the available shared disks.
     */
    public DynamicMethodWorker(String name, MethodResourceDescription description, MethodConfiguration config,
        Map<String, String> sharedDisks) {

        super(name, description, config, sharedDisks);

        this.toRemove = new MethodResourceDescription();
        this.pendingReductions = new LinkedList<>();
    }

    /**
     * Clones the given dynamic method worker.
     * 
     * @param cmw Dynamic method worker to clone.
     */
    public DynamicMethodWorker(DynamicMethodWorker cmw) {
        super(cmw);
        this.toRemove = cmw.toRemove.copy();
        this.pendingReductions = cmw.pendingReductions;
    }

    @Override
    public ResourceType getType() {
        return ResourceType.WORKER;
    }

    @Override
    public MethodResourceDescription getDescription() {
        return (MethodResourceDescription) super.getDescription();
    }

    @Override
    public String getMonitoringData(String prefix) {
        StringBuilder sb = new StringBuilder();
        sb.append(prefix).append(super.getMonitoringData(prefix));

        return sb.toString();
    }

    /**
     * Increases the current worker with the given increment {@code increment}.
     * 
     * @param increment Description of the increment to perform.
     */
    public void increaseFeatures(MethodResourceDescription increment) {
        final int CPUCount = increment.getTotalCPUComputingUnits();
        final int GPUCount = increment.getTotalGPUComputingUnits();
        final int FPGACount = increment.getTotalFPGAComputingUnits();
        final int otherCount = increment.getTotalOTHERComputingUnits();

        this.getNode().increaseComputingCapabilities(increment);
        synchronized (this.available) {
            this.available.increase(increment);
        }
        synchronized (this.description) {
            ((MethodResourceDescription) this.description).increase(increment);
        }

        setMaxCPUTaskCount(getMaxCPUTaskCount() + CPUCount);
        setMaxGPUTaskCount(getMaxGPUTaskCount() + GPUCount);
        setMaxFPGATaskCount(getMaxFPGATaskCount() + FPGACount);
        setMaxOthersTaskCount(getMaxOthersTaskCount() + otherCount);
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
        LOGGER.debug("Checking cloud resources to release...");
        // Freeing task constraints
        synchronized (this.available) {
            super.releaseResource(consumption);

            // Performing as many as possible reductions
            synchronized (this.pendingReductions) {
                if (!this.pendingReductions.isEmpty()) {
                    Iterator<PendingReduction<MethodResourceDescription>> prIt = this.pendingReductions.iterator();
                    while (prIt.hasNext()) {
                        PendingReduction<MethodResourceDescription> pRed = prIt.next();
                        if (this.available.containsDynamic(pRed.getModification())) {
                            // Perform reduction
                            this.available.reduce(pRed.getModification());

                            // Untag pending to remove reduction
                            synchronized (this.toRemove) {
                                this.toRemove.reduce(pRed.getModification());
                            }
                            // Reduction is done, release sem
                            LOGGER.debug("Releasing cloud resource " + this.getName());
                            pRed.notifyCompletion();
                            prIt.remove();
                        } else {
                            break;
                        }
                    }
                }
            }
        }
    }

    /**
     * Decreases the current worker with the given reduction {@code pRed}.
     * 
     * @param pRed Reduction to perform.
     */
    public synchronized void applyReduction(PendingReduction<MethodResourceDescription> pRed) {
        MethodResourceDescription reduction = pRed.getModification();
        final int CPUCount = reduction.getTotalCPUComputingUnits();
        final int GPUCount = reduction.getTotalGPUComputingUnits();
        final int FPGACount = reduction.getTotalFPGAComputingUnits();
        final int otherCount = reduction.getTotalOTHERComputingUnits();

        if (!this.isLost()) {
            this.getNode().reduceComputingCapabilities(reduction);
        }
        synchronized (this.description) {
            this.getDescription().reduce(reduction);
        }
        synchronized (this.available) {
            if (!hasAvailable(reduction) && this.getUsedCPUTaskCount() > 0) {

                // This resource is still running tasks. Wait for them to finish...
                // Mark to remove and enqueue pending reduction
                LOGGER.debug("Resource in use. Adding pending reduction");
                synchronized (this.toRemove) {
                    this.toRemove.increase(reduction);
                }
                synchronized (this.pendingReductions) {
                    this.pendingReductions.add(pRed);
                }
            } else {
                // Resource is not executing tasks. We can erase it, nothing to do
                this.available.reduce(reduction);
                pRed.notifyCompletion();
            }
        }
        setMaxCPUTaskCount(getMaxCPUTaskCount() - CPUCount);
        setMaxGPUTaskCount(getMaxGPUTaskCount() - GPUCount);
        setMaxFPGATaskCount(getMaxFPGATaskCount() - FPGACount);
        setMaxOthersTaskCount(getMaxOthersTaskCount() - otherCount);

        updatedFeatures();
    }

    @Override
    public boolean hasAvailable(MethodResourceDescription consumption) {
        synchronized (this.available) {
            synchronized (this.toRemove) {
                consumption.increaseDynamic(this.toRemove);
                boolean fits = super.hasAvailable(consumption);
                consumption.reduceDynamic(this.toRemove);
                if (LOGGER.isDebugEnabled()) {
                    LOGGER.debug("Cloud Method Worker received:");
                    LOGGER.debug("With result: " + fits);
                }
                return fits;
            }
        }
    }

    /**
     * Returns whether the worker should be stopped or not.
     * 
     * @return {@literal true} if the worker should be stopped (is useless), {@literal false} otherwise.
     */
    public boolean shouldBeStopped() {
        synchronized (this.description) {
            return this.getDescription().isDynamicUseless();
        }
    }

    /**
     * Destroys the current worker applying the given modifications.
     * 
     * @param modification Modifications to destroy.
     */
    public <T extends WorkerResourceDescription> void destroyResources(T modification) {
        ResourceManager.terminateDynamicResource(this, (MethodResourceDescription) modification);
    }

    @Override
    public DynamicMethodWorker getSchedulingCopy() {
        return new DynamicMethodWorker(this);
    }

}
