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
package es.bsc.compss.connectors;

import es.bsc.compss.types.resources.CloudMethodWorker;
import es.bsc.compss.types.resources.description.CloudMethodResourceDescription;


/**
 * Representation of a VM.
 */
public class VM implements Comparable<VM> {

    // VM Id
    private final Object envId;

    // Associated descriptions
    private final CloudMethodResourceDescription rd;
    private CloudMethodWorker worker;

    // General Information
    private long requestTime;
    private long startTime;
    private long creationTime;

    // VM marked to delete
    private boolean toDelete;


    /**
     * New VM object with envId {@code envId} and description {@code description}.
     * 
     * @param envId VM Id.
     * @param description VM description.
     */
    public VM(Object envId, CloudMethodResourceDescription description) {
        this.envId = envId;
        this.rd = description;

        this.requestTime = System.currentTimeMillis();
        this.startTime = 0;
        this.creationTime = 0;

        this.toDelete = false;
    }

    /**
     * Returns the VM Id.
     * 
     * @return The VM Id.
     */
    public Object getEnvId() {
        return this.envId;
    }

    /**
     * Returns the VM name.
     * 
     * @return The VM name.
     */
    public String getName() {
        return this.rd.getName();
    }

    /**
     * Returns the VM description.
     * 
     * @return The VM description.
     */
    public CloudMethodResourceDescription getDescription() {
        return this.rd;
    }

    /**
     * Returns the VM worker.
     * 
     * @return The associated VM worker.
     */
    public CloudMethodWorker getWorker() {
        return this.worker;
    }

    /**
     * Returns the start time.
     * 
     * @return The VM start time.
     */
    public long getStartTime() {
        return this.startTime;
    }

    /**
     * Returns the creation time.
     * 
     * @return The VM creation time.
     */
    public long getCreationTime() {
        return this.creationTime;
    }

    /**
     * Returns the image name.
     * 
     * @return The image name.
     */
    public String getImage() {
        return this.rd.getImage().getImageName();
    }

    /**
     * Returns whether the VM is to delete or not.
     * 
     * @return {@literal true} if the VM is to delete, {@literal false} otherwise.
     */
    public boolean isToDelete() {
        return this.toDelete;
    }

    /**
     * Computes the creation time (stored internally).
     */
    public void computeCreationTime() {
        this.creationTime = this.startTime - this.requestTime;
    }

    /**
     * Sets the start time.
     * 
     * @param startTime New start time.
     */
    public void setStartTime(long startTime) {
        this.startTime = startTime;
    }

    /**
     * Sets the request time.
     * 
     * @param requestTime New request time.
     */
    public void setRequestTime(long requestTime) {
        this.requestTime = requestTime;
    }

    /**
     * Sets the worker representing this VM.
     * 
     * @param worker Associated worker.
     */
    public void setWorker(CloudMethodWorker worker) {
        this.worker = worker;
    }

    /**
     * Sets whether the VM is to delete or not.
     * 
     * @param toDelete Boolean indicating whether the VM is to delete or not.
     */
    public void setToDelete(boolean toDelete) {
        this.toDelete = toDelete;
    }

    // Comparable interface implementation
    @Override
    public int compareTo(VM vm) throws NullPointerException {
        if (vm == null) {
            throw new NullPointerException();
        }

        if (vm.getName().equals(getName())) {
            return 0;
        }

        long now = System.currentTimeMillis();
        int mod1 = (int) (now - getStartTime()) % 3_600_000; // 1 h in ms
        int mod2 = (int) (now - vm.getStartTime()) % 3_600_000; // 1 h in ms

        return mod2 - mod1;
    }

    @Override
    public boolean equals(Object obj) {
        if (obj instanceof VM) {
            VM vm = (VM) obj;
            return vm.getName().equals(getName());
        }

        return false;
    }

    @Override
    public int hashCode() {
        return getName().hashCode();
    }

    @Override
    public String toString() {
        return "VM " + this.envId + " (ip = " + this.rd.getName() + ", request time = " + this.requestTime
            + ", start time = " + this.startTime + ", creation time = " + this.creationTime + ", image = "
            + this.rd.getImage().getImageName() + ", procs = CPU: " + this.rd.getTotalCPUComputingUnits() + ", GPU: "
            + this.rd.getTotalGPUComputingUnits() + ", FPGA: " + this.rd.getTotalFPGAComputingUnits() + ", OTHER: "
            + this.rd.getTotalOTHERComputingUnits() + ", memory = " + this.rd.getMemorySize() + ", disk = "
            + this.rd.getStorageSize() + ", to delete = " + this.toDelete + ")";
    }

}
