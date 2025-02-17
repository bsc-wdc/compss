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
package es.bsc.compss.ui;

import java.security.SecureRandom;
import java.util.Random;
import java.util.Vector;


public class StateData {

    private int timestamp;

    // Global core-load values
    private float totalLoad;
    private int totalCoresRunning;
    private int totalCoresPending;

    // Global resource values
    private int totalCPU;
    private float totalMemory;

    // Values per core
    private Vector<Float> meanTime; // load
    private Vector<Integer> runningCores;
    private Vector<Integer> pendingCores;

    // Values per resource
    private Vector<ResourceInfo> resourcesInformation;


    /**
     * Creates a new empty state data.
     */
    public StateData() {
        this.timestamp = 0;

        this.totalLoad = Float.valueOf(0);
        this.totalCoresRunning = 0;
        this.totalCoresPending = 0;

        this.totalCPU = 0;
        this.totalMemory = 0;

        this.meanTime = new Vector<>();
        this.runningCores = new Vector<>();
        this.pendingCores = new Vector<>();

        this.resourcesInformation = new Vector<>();
    }

    /**
     * Creates a new state data with the given timestamp {@code timestamp}.
     * 
     * @param timestamp State data timestamp.
     */
    public StateData(int timestamp) {
        this.timestamp = timestamp;

        this.totalLoad = Float.valueOf(0);
        this.totalCoresRunning = 0;
        this.totalCoresPending = 0;

        this.totalCPU = 0;
        this.totalMemory = 0;

        this.meanTime = new Vector<>();
        this.runningCores = new Vector<>();
        this.pendingCores = new Vector<>();

        this.resourcesInformation = new Vector<>();
    }

    /**
     * Copies the given state data {@code d}.
     * 
     * @param d State data object to copy.
     */
    public StateData(StateData d) {
        this.timestamp = d.getTimestamp();

        this.totalLoad = d.getTotalLoad();
        this.totalCoresRunning = d.getTotalCoresRunning();
        this.totalCoresPending = d.getTotalCoresPending();

        this.totalCPU = d.getTotalCPU();
        this.totalMemory = d.getTotalMemory();

        this.meanTime = d.getMeanTime();
        this.runningCores = d.getRunningCores();
        this.pendingCores = d.getPendingCores();

        this.resourcesInformation = d.getResourcesInformation();
    }

    // ---------------------------
    // PURGE FUNCTIONS
    // ---------------------------
    /**
     * Removes all the loaded values from the current state data except its timestamp.
     */
    public void purgeValues() {
        // Purges all values except timestamp
        this.totalLoad = Float.valueOf(0);
        this.totalCoresRunning = 0;
        this.totalCoresPending = 0;

        this.totalCPU = 0;
        this.totalMemory = 0;

        this.meanTime = new Vector<>();
        this.runningCores = new Vector<>();
        this.pendingCores = new Vector<>();

        this.resourcesInformation = new Vector<>();
    }

    /**
     * Removes all the load related values from the current state data (total load, running cores, pending cores, and
     * mean time).
     */
    public void purgeLoadValues() {
        // Purges all values related to core load
        this.totalLoad = Float.valueOf(0);
        this.totalCoresRunning = 0;
        this.totalCoresPending = 0;

        this.meanTime = new Vector<>();
        this.runningCores = new Vector<>();
        this.pendingCores = new Vector<>();
    }

    /**
     * Removes all the resource related values from the current state data (total cpus, total memory, resource
     * information).
     */
    public void purgeResourcesValues() {
        // Purges all values related to resources state
        this.totalCPU = 0;
        this.totalMemory = 0;

        this.resourcesInformation = new Vector<>();
    }

    // ---------------------------
    // GETTERS
    // ---------------------------
    public int getTimestamp() {
        return this.timestamp;
    }

    public float getTotalLoad() {
        return this.totalLoad;
    }

    public int getTotalCoresRunning() {
        return this.totalCoresRunning;
    }

    public int getTotalCoresPending() {
        return this.totalCoresPending;
    }

    public int getTotalCPU() {
        return this.totalCPU;
    }

    public float getTotalMemory() {
        return this.totalMemory;
    }

    public Vector<Float> getMeanTime() {
        return this.meanTime;
    }

    public Vector<Integer> getRunningCores() {
        return this.runningCores;
    }

    public Vector<Integer> getPendingCores() {
        return this.pendingCores;
    }

    public int getTotalResources() {
        return this.resourcesInformation.size();
    }

    public Vector<ResourceInfo> getResourcesInformation() {
        return this.resourcesInformation;
    }

    /**
     * Returns the total CPU consumption.
     * 
     * @return The total CPU consumption.
     */
    public int getTotalCPUConsumption() {
        float op1 = new SecureRandom().nextFloat() * Float.valueOf(this.totalCPU); // TODO
        float op2 = Float.valueOf(this.totalCPU);

        // Protection
        if (op2 == Float.valueOf(0)) {
            return 0;
        }

        // Normal result
        float result = (op1 / op2) * Float.valueOf(100);
        return (int) result;
    }

    /**
     * Returns the total memory consumption.
     * 
     * @return The total memory consumption.
     */
    public float getTotalMemoryConsumption() {
        float op1 = new SecureRandom().nextFloat() * this.totalMemory; // TODO

        // Protection
        if (this.totalMemory == Float.valueOf(0)) {
            return Float.valueOf(0);
        }

        // Normal result
        float result = (op1 / this.totalMemory) * Float.valueOf(100);
        return result;
    }

    /**
     * Returns the CPU consumption per resource.
     * 
     * @return The CPU consumption per resource.
     */
    public Vector<String> getCPUConsumption() {
        Vector<String> result = new Vector<String>();

        for (ResourceInfo resource : this.resourcesInformation) {
            result.add(resource.getName() + ":" + String.valueOf(resource.getCPUConsumption()));
        }

        return result;
    }

    /**
     * Returns the memory consumption per resource.
     * 
     * @return The memory consumption per resource.
     */
    public Vector<String> getMemoryConsumption() {
        Vector<String> result = new Vector<String>();

        for (ResourceInfo resource : this.resourcesInformation) {
            result.add(resource.getName() + ":" + String.valueOf(resource.getMemoryConsumption()));
        }

        return result;
    }

    // ---------------------------
    // SETTERS
    // ---------------------------
    public void setTimestamp(int timestamp) {
        this.timestamp = timestamp;
    }

    /**
     * Adds some load {@code load} to the core {@code id}.
     * 
     * @param id Core id.
     * @param load Load increase.
     */
    public void addCoreLoad(int id, float load) {
        // Resize if needed
        if (id >= this.meanTime.size()) {
            this.meanTime.setSize(id + 1);
        }
        // Update structures
        this.totalLoad = this.totalLoad + load;
        this.meanTime.set(id, load);
    }

    public void addResource(String resourceName, String type) {
        addResource(resourceName, type, 0, (float) 0.0);
    }

    /**
     * Adds a new resource to the current state data with the given information.
     * 
     * @param resourceName New resource name.
     * @param type New resource type.
     * @param cpus Amount of CPUs of the new resource.
     * @param memory Amount of memory of the new resource.
     */
    public void addResource(String resourceName, String type, int cpus, float memory) {
        this.resourcesInformation.add(new ResourceInfo(resourceName, type, cpus, memory));

        // Update global counters
        this.totalCPU += cpus;
        this.totalMemory += memory;
    }

    /**
     * Removes the resource with the given name {@code resourceName} if it exists.
     * 
     * @param resourceName Resource name.
     */
    public void removeResource(String resourceName) {
        boolean found = false;
        for (int i = 0; i < this.resourcesInformation.size() && !found; ++i) {
            if (this.resourcesInformation.get(i).getName().equals(resourceName)) {
                this.resourcesInformation.remove(i);
                found = true;
            }
        }
    }

    /**
     * Adds {@code running} running cores associated with the coreId {@code coreId}.
     * 
     * @param coreId CoreElementId.
     * @param running Number of running cores.
     */
    public void addCoreRunning(int coreId, int running) {
        // Update global variable
        this.totalCoresRunning = this.totalCoresRunning + running;

        // Resize if needed
        if (coreId >= this.runningCores.size()) {
            this.runningCores.setSize(coreId + 1);
        }
        // Update
        this.runningCores.set(coreId, running);
    }

    /**
     * Adds {@code newPending} pending cores associated with the coreId {@code coreId}.
     * 
     * @param coreId CoreElementId.
     * @param newPending Number of pending cores.
     */
    public void addCorePending(int coreId, int newPending) {
        // Update global variable
        this.totalCoresPending = this.totalCoresPending + newPending;

        // Resize if needed
        if (coreId >= this.pendingCores.size()) {
            this.pendingCores.setSize(coreId + 1);
        }
        // Update
        this.pendingCores.set(coreId, newPending);
    }


    // ----------------------------------
    // Private classes
    // ----------------------------------
    private class ResourceInfo {

        private final String name;
        private final String type;
        private final int cpu;
        private final float memory;


        public ResourceInfo(String name, String type, int cpu, float memory) {
            this.name = name;
            this.type = type;
            this.cpu = cpu;
            this.memory = memory;
        }

        public String getName() {
            return this.name;
        }

        public int getCPUConsumption() {
            if (type.equals("WORKER")) {
                float op1 = new SecureRandom().nextFloat() * Float.valueOf(this.cpu); // TODO
                float op2 = Float.valueOf(this.cpu);

                // Protection
                if (op2 == Float.valueOf(0)) {
                    return 0;
                }

                // Normal result
                float result = (op1 / op2) * Float.valueOf(100);
                return (int) result;
            }
            // Type SERVICE or UNKNOWN
            return 0;
        }

        public float getMemoryConsumption() {
            if (type.equals("WORKER")) {
                float op1 = new SecureRandom().nextFloat() * this.memory; // TODO

                // Protection
                if (this.memory == Float.valueOf(0)) {
                    return Float.valueOf(0);
                }

                // Normal result
                float result = (op1 / this.memory) * Float.valueOf(100);
                return result;
            }
            // Type SERVICE or UNKNOWN
            return Float.valueOf(0);
        }
    }

}
