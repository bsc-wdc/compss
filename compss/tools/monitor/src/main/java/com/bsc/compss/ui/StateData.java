package com.bsc.compss.ui;

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

    public void purgeLoadValues() {
        // Purges all values related to core load
        this.totalLoad = Float.valueOf(0);
        this.totalCoresRunning = 0;
        this.totalCoresPending = 0;

        this.meanTime = new Vector<>();
        this.runningCores = new Vector<>();
        this.pendingCores = new Vector<>();
    }

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

    public int getTotalCPUConsumption() {
        float op1 = new Random().nextFloat() * Float.valueOf(this.totalCPU); // TODO
        float op2 = Float.valueOf(this.totalCPU);

        // Protection
        if (op2 == Float.valueOf(0)) {
            return 0;
        }

        // Normal result
        float result = (op1 / op2) * Float.valueOf(100);
        return (int) result;
    }

    public float getTotalMemoryConsumption() {
        float op1 = new Random().nextFloat() * this.totalMemory; // TODO

        // Protection
        if (this.totalMemory == Float.valueOf(0)) {
            return Float.valueOf(0);
        }

        // Normal result
        float result = (op1 / this.totalMemory) * Float.valueOf(100);
        return result;
    }

    public Vector<String> getCPUConsumption() {
        Vector<String> result = new Vector<String>();

        for (ResourceInfo resource : this.resourcesInformation) {
            result.add(resource.getName() + ":" + String.valueOf(resource.getCPUConsumption()));
        }

        return result;
    }

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

    public void addResource(String resourceName, String type, int cpus, float memory) {
        this.resourcesInformation.add(new ResourceInfo(resourceName, type, cpus, memory));

        // Update global counters
        this.totalCPU += cpus;
        this.totalMemory += memory;
    }

    public void removeResource(String resourceName) {
        boolean found = false;
        for (int i = 0; i < this.resourcesInformation.size() && !found; ++i) {
            if (this.resourcesInformation.get(i).getName().equals(resourceName)) {
                this.resourcesInformation.remove(i);
                found = true;
            }
        }
    }

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
                float op1 = new Random().nextFloat() * Float.valueOf(this.cpu); // TODO
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
                float op1 = new Random().nextFloat() * this.memory; // TODO

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
