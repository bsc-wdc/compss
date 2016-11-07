package integratedtoolkit.types;

import integratedtoolkit.util.CoreManager;

import java.util.HashMap;


public class ResourcesState {

    // Resource Information
    private final HashMap<String, HostInfo> currentResources_nameToInfo;
    private final HashMap<String, HostInfo> pendingResources_nameToInfo;

    // Cloud Usage
    private boolean useCloud;
    private int currentCloudVMCount;
    private long creationTime;


    public ResourcesState() {
        // Resource Information
        currentResources_nameToInfo = new HashMap<>();
        pendingResources_nameToInfo = new HashMap<>();
    }

    public void setUseCloud(boolean useCloud) {
        this.useCloud = useCloud;
    }

    public boolean getUseCloud() {
        return useCloud;
    }

    public void setCurrentCloudVMCount(int currentCloudVMCount) {
        this.currentCloudVMCount = currentCloudVMCount;
    }

    public int getCurrentCloudVMCount() {
        return currentCloudVMCount;
    }

    public void setCreationTime(long creationTime) {
        this.creationTime = creationTime;
    }

    public long getCreationTime() {
        return creationTime;
    }

    public void addHost(String hostName, String type, int cpus, float memory, int[] coreSimultaneousTasks, boolean active) {
        HostInfo hi = new HostInfo(hostName, type, cpus, memory);
        hi.setCoreSlots(coreSimultaneousTasks);
        if (active) {
            currentResources_nameToInfo.put(hi.getHostName(), hi);
        } else {
            pendingResources_nameToInfo.put(hi.getHostName(), hi);
        }
    }

    public void updateHostInfo(String hostName, String type, int cpus, float memory, int coreId, int coreSimTasks, boolean active) {
        // Check if core exists
        HostInfo hi = pendingResources_nameToInfo.get(hostName);
        if (hi == null) {
            hi = currentResources_nameToInfo.get(hostName);
            if (hi == null) {
                // Host doesn't exist. Add host
                hi = new HostInfo(hostName, type, cpus, memory);
                if (active) {
                    currentResources_nameToInfo.put(hi.getHostName(), hi);
                } else {
                    pendingResources_nameToInfo.put(hi.getHostName(), hi);
                }
            }
        }

        // Update core Slots of this host
        hi.updateCoreSlots(coreId, coreSimTasks);
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("RESOURCES_INFO = [").append("\n");
        for (java.util.Map.Entry<String, HostInfo> entry : currentResources_nameToInfo.entrySet()) {
            String resourceName = entry.getKey();
            HostInfo hi = entry.getValue();
            sb.append("\t").append("RESOURCE = [").append("\n");
            sb.append("\t").append("\t").append("NAME = ").append(resourceName).append("\n");
            sb.append("\t").append("\t").append("TYPE = ").append(hi.getType()).append("\n");
            sb.append("\t").append("\t").append("CPUS = ").append(hi.getCPUS()).append("\n");
            sb.append("\t").append("\t").append("MEMORY = ").append(hi.getMemory()).append("\n");
            sb.append("\t").append("\t").append("CAN_RUN = [").append("\n");
            sb.append(hi.getCoreSlots("\t\t\t"));
            sb.append("\t").append("\t").append("]").append("\n"); // End CAN_RUN
            sb.append("\t").append("]").append("\n"); // End RESOURCE
        }
        sb.append("]").append("\n"); // END RESOURCES_INFO

        // Cloud Information
        sb.append("CLOUD_INFO = [").append("\n");
        if (useCloud) {
            sb.append("\t").append("CURRENT_CLOUD_VM_COUNT = ").append(currentCloudVMCount).append("\n");
            sb.append("\t").append("CREATION_TIME = ").append(creationTime).append("\n");
            sb.append("\t").append("PENDING_RESOURCES = [").append("\n");
            for (java.util.Map.Entry<String, HostInfo> entry : pendingResources_nameToInfo.entrySet()) {
                String resourceName = entry.getKey();
                HostInfo hi = entry.getValue();
                sb.append("\t").append("RESOURCE = [").append("\n");
                sb.append("\t").append("\t").append("NAME = ").append(resourceName).append("\n");
                sb.append("\t").append("\t").append("TYPE = ").append(hi.getType()).append("\n");
                sb.append("\t").append("\t").append("CPUS = ").append(hi.getCPUS()).append("\n");
                sb.append("\t").append("\t").append("MEMORY = ").append(hi.getMemory()).append("\n");
                sb.append("\t").append("\t").append("CAN_RUN = [").append("\n");
                sb.append(hi.getCoreSlots("\t\t\t"));
                sb.append("\t").append("\t").append("]").append("\n"); // End CAN_RUN
                sb.append("\t").append("]").append("\n"); // End RESOURCE
            }
            sb.append("\t").append("]").append("\n"); // End PENDING_RESOURCES
        }
        sb.append("]"); // END CLOUD_INFO
        return sb.toString();
    }


    private static class HostInfo {

        private String hostName;
        private String type;
        private int cpus;
        private float memory;
        private int[] coreSlots;


        public HostInfo(String hostName, String type, int cpus, float memory) {
            this.hostName = hostName;
            this.type = type;
            this.cpus = cpus;
            this.memory = memory;
            this.coreSlots = new int[CoreManager.getCoreCount()];
        }

        public String getHostName() {
            return this.hostName;
        }

        public String getType() {
            return type.toString();
        }

        public int getCPUS() {
            return cpus;
        }

        public double getMemory() {
            return memory;
        }

        public String getCoreSlots(String prefix) {
            StringBuilder sb = new StringBuilder();
            for (int i = 0; i < coreSlots.length; i++) {
                sb.append(prefix).append("CORE = [").append("\n");
                sb.append(prefix).append("\t").append("COREID = ").append(i).append("\n");
                sb.append(prefix).append("\t").append("NUM_SLOTS = ").append(coreSlots[i]).append("\n");
                sb.append(prefix).append("]").append("\n");
            }

            return sb.toString();
        }

        public void setCoreSlots(int[] coreSlots) {
            this.coreSlots = coreSlots;
        }

        public void updateCoreSlots(int coreId, int coreSimTasks) {
            if (coreId < this.coreSlots.length) {
                coreSlots[coreId] += coreSimTasks;
            }
        }
    }

}
